/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

//
// This API contains all logic for CRUD on object metadata, which is
// stored in Moray.
//

var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var jsprim = require('jsprim');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var moray = require('moray');
var once = require('once');
var vasync = require('vasync');
var VError = require('verror');

var utils = require('../utils');



///--- Globals

var sprintf = util.format;

var BUCKET = process.env.MANTA_RING_BUCKET || 'manta';
/*
 * NOTE: Care must be taken when deploying a incremented version of the manta
 * bucket for large databases. Currently `no_reindex = true` is being
 * passed into createBucket below. If new columns are added to the manta
 * bucket, this reindex option should be removed or at least revisited.
 * Do not change BUCKET_VERSION without discussing a deployment strategy.
 */
var BUCKET_VERSION = 2;

/* JSSTYLED */
var ROOT_RE = /^\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\/stor$/;
var SCHEMA = {
    dirname: {
        type: 'string'
    },
    name: {
        type: 'string'
    },
    owner: {
        type: 'string'
    },
    objectId: {
        type: 'string'
    },
    type: {
        type: 'string'
    }
};
var POST = [
    recordDeleteLog
];


var DELETE_LOG_BUCKET = process.env.MANTA_DELETE_LOG_BUCKET ||
    'manta_delete_log';
var DELETE_LOG_SCHEMA = {
    objectId: {
        type: 'string'
    }
};
var DELETE_LOG_VERSION = 1;

var FASTDELETE_QUEUE_BUCKET = process.env.MANTA_FASTDELETE_QUEUE_BUCKET ||
    'manta_fastdelete_queue';
var FASTDELETE_QUEUE_VERSION = 1;

var DIR_COUNT_BUCKET = 'manta_directory_counts';
var DIR_COUNT_SCHEMA = {
    entries: {
        type: 'number'
    }
};
var DIR_COUNT_VERSION = 1;


var MANTA_UPLOADS_BUCKET = process.env.MANTA_UPLOADS_BUCKET ||
    'manta_uploads';
var MANTA_UPLOADS_SCHEMA = {
    finalizingType: {
        type: 'string'
    },
    uploadId: {
        type: 'string'
    }
};
var MANTA_UPLOADS_VERSION = 1;


///--- Internal Functions


function clone(obj) {
    if (!obj)
        return (obj);

    var copy = {};
    Object.keys(obj).forEach(function (k) {
        copy[k] = obj[k];
    });
    return (copy);
}


function recordDeleteLog(req, cb) {
    var microtime = require('microtime');
    var crc = require('crc');

    var log = req.log;
    log.debug({
        id: req.id,
        bucket: req.bucket,
        key: req.key,
        value: req.value,
        headers: req.headers
    }, 'recordDeleteLog entered.');
    var prevmd = req.headers['x-muskie-prev-metadata'];
    if (!prevmd || !prevmd.objectId) {
        log.debug('not logging without previous metadata');
        cb();
        return;
    }
    var prevObjectId = prevmd.objectId;

    if (req.value && req.value.objectId &&
        prevObjectId === req.value.objectId) {
        log.debug('not logging since object === prev object');
        cb();
        return;
    }
    log.debug('object ' + prevObjectId + ' is candidate for deletion.');

    // now log to the manta_delete_log table or the manta_fastdelete_queue...
    var now = Math.round((microtime.now() / 1000));
    var _key = '/' + prevObjectId + '/' + now;
    var _value = JSON.stringify(prevmd);
    var _etag = crc.hex32(crc.crc32(_value));
    var _mtime = now;
    var sql = '';
    var values = [];

    // If snaplinks are disabled use the fastdelete_queue rather than delete_log
    if (req.headers['x-muskie-snaplinks-disabled']) {
        log.debug('object ' + prevObjectId + ' being added to fastdelete.');
        sql = 'INSERT INTO manta_fastdelete_queue (_key, _value, _etag, ' +
            '_mtime) VALUES ($1, $2, $3, $4)';
        values = [prevObjectId, _value, _etag, _mtime];
    } else {
        sql = 'INSERT INTO manta_delete_log (_key, _value, _etag, ' +
            '_mtime, objectId) VALUES ($1, $2, $3, $4, $5)';
        values = [_key, _value, _etag, _mtime, prevObjectId];
    }

    // execute
    var q = req.pg.query(sql, values);
    q.once('error', function (err) {
        log.debug(err, 'manta delete log insert: failed');
        cb(err);
    });
    q.once('end', function () {
        log.debug('manta delete log insert: done');
        cb();
    });
}


function createMetadata(options) {
    assert.string(options.owner, 'options.owner');
    assert.string(options.type, 'options.type');
    assert.optionalObject(options.headers, 'options.headers');

    var key = options.key;
    var md = {
        dirname: (options.type !== 'bucketobject' ?
                  (ROOT_RE.test(key) ? key : path.dirname(key)) :
                  options.dirname),
        key: key,
        headers: (options.type !== 'link' ?
                  clone(options.headers || {}) : undefined),
        mtime: Date.now(),
        name: (options.type !== 'bucketobject' ? path.basename(key) :
               (key.split(options.dirname+'/').pop())),
        creator: options.creator || options.owner,
        owner: options.owner,
        roles: options.roles,
        type: options.type
    };

    switch (options.type) {
    case 'bucketobject':
    case 'object':
        assert.number(options.contentLength, 'options.contentLength');
        assert.string(options.contentMD5, 'options.contentMD5');
        assert.string(options.contentType, 'options.contentType');
        assert.string(options.objectId, 'options.objectId');
        assert.arrayOfObject(options.sharks, 'options.sharks');

        if (!process.env.NODE_NDEBUG) {
            options.sharks.forEach(function validateShark(s) {
                assert.string(s.manta_storage_id,
                              'shark.manta_storage_id');
            });
        }

        md.contentLength = options.contentLength;
        md.contentMD5 = options.contentMD5;
        md.contentType = options.contentType;
        md.etag = options.etag || options.objectId;
        md.objectId = options.objectId;
        md.sharks = options.sharks.slice();
        break;
    case 'link':
        assert.object(options.link, 'options.link');
        var src = options.link;
        md.contentLength = src.contentLength;
        md.contentMD5 = src.contentMD5;
        md.contentType = src.contentType;
        md.createdFrom = src.key;
        md.etag = src.etag;
        md.headers = clone(src.headers);
        md.objectId = src.objectId;
        md.sharks = src.sharks;
        md.type = 'object'; // overwrite;
        break;

    case 'bucket':
    case 'directory':
        if (options.upload) {
            md.upload = options.upload;
        }
        break;

    default:
        break;
    }

    return (md);
}




// Helper that calls Moray's `putObject`.
function put(options, cb) {
    assert.object(options, 'options');
    assert.number(options.attempts, 'options.number');
    assert.ok(options.attempts >= 0);
    assert.object(options.client, 'options.client');
    assert.object(options.log, 'options.log');
    assert.string(options.op, 'options.op');
    assert.ok(options.op === 'putMetadata' ||
        options.op === 'putFinalizingMetadata');
    assert.string(options.bucket, 'options.bucket');
    assert.string(options.key, 'options.key');
    assert.object(options.md, 'options.md');
    assert.object(options.putOptions, 'options.putOptions');
    assert.func(cb, 'callback');

    var attempts = options.attempts;
    var client = options.client;
    var log = options.log;
    var op = options.op;
    var bucket = options.bucket;
    var key = options.key;
    var md = options.md;
    var opts = options.putOptions;

    log.debug({
        attempts: attempts,
        key: key,
        metadata: md,
        etag: opts.etag,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.' + op + ': entered');

    client.putObject(bucket, key, md, opts, function (err, data) {
        if (err) {
            log.debug({
                err: err,
                key: key,
                requestId: opts.requestId
            }, 'Moray.' + op + ': error writing metadata');

            if ((err.name === 'EtagConflictError' ||
                err.name === 'UniqueAttributeError') &&
                opts.etag === undefined && ++attempts < 3) {
                options.attempts++;
                setImmediate(put, options, cb);
            } else {
                cb(err);
            }
        } else {
            log.debug({
                key: key,
                requestId: opts.requestId
            }, 'Moray.' + op + ': done');
            cb(null, md, data);
        }
    });
}

///--- API

function Moray(options) {
    var self = this;

    assert.optionalBool(options.readOnly, 'options.readOnly');

    EventEmitter.call(this);

    this.client = null;

    if (options.hasOwnProperty('morayOptions')) {
        this.morayOptions = jsprim.deepCopy(options.morayOptions);
    } else {
        this.morayOptions = {
            'host': options.host,
            'port': parseInt(options.port || 2020, 10),
            'retry': options.retry,
            'connectTimeout': options.connectTimeout || 1000
        };
    }

    this.log = options.log.child({ component: 'MorayIndexClient' }, true);
    this.morayOptions.log = this.log;
    this.morayOptions.unwrapErrors = true;
    this.readOnly = false;

    if (options.readOnly) {
        this.readOnly = true;
    }

    /*
     * Configure the exponential backoff object we use to manage backoff during
     * initialization.
     */
    this.initBackoff = new backoff.exponential({
        'randomisationFactor': 0.5,
        'initialDelay': 1000,
        'maxDelay': 300000
    });

    this.initBackoff.on('backoff', function (which, delay, error) {
        assert.equal(which + 1, self.initAttempts);
        self.log.warn({
            'nfailures': which + 1,
            'willRetryAfterMilliseconds': delay,
            'error': error
        }, 'libmanta.Moray.initAttempt failed (will retry)');
    });

    this.initBackoff.on('ready', function () {
        self.initAttempt();
    });

    /*
     * Define event handlers for the Moray client used at various parts during
     * initialization.
     *
     * The Moray client should generally not emit errors, but it's known to do
     * so under some conditions.  Our response depends on what phases of
     * initialization we've already completed:
     *
     * (1) Before we've established a connection to the client: if an error is
     *     emitted at this phase, we assume that we failed to establish a
     *     connection and we abort the current initialization attempt.  We will
     *     end up retrying with exponential backoff.
     *
     * (2) After we've established a connection, but before initialization has
     *     completed: if an error is emitted at this phase, we'll log it but
     *     otherwise ignore it because we assume that whatever operations we
     *     have outstanding will also fail.
     *
     * (3) After we've initialized, errors are passed through to our consumer.
     */
    this.onErrorDuringInit = function onErrorDuringInit(err) {
        self.log.warn(err, 'ignoring client-level error during init');
    };
    this.onErrorPostInit = function onErrorPostInit(err) {
        self.log.warn(err, 'moray client error');
        self.emit('error', err);
    };

    /* These fields exist only for debugging. */
    this.initAttempts = 0;
    this.initPipeline = null;
    this.initBuckets = null;
    this.initTrigger = null;

    this.initAttempt();
}

util.inherits(Moray, EventEmitter);

Moray.prototype.initAttempt = function initAttempt() {
    var self = this;
    var log = this.log;

    assert.ok(this.client === null, 'previous initAttempt did not complete');
    assert.ok(this.initPipeline === null);
    assert.ok(this.initBuckets === null);
    assert.ok(this.initTrigger === null);

    this.initAttempts++;
    log.debug({
        'attempt': this.initAttempts
    }, 'libmanta.Moray.initAttempt: entered');

    var initFuncs = [];

    /*
     * Define vasync waterfall steps such that we can
     * decide which ones to add to the waterfall depending
     * on whether or not this is a read-only client.
     */
    function initClient(callback) {
        self.client = moray.createBucketClient(self.morayOptions);

        var onErrorDuringConnect = function onErrDuringConnect(err) {
            callback(new VError(err, 'moray client error'));
        };

        self.client.on('error', onErrorDuringConnect);
        self.client.once('connect', function onConnect() {
            self.client.removeListener('error', onErrorDuringConnect);
            self.client.on('error', self.onErrorDuringInit);
            callback();
        });
    }

    function setupBuckets(callback) {
        self.initBuckets = setupMantaBuckets(log, self.client, callback);
    }

    function setupTrigger(callback) {
        self.initTrigger = setupMantaTrigger(log, self.client, callback);
    }

    initFuncs.push(initClient);
    // If this is a readOnly client, do not do database setup tasks
    // if (!this.readOnly) {
    //     initFuncs.push(setupBuckets);
    //     initFuncs.push(setupTrigger);
    // }

    this.initPipeline = vasync.waterfall(initFuncs, function (err) {
        self.initPipeline = null;
        self.initBuckets = null;
        self.initTrigger = null;

        if (err) {
            if (self.initBuckets !== null) {
                self.client.removeListener('error', self.onErrorDuringInit);
            }
            self.client.close();
            self.client = null;
            err = new VError(err, 'libmanta.Moray.initAttempt');
            self.initBackoff.backoff(err);
        } else {
            /*
             * We could reset the "backoff" object in the success case, or
             * even null it out since we're never going to use it again.
             * But it's not that large, and it may be useful for debugging,
             * so we just leave it alone.
             */
            self.client.removeListener('error', self.onErrorDuringInit);
            self.client.on('error', self.onErrorPostInit);
            self.client.on('close', self.emit.bind(self, 'close'));
            self.client.on('connect', self.emit.bind(self, 'connect'));
            log.info({ 'attempt': self.initAttempts },
                'libmanta.Moray.initAttempt: done');
            self.emit('connect');
        }
    });
};

Moray.prototype.createBucket = function createBucket(opts, callback) {
    assert.string(opts.owner, 'opts.owner');
    assert.string(opts.key, 'opts.key');
    assert.object(opts.log, 'opts.log');
    assert.func(callback, 'callback');

    var owner = opts.owner;
    var bucket = opts.key;
    var log = opts.log;

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var client = this.client;

    client.createBucketNoVnode(owner, bucket, function (err, bucket_data) {
        if (err) {
            log.debug({
                err: err,
                bucket: bucket,
                requestId: opts.requestId
            }, 'Moray.createBucket: error reading metadata');
            callback(err);
        } else {
            log.debug({
                bucket: bucket,
                data: bucket_data,
                requestId: opts.requestId
            }, 'Moray.createBucket: done');
            callback(null, bucket_data);
        }
    });
}


Moray.prototype.getBucket = function getBucket(options, callback) {
    assert.object(options, 'options');
    assert.string(options.owner, 'options.owner');
    assert.string(options.key, 'options.key');
    assert.string(options.requestId, 'options.requestId');
    assert.func(callback, 'callback');

    var owner = options.owner;
    var bucket = options.key;

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var client = this.client;
    var log = this.log;
    var opts = {
        req_id: options.requestId,
        noCache: true
    };

    log.debug({
        bucket: bucket,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.getBucket: entered');

    client.getBucketNoVnode(owner, bucket, function (err, bucket_data) {
        if (err) {
            log.debug({
                err: err,
                bucket: bucket,
                requestId: opts.requestId
            }, 'Moray.getBucket: error reading metadata');
            callback(err);
        } else {
            log.debug({
                bucket: bucket,
                data: bucket_data,
                requestId: opts.requestId
            }, 'Moray.getBucket: done');
            callback(null, bucket_data);
        }
    });
};


Moray.prototype.delBucket = function delBucket(options, callback) {
    assert.object(options, 'options');
    assert.string(options.owner, 'options.owner');
    assert.string(options.key, 'options.key');
    assert.string(options.requestId, 'options.requestId');
    assert.func(callback, 'callback');

    var owner = options.owner;
    var bucket = options.key;

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var client = this.client;
    var log = this.log;
    var opts = {
        req_id: options.requestId,
        noCache: true
    };

    log.debug({
        bucket: bucket,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.delBucket: entered');

    client.deleteBucketNoVnode(owner, bucket, function (err, bucket_data) {
        if (err) {
            log.debug({
                err: err,
                bucket: bucket,
                requestId: opts.requestId
            }, 'Moray.delBucket: error reading metadata');
            callback(err);
        } else {
            log.debug({
                bucket: bucket,
                data: bucket_data,
                requestId: opts.requestId
            }, 'Moray.delBucket: done');
            callback(null, bucket_data);
        }
    });
};


Moray.prototype.putObject = function putObject(options, callback) {
    assert.string(opts.owner, 'opts.owner');
    assert.string(opts.bucket_id, 'opts.bucket_id');
    assert.string(opts.key, 'opts.key');
    assert.object(opts.log, 'opts.log');
    assert.number(opts.content_length, 'opts.content_length');
    assert.string(opts.content_md5, 'opts.content_md5');
    assert.string(opts.content_type, 'opts.content_type');
    assert.object(opts.headers, 'opts.headers');
    assert.object(opts.sharks, 'opts.sharks');
    assert.object(opts.props, 'opts.props');
    assert.func(callback, 'callback');

    var owner = opts.owner;
    var bucket_id = opts.bucket_id;
    var key = opts.key;
    var content_length = opts.content_length;
    var content_md5 = opts.content_md5;
    var content_type = opts.content_type;
    var headers = opts.headers;
    var sharks = opts.sharks;
    var props = opts.props;
    var log = opts.log;

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var client = this.client;

    client.putObjectNoVnode(owner, bucket_id, key, content_length, content_md5, content_type, headers, sharks, props, function (err, object_data) {
        if (err) {
            log.debug({
                err: err,
                bucket_id: bucket_id,
                key: key,
                requestId: opts.requestId
            }, 'Moray.putObject: error writing metadata');
            callback(err);
        } else {
            log.debug({
                bucket_id: bucket,
                key: key,
                data: object_data,
                requestId: opts.requestId
            }, 'Moray.putObject: done');
            callback(null, object_data);
        }
    });
};


Moray.prototype.getObject = function getObject(options, callback) {
    assert.object(options, 'options');
    assert.string(options.owner, 'options.owner');
    assert.string(options.owner, 'options.bucket_id');
    assert.string(options.key, 'options.key');
    assert.string(options.requestId, 'options.requestId');
    assert.func(callback, 'callback');

    var owner = options.owner;
    var bucket_id = options.bucket_id;
    var key = options.key;

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var client = this.client;
    var log = this.log;
    var opts = {
        req_id: options.requestId,
        noCache: true
    };

    log.debug({
        bucket_id: bucket_id,
        object: key,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.getObject: entered');

    client.getObjectNoVnode(owner, bucket_id, key, function (err, object_data) {
        if (err) {
            log.debug({
                err: err,
                bucket_id: bucket_id,
                key: key,
                requestId: opts.requestId
            }, 'Moray.getObject: error reading metadata');
            callback(err);
        } else {
            log.debug({
                bucket_id: bucket_id,
                key: key,
                data: object_data,
                requestId: opts.requestId
            }, 'Moray.getObject: done');
            callback(null, object_data);
        }
    });
};


Moray.prototype.delObject = function delObject(options, callback) {
    assert.object(options, 'options');
    assert.string(options.owner, 'options.owner');
    assert.string(options.owner, 'options.bucket_id');
    assert.string(options.key, 'options.key');
    assert.string(options.requestId, 'options.requestId');
    assert.func(callback, 'callback');

    var owner = options.owner;
    var bucket_id = options.bucket_id;
    var key = options.key;

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var client = this.client;
    var log = this.log;
    var opts = {
        req_id: options.requestId,
        noCache: true
    };

    log.debug({
        bucket_id: bucket_id,
        object: key,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.delObject: entered');

    client.deleteObjectNoVnode(owner, bucket_id, key, function (err, object_data) {
        if (err) {
            log.debug({
                err: err,
                bucket_id: bucket_id,
                key: key,
                requestId: opts.requestId
            }, 'Moray.delObject: error deleting metadata');
            callback(err);
        } else {
            log.debug({
                bucket_id: bucket_id,
                key: key,
                data: object_data,
                requestId: opts.requestId
            }, 'Moray.delObject: done');
            callback(null, object_data);
        }
    });
};


Moray.prototype.ping = function ping(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    if (!this.client) {
        process.nextTick(cb.bind(this, new Error('not connected')));
        return;
    }

    this.client.ping(opts, cb);
};


///--- Low level wrappers over the plain jane Moray Client

Moray.prototype.search = function search(options) {
    assert.object(options, 'options');
    assert.string(options.filter, 'options.filter');
    assert.string(options.requestId, 'options.requestId');

    if (!this.client)
        throw new Error('not connected');

    var client = this.client;
    var log = this.log;
    var opts = {
        limit: options.limit,
        no_count: options.no_count,
        offset: options.offset,
        req_id: options.requestId,
        sort: options.sort,
        req_id: options.requestId,
        hashkey: options.hashkey
    };

    log.debug({
        filter: options.filter,
        requestId: opts.requestId,
        opts: opts
    }, 'Moray.search: entered');
    return (client.findObjects(BUCKET, options.filter, opts));
};


Moray.prototype.close = function close(callback) {
    if (!this.client) {
        if (callback) {
            process.nextTick(function () {
                callback(new Error('not connected'));
            });
        }
    } else {
        if (callback)
            this.client.once('close', callback);
        this.client.close();
    }
};


Moray.prototype.toString = function toString() {
    return ('[object MorayRingBucketClient]');
};


///--- Exports

module.exports = {
    createMorayBucketClient: function createMorayBucketClient(opts) {
        return (new Moray(opts));
    }
};
