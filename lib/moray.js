/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

//
// This API contains all logic for CRUD on object metadata, which is
// stored in Moray.
//

var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var moray = require('moray');
var once = require('once');
var vasync = require('vasync');
var VError = require('verror');

var utils = require('./utils');



///--- Globals

var sprintf = util.format;

var BUCKET = process.env.MANTA_RING_BUCKET || 'manta';
var BUCKET_VERSION = 1;
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

var DIR_COUNT_BUCKET = 'manta_directory_counts';
var DIR_COUNT_SCHEMA = {
    entries: {
        type: 'number'
    }
};
var DIR_COUNT_VERSION = 1;



///--- Internal Functions

/*
 * Create the Moray buckets used by Manta.
 */
function setupMantaBuckets(log, client, cb) {
    return (vasync.forEachParallel({
        func: createBucket,
        inputs: [ {
            client: client,
            bucket: BUCKET,
            opts: {
                index: SCHEMA,
                post: POST,
                options: {
                    version: BUCKET_VERSION
                }
            },
            log: log
        }, {
            client: client,
            bucket: DELETE_LOG_BUCKET,
            opts: {
                index: DELETE_LOG_SCHEMA,
                options: {
                    version: DELETE_LOG_VERSION
                }
            },
            log: log
        }, {
            client: client,
            bucket: DIR_COUNT_BUCKET,
            opts: {
                index: DIR_COUNT_SCHEMA,
                options: {
                    version: DIR_COUNT_VERSION
                }
            },
            log: log
        } ]
    }, function onPipelineDone(err) {
        /*
         * It's possible for these operations to fail if they overlap with
         * concurrent invocations of the same operation.  Among the errors that
         * have been observed from PostgreSQL here are:
         *
         *     - "tuple concurrently updated" (typically translated as a
         *       BucketConflictError by Moray)
         *
         *     - "deadlock detected" (which can happen if multiple callers
         *       attempt to add an index to the same bucket concurrently)
         *
         *     - "duplicate key value violates unique constraint"
         *
         *     - "column ... of relation ... already exists"
         *
         * From Moray, we can also see:
         *
         *     - "$bucket has a newer version than $version" (which can happen
         *       when a bucket is being upgraded).
         *
         * When these errors are reported, it's likely that at least one of the
         * concurrent operations will have succeeded, and historically we just
         * ignored these errors (i.e., we did not retry the operation).
         * However, it's difficult to keep this list up to date, and it's even
         * harder to actually verify correctness for these cases.  Instead, we
         * treat these like any other error, by failing this operation. The
         * caller will retry.
         *
         * There are two potential problems with retrying so liberally:
         *
         *    (1) If the errors are induced by concurrent requests and each of
         *        the callers retries with the same delay, convergence may take
         *        a very long time.  The caller avoids this using randomized
         *        exponential backoff.
         *
         *    (2) If the errors are common, then even a quick convergence might
         *        take several seconds, during which consumers like Muskie may
         *        be responding with 503 errors.  These errors should not be
         *        that common, however: most of the time when we start up, the
         *        buckets already exist with the expected version, so we will
         *        not try to make any changes, and we won't run into these
         *        errors.  We should only see these when multiple components
         *        start up concurrently that both decide they need to create or
         *        upgrade the buckets.
         */
        if (err) {
            err = new VError(err, 'setupMantaBuckets');
        }

        cb(err);
    }));
}

/*
 * We use a PostgreSQL trigger to maintain a separate table of sizes for each
 * directory.  We install that trigger immediately after creating the Manta
 * buckets in Moray.  This step is idempotent.
 */
function setupMantaTrigger(log, client, cb) {
    var readoptions, updatesql, funcsql;

    readoptions = { 'encoding': 'utf8' };

    return (vasync.waterfall([
        function readUpdateFunction(callback) {
            var filepath = path.join(__dirname, 'trigger_update.plpgsql');
            log.trace('setupMantaTrigger: read "%s"', filepath);
            fs.readFile(filepath, readoptions, function (err, c) {
                if (!err)
                    updatesql = c;
                callback(err);
            });
        },

        function readTriggerFunction(callback) {
            var filepath = path.join(__dirname, 'trigger_dircount.plpgsql');
            log.trace('setupMantaTrigger: read "%s"', filepath);
            fs.readFile(filepath, readoptions, function (err, c) {
                if (!err)
                    funcsql = c;
                callback(err);
            });
        },

        function updateTrigger(callback) {
            var sql, req;

            sql = updatesql + '\n' + funcsql;
            log.info({ 'sql': sql }, 'setupMantaTrigger: apply update');
            callback = once(callback);
            req = client.sql(sql);
            req.on('record', function (row) {
                log.info(row, 'setupMantaTrigger: row');
            });
            req.once('error', callback);
            req.once('end', callback);
        }
    ], function (err) {
        if (err) {
            err = new VError(err, 'setupMantaTrigger');
        }

        cb(err);
    }));
}


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

    //now log to the manta_delete_log table...
    var now = Math.round((microtime.now() / 1000));
    var _key = '/' + prevObjectId + '/' + now;
    var _value = JSON.stringify(prevmd);
    var _etag = crc.hex32(crc.crc32(_value));
    var _mtime = now;
    var objectId = prevObjectId;
    var sql = 'INSERT INTO manta_delete_log (_key, _value, _etag, ' +
        '_mtime, objectId) VALUES ($1, $2, $3, $4, $5)';
    var values = [_key, _value, _etag, _mtime, objectId];

    //execute
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
    assert.object(options, 'options');
    assert.string(options.key, 'options.key');
    assert.string(options.owner, 'options.owner');
    assert.string(options.type, 'options.type');
    assert.optionalObject(options.headers, 'options.headers');

    var key = options.key;
    var md = {
        dirname: ROOT_RE.test(key) ? key : path.dirname(key),
        key: key,
        headers: (options.type !== 'link' ?
                  clone(options.headers || {}) : undefined),
        mtime: Date.now(),
        name: path.basename(key),
        creator: options.creator || options.owner,
        owner: options.owner,
        roles: options.roles,
        type: options.type
    };

    switch (options.type) {
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

    case 'directory':
        // noop
        break;

    default:
        break;
    }

    return (md);
}


function createBucket(opts, cb) {
    var bucket = opts.bucket;
    var client = opts.client;

    client.putBucket(bucket, opts.opts, function (err) {
        if (err) {
            err.bucket = opts.bucket;
            err.opts = opts.opts;
            cb(err);
        } else {
            opts.log.debug(opts.opts, 'Moray.createBucket done');
            cb();
        }
    });
}



///--- API

function Moray(options) {
    var self = this;

    EventEmitter.call(this);

    this.connectTimeout = options.connectTimeout || 1000;
    this.client = null;
    this.host = options.host;
    this.log = options.log.child({
        component: 'MorayIndexClient',
        host: options.host,
        port: (options.port || 2020)
    }, true);
    this.port = parseInt(options.port || 2020, 10);
    this.retry = options.retry;
    this.url = 'tcp://' + options.host + ':' + (options.port || 2020);

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

    this.initPipeline = vasync.waterfall([
        function initClient(callback) {
            self.client = moray.createClient({
                connectTimeout: self.connectTimeout,
                log: self.log,
                host: self.host,
                port: self.port,
                retry: self.retry,
                unwrapErrors: true
            });

            var onErrorDuringConnect = function onErrDuringConnect(err) {
                callback(new VError(err, 'moray client error'));
            };

            self.client.on('error', onErrorDuringConnect);
            self.client.once('connect', function onConnect() {
                self.client.removeListener('error', onErrorDuringConnect);
                self.client.on('error', self.onErrorDuringInit);
                callback();
            });
        },

        function setupBuckets(callback) {
            self.initBuckets = setupMantaBuckets(log, self.client, callback);
        },

        function setupTrigger(callback) {
            self.initTrigger = setupMantaTrigger(log, self.client, callback);
        }
    ], function (err) {
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
             * We could reset the "backoff" object in the success case, or even
             * null it out since we're never going to use it again.  But it's
             * not that large, and it may be useful for debugging, so we just
             * leave it alone.
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

Moray.prototype.putMetadata = function putMetadata(options, callback) {
    assert.object(options, 'options');
    assert.string(options.key, 'options.key');
    assert.string(options.requestId, 'options.requestId');
    assert.object(options, 'options.previousMetadata');
    assert.func(callback, 'callback');

    callback = once(callback);

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var attempts = 0;
    var client = this.client;
    var key = options.key;
    var log = this.log;
    var md = createMetadata(options);
    var opts = {
        req_id: options.requestId,
        etag: options._etag,
        headers: {
            'x-muskie-prev-metadata': options.previousMetadata
        }
    };

    log.debug({
        key: key,
        metadata: md,
        etag: opts.etag,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.putMetadata: entered');
    (function put() {
        client.putObject(BUCKET, key, md, opts, function (err) {
            if (err) {
                log.debug({
                    err: err,
                    key: key,
                    requestId: opts.requestId
                }, 'Moray.putMetadata: error writing metadata');

                if ((err.name === 'EtagConflictError' ||
                     err.name === 'UniqueAttributeError') &&
                    opts.etag === undefined && ++attempts < 3) {
                    process.nextTick(put);
                } else {
                    callback(err);
                }
            } else {
                log.debug({
                    key: key,
                    requestId: opts.requestId
                }, 'Moray.putMetadata: done');
                callback(null, md);
            }
        });
    })();
};


Moray.prototype.getMetadata = function getMetadata(options, callback) {
    assert.object(options, 'options');
    assert.string(options.key, 'options.key');
    assert.string(options.requestId, 'options.requestId');
    assert.func(callback, 'callback');

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var client = this.client;
    var key = options.key;
    var log = this.log;
    var opts = {
        req_id: options.requestId,
        noCache: true
    };

    log.debug({
        key: key,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.getMetadata: entered');

    client.getObject(BUCKET, key, opts, function (err, md) {
        if (err) {
            log.debug({
                err: err,
                key: key,
                requestId: opts.requestId
            }, 'Moray.getMetadata: error reading metadata');
            callback(err);
        } else {
            log.debug({
                key: key,
                metadata: md.value,
                requestId: opts.requestId
            }, 'Moray.getMetadata: done');
            callback(null, md.value, md);
        }
    });
};


Moray.prototype.delMetadata = function delMetadata(options, callback) {
    assert.object(options, 'options');
    assert.string(options.key, 'options.key');
    assert.string(options.requestId, 'options.requestId');
    assert.object(options, 'options.previousMetadata');
    assert.func(callback, 'callback');

    if (!this.client) {
        setImmediate(function () {
            callback(new Error('not connected'));
        });
        return;
    }

    var attempts = 0;
    var client = this.client;
    var key = options.key;
    var log = this.log;
    var opts = {
        req_id: options.requestId,
        etag: options._etag,
        headers: {
            'x-muskie-prev-metadata': options.previousMetadata
        }
    };

    log.debug({
        key: key,
        etag: opts.etag,
        requestId: opts.requestId,
        headers: opts.headers
    }, 'Moray.delMetadata: entered');
    (function del() {
        client.delObject(BUCKET, key, opts, function (err) {
            if (err) {
                log.debug({
                    err: err,
                    key: key,
                    requestId: opts.requestId
                }, 'Moray.delMetadata: error');
                if ((err.name === 'EtagConflictError' ||
                     err.name === 'UniqueAttributeError') &&
                    opts.etag === undefined && ++attempts < 3) {
                    process.nextTick(del);
                } else {
                    callback(err);
                }
            } else {
                log.debug({
                    key: key,
                    requestId: opts.requestId
                }, 'Moray.delMetadata: done');
                callback(null);
            }
        });
    })();
};


Moray.prototype.getDirectoryCount = function getDirectoryCount(options, cb) {
    assert.object(options, 'options');
    assert.string(options.directory, 'options.directory');
    assert.string(options.requestId, 'options.requestId');
    assert.func(cb, 'callback');

    cb = once(cb);

    if (!this.client) {
        setImmediate(function () {
            cb(new Error('not connected'));
        });
        return;
    }

    var client = this.client;
    var dir = options.directory;
    var log = this.log;
    var opts = {
        req_id: options.requestId,
        noCache: true
    };

    log.debug({
        dir: dir,
        requestId: opts.requestId
    }, 'Moray.getDirectoryCount: entered');

    client.getObject(DIR_COUNT_BUCKET, dir, opts, function (err, obj) {
        if (err) {
            cb(err);
        } else {
            var count = parseInt(obj.value.entries, 10);
            log.debug({
                dir: dir,
                count: count,
                requestId: opts.requestId
            }, 'Moray.getDirectoryCount: done');
            cb(null, count, obj);
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
    // ugly...
    var c = (((this.client || {}).client || {}).conn || {});
    var str = sprintf('[object MorayRingClient<url=%s, remote=%s:%s>]',
                      this.url,
                      c.remoteAddress, c.remotePort);
    return (str);
};



///--- Exports

module.exports = {
    createMorayClient: function createMorayClient(opts) {
        return (new Moray(opts));
    }
};



///--- Tests

function _test() {
    var bunyan = require('bunyan');

    var log = bunyan.createLogger({
        name: 'moray_test',
        stream: process.stdout,
        level: process.env.LOG_LEVEL || 'trace',
        serializers: bunyan.stdSerializers
    });

    var clients = [];
    for (var i = 0; i < 20; i++) {
        clients.push(new Moray({
            connectTimeout: 4000,
            host: 'electric-moray.coal.joyent.us',
            port: 2020,
            log: log
        }));

        clients[clients.length - 1].once('connect', function () {
            clients[clients.length - 1].close();
        });
    }
}

if (require.main === module)
    _test();
