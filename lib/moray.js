// Copyright 2012 Joyent, Inc.  All rights reserved.
//
// This API contains all logic for CRUD on object metadata, which is
// stored in Moray.
//

var EventEmitter = require('events').EventEmitter;
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var moray = require('moray');
var once = require('once');
var vasync = require('vasync');



///--- Globals

var sprintf = util.format;

var BUCKET = 'manta';
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


var DELETE_LOG_BUCKET = 'manta_delete_log';
var DELETE_LOG_SCHEMA = {
        objectId: {
                type: 'string'
        }
};



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
                owner: options.owner,
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
                                assert.string(s.url, 'shark.url');
                                assert.string(s.server_uuid,
                                              'shark.server_uuid');
                                assert.string(s.zone_uuid, 'shark.zone_uuid');
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

        this.init();
}
util.inherits(Moray, EventEmitter);
module.exports = {
        createMorayClient: function createMorayClient(opts) {
                return (new Moray(opts));
        }
};


Moray.prototype.init = function init() {
        var log = this.log;
        var self = this;

        log.debug('Moray.init: entered');

        var client = moray.createClient({
                connectTimeout: self.connectTimeout,
                log: self.log,
                host: self.host,
                port: self.port,
                reconnect: true,
                retry: self.retry
        });

        function onConnect() {
                client.removeListener('error', onError);
                //Create the manta and manta_delete_log buckets
                vasync.forEachParallel({
                        func: createBucket,
                        inputs: [ {
                                client: client,
                                bucket: BUCKET,
                                opts: { index: SCHEMA, post: POST },
                                log: log
                        }, {
                                client: client,
                                bucket: DELETE_LOG_BUCKET,
                                opts: { index: DELETE_LOG_SCHEMA },
                                log: log
                        }]
                }, function (err, results) {
                        if (err) {
                                log.error({
                                        bucket: err.bucket,
                                        err: err,
                                        opts: err.opts
                                }, 'Moray.init: error');
                                self.emit('error', err);
                        }
                        log.debug('Moray.init: done');
                        self.client = client;

                        client.on('close', self.emit.bind(self, 'close'));
                        client.on('connect', self.emit.bind(self, 'connect'));
                        client.on('error', self.emit.bind(self, 'error'));

                        self.emit('connect');
                });
        }

        function onError(err) {
                client.removeAllListeners('connectAttempt');
                client.removeListener('connect', onConnect);
                self.emit('error', err);
        }

        client.on('connectAttempt', self.emit.bind(self, 'connectAttempt'));
        client.once('connect', onConnect);
        client.once('error', onError);
};


Moray.prototype.putMetadata = function putMetadata(options, callback) {
        assert.object(options, 'options');
        assert.string(options.key, 'options.key');
        assert.string(options.requestId, 'options.requestId');
        assert.object(options, 'options.previousMetadata');
        assert.func(callback, 'callback');

        callback = once(callback);

        if (!this.client) {
                process.nextTick(function () {
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
                process.nextTick(function () {
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
                process.nextTick(function () {
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
