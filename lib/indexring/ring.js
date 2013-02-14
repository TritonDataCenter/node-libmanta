// Copyright 2012 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var path = require('path');
var url = require('url');
var util = require('util');

var assert = require('assert-plus');
var fash = require('fash');
var once = require('once');

var logger = require('./logger');
var Moray = require('./moray');



///--- Globals

/* JSSTYLED */
var ROOT_RE = /^\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\/stor$/;



///--- Helpers

function sanitizeURL(u) {
        var obj = url.parse(u);
        var _url =
                obj.protocol + '//' +
                obj.host +
                path.normalize((obj.pathname || '/'));

        return (_url);
}


function createMorayClient(opts, cb) {
        assert.object(opts, 'options');
        assert.number(opts.connectTimeout, 'options.connectTimeout');
        assert.string(opts.host, 'options.host');
        assert.object(opts.log, 'options.log');
        assert.number(opts.port, 'options.port');
        assert.object(opts.retry, 'options.retry');
        assert.func(cb, 'callback');

        cb = once(cb);
        var client = new Moray(opts);
        var log = opts.log;

        client.once('error', function (err) {
                client.removeAllListeners('connect');

                log.debug(err, 'ring.moray: failed to connect');
                cb(err);
        });

        client.once('connect', function onConnect() {
                client.removeAllListeners('error');

                log.debug({
                        host: opts.host,
                        port: opts.port
                }, 'ring.moray: connected');
                cb(null, client);
        });
}



///--- API

/**
 * Represents a consistent hash-ring of Moray shards, and allows you to
 * retrieve a moray client for a given customer key.
 *
 * This API uses yjxiao's [node-fash](https://github.com/yunong/node-fash), so
 * it is expected that you pass in a consistent set of URLs, and configuration
 * otherwise you're going to be SOL.
 *
 * It's ok to incrementally grow the ring, but after you place a new shard in,
 * you'll need to save that configuration back somewhere so the next invocation
 * picks it up.  You can either listen for the 'update' event, or just grab
 * myRing.ring after you update the topology.
 *
 * Once you've made one of these things, you pretty much always want to be
 * using the `getClientByKey` API, as that will hash out the customer key
 * to the right shard based on dirname.
 *
 * @param {object} options
 */
function Ring(options) {
        EventEmitter.call(this);
        var self = this;

        var urls = (options.urls || []).map(function (u) {
                return (sanitizeURL(u));
        });

        this.chash = new fash.createHash({
                algorithm: (options.algorithm || 'sha256'),
                log: options.log,
                nodes: urls,
                numberOfReplicas: options.replicas
        });
        this.connectTimeout = options.connectTimeout || 500;
        this.log = options.log.child({component: 'ring'}, true);
        this.morays = {};
        this.noReconnect = options.noReconnect || false;
        this.retry = options.retry;

        this.chash.on('update', this.emit.bind(this, 'update'));

        this.__defineGetter__('ring', function () {
                return (self.chash.ring);
        });

        var ready = 0;
        urls.forEach(function (u) {
                self.addShard(u, true, function (err, client) {
                        if (err) {
                                self.emit('error', err);
                        } else if (++ready === urls.length) {
                                self.emit('ready');
                        }
                });
        });
}
util.inherits(Ring, EventEmitter);
module.exports = Ring;


///--- Metadata APIs (i.e., high level things)

/**
 * Given a key and some information snagged out of a request, write a metadata
 * record out to the correct place on the hash ring.
 *
 * options is a big object that is context-sensitive based on what it is you're
 * saving (i.e, an object vs a link vs a directory).  Here's a quasi-schema:
 *
 * struct shark {
 *         url <string>,               // obvious
 *         zone_uuid <string>,         // mako's zone name
 *         server_uuid <string>,       // mako's CN uuid
 * }
 *
 * struct object {
 *         contentLength <number>,     // object size
 *         contentMD5 <string>,        // obvious
 *         contentType <string>,       // obvious
 *         objectId <string>,          // The uuid *you* generated
 *         sharks [<shark>],           // list of mako shark objects (see above)
 * }
 *
 * struct link {
 *        $struct object fields,       // everything from above
 *        createdFrom <string>,        // the original key this was copied from
 * }
 *
 * struct metadata {
 *         dirname <string>,           // parent directory
 *         key <string>,               // customer key
 *         mtime <number>,             // epoch ms
 *         owner <string>,             // customer uuid
 *         type <enum>,                // one of directory' || 'object'
 *         UNION {                     // no field - the structs above are just
 *                 struct object,      // present depending on type
 *                 struct link         // type gets written as object if link
 *         }
 * }
 *
 * @param {object} options
 * @param {function} callback -> f(err)
 */
Ring.prototype.putMetadata = function putMetadata(options, callback) {
        assert.object(options, 'options');
        assert.string(options.key, 'options.key');
        assert.func(callback, 'callback');

        var morayClient = this.getClientByKey(options.key);
        morayClient.putMetadata(options, callback);
};


/**
 * Retrieves the metadata for a given key, by finding the shard on the ring,
 * and then performing the moray operation for you.  See above for the stuff
 * this thing will dump on you.
 *
 * @param {object} options -> specify 'key'
 * @param {function} callback -> f(err, metadata)
 */
Ring.prototype.getMetadata = function getMetadata(options, callback) {
        assert.object(options, 'options');
        assert.string(options.key, 'options.key');
        assert.func(callback, 'callback');

        var morayClient = this.getClientByKey(options.key);
        morayClient.getMetadata(options, callback);
};


/**
 * Retrieves the metadata for a given key, by finding the shard on the ring,
 * and then performing the moray operation for you.  See above for the stuff
 * this thing will dump on you.
 *
 * @param {object} options -> specify 'key'
 * @param {function} callback -> f(err, metadata)
 */
Ring.prototype.delMetadata = function delMetadata(options, callback) {
        assert.object(options, 'options');
        assert.string(options.key, 'options.key');
        assert.func(callback, 'callback');

        var morayClient = this.getClientByKey(options.key);
        morayClient.delMetadata(options, callback);
};


///--- Low-level ring APIs (i.e., things you probably don't need)

/**
 * Adds a shard into the ring (only takes the URL).
 *
 * Note this method also creates a moray client object and overwrites
 * whatever was there in the reverse mapping table.  You can use the
 * `alreadyInRing` parameter to drive just creating a client, and not
 * mucking with the consistent hash topology.  Although, realistically
 * only the constructor needs that.
 *
 * @param {string} url
 * @param {boolean} [optional] alreadyInRing.
 * @param {function} [optional] callback.
 *
 * @return {MorayClient} the client mapping to this URL.
 */
Ring.prototype.addShard = function addShard(u, alreadyInRing, cb) {
        assert.string(u, 'url');
        if (typeof (alreadyInRing) === 'function') {
                alreadyInRing = false;
                cb = alreadyInRing;
        }
        assert.optionalBool(alreadyInRing, 'alreadyInRing');
        assert.optionalFunc(cb, 'callback');

        var log = this.log;
        var self = this;
        var _u = sanitizeURL(u);
        var _url = url.parse(_u);

        log.debug({
                url: _u,
                alreadyInRing: alreadyInRing || false
        }, 'addShard: entered');

        var opts = {
                connectTimeout: self.connectTimeout,
                host: _url.hostname,
                log: self.log,
                port: parseInt(_url.port || 2020, 10),
                retry: self.retry
        };

        cb = once(cb || function (err, client) {
                if (err) {
                        self.emit('error', err);
                }
        });

        createMorayClient(opts, function (err, client) {
                if (err) {
                        cb(err);
                        return;
                }

                self.morays[_u] = client;

                client.on('close', self.emit.bind(self, 'close', client));
                client.on('connect', self.emit.bind(self, 'connect', client));
                client.on('connectAttempt',
                          self.emit.bind(self, 'connectAttempt'));
                client.on('error', self.emit.bind(self, 'error'));

                self.emit('connect', client);

                process.nextTick(function onMorayReady() {
                        cb(null, client);
                });
        });
};


/**
 * Takes a direct user-defined path to an object in manta and returns a
 * MorayClient object (note, the wrapped one - bucket is already set).
 *
 * Recall that we hash keys based on the parent directory.  So,
 * `/mark/stor/home/mcavage/foo.txt` would be located on the ring wherever
 * `/mark/stor/home/mcavage` is located.  As would all other "things" in
 * that "directory".
 *
 * So this method assumes that you're passing it in the user key "mostly"
 * untouched.  "mostly" in the sense that you need to have already resolved
 * the customer login name to :uuid. So in the example above, we would go from:
 *
 * /mark/stor/home/mcavage/foo.txt
 *
 * =>
 *
 * /ff2f9a80-bbda-11e1-afa7-0800200c9a66/stor/home/mcavage/foo.txt
 *
 * Which again gets the shard from wherever
 * /ff2f9a80-bbda-11e1-afa7-0800200c9a66/stor/home/mcavage lands.
 *
 * @param {string} key
 * @return {MorayClient}
 */
Ring.prototype.getClientByKey = function getClientByKey(key) {
        assert.string(key, 'key');

        var client;
        var dir;
        var log = this.log;

        dir = ROOT_RE.test(key) ? key : path.dirname(key);
        client = this.getClient(dir);

        log.trace({
                key: key,
                dir: dir,
                client: client.url
        }, 'Ring.getClientByKey: done');

        return (client);
};


/**
 * "Raw" API to return a shard for a given string, exactly as-is.
 *
 * Use this only if you know what you're doing (you almost assuredly want
 * getClientByKey(key)).
 *
 * @param {key}
 * @return {MorayClient}
 */
Ring.prototype.getClient = function getClient(key) {
        assert.string(key, 'key');

        var client;
        var log = this.log;
        var _url;

        log.trace({
                key: key
        }, 'Ring.getClient: entered');

        _url = this.chash.getNode(key);

        client = this.morays[_url];
        assert.ok(client);

        log.trace({
                key: key,
                client: client.url
        }, 'Ring.getClient: entered');
        return (client);
};


// Really only useful for unit testing. You probably never want this.
Ring.prototype.destroy = function destroy(callback) {
        var finished = 0;
        var keys = Object.keys(this.morays);
        var self = this;

        callback = once(callback || function () {
                self.emit('close');
        });

        keys.forEach(function (k) {
                self.morays[k].removeAllListeners('close');
                self.morays[k].removeAllListeners('connect');
                self.morays[k].removeAllListeners('error');
                self.morays[k].once('close', function () {
                        if (++finished === keys.length && callback) {
                                callback();
                        }
                });
                self.morays[k].once('error', function () {});
                self.morays[k].close();
        });
};
Ring.prototype.close = Ring.prototype.destroy;


Ring.prototype.toString = function toString() {
        var str = '[object Ring <';
        str += 'connectTimeout=' + this.connectTimeout + ', ';
        str += 'urls=[' + Object.keys(this.morays).join(', ') + '], ';
        str += 'ring=[algorithm=' + this.chash.algorithm + ', ';
        str += 'replicas=' + this.chash.numberOfReplicas + ']';
        str += '>]';

        return (str);
};
