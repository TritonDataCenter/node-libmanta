// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var dns = require('dns');
var domain = require('domain');
var EventEmitter = require('events').EventEmitter;
var net = require('net');
var util = require('util');

var assert = require('assert-plus');
var backoff = require('backoff');
var extend = require('xtend');
var LRU = require('lru-cache');
var once = require('once');
var redis = require('redis');
redis.debug_mode = process.env.REDIS_DEBUG ? true : undefined;

var utils = require('./utils');
require('./errors');


///--- Globals

var sprintf = util.format;

var shuffle = utils.shuffle;



///--- Internal Functions

/**
 * The basic constructor for a redis client.  This is a one time
 * call and returns either an error or a client that's good to go.
 *
 * var opts = {
 *   connectTimeout: 1000,
 *   host: '10.1.2.3',
 *   log: $bunyan,
 *   options: {
 *     // anything that can be passed into node_redis options
 *   },
 *   port: 6379
 * };
 *
 * connect(opts, function (err, mahi) {
 *   assert.ifError(err);
 *   ...
 * });
 *
 */
function connect(opts, cb) {
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        cb = once(cb);
        var log = opts.log;

        dns.resolve4(opts.host, function (dns_err, addrs) {
                if (dns_err && dns_err.code == dns.NOTFOUND &&
                    net.isIP(opts.host) != 0) {
                        log.debug({
                            host: opts.host
                        }, 'failed to resolve mahi, but host is an IP');
                        dns_err = null;
                        addrs = [ opts.host ];
                }

                if (dns_err) {
                        log.debug(dns_err, 'DNS call failed');
                        cb(new NameResolutionError(dns_err, 'mahi'));
                        return;
                } else if (!addrs || addrs.length === 0) {
                        log.debug('mahi: no entries in DNS');
                        cb(new NameResolutionError('mahi'));
                        return;
                }

                log.debug({
                        host: opts.host,
                        addresses: addrs
                }, 'resolved mahi in DNS');

                var addr = shuffle(addrs).pop();
                var client = redis.createClient(opts.port, addr, opts.options);
                opts.domain.add(client);
                var t;
                var ct = opts.connectTimeout;

                function onConnectTimeout() {
                        client.removeAllListeners('error');
                        client.removeAllListeners('ready');

                        log.debug('connection timeout');
                        cb(new ConnectTimeoutError('mahi', ct));
                }

                client.once('error', function (err) {
                        client.removeAllListeners('ready');
                        clearTimeout(t);

                        log.debug(err, 'failed to connect');
                        cb(new NotConnectedError(err, 'mahi', addr));
                });

                client.once('ready', function onConnect() {
                        client.removeAllListeners('error');
                        clearTimeout(t);

                        log.debug({
                                host: opts.host,
                                port: opts.port
                        }, 'connected');
                        cb(null, client);
                });

                t = setTimeout(onConnectTimeout, ct);
        });
}


/**
 * The retry/backoff constructor for a redis client.  This is a one time
 * call and returns either an error or a client that's good to go.
 *
 * var opts = {
 *   connectTimeout: 1000,
 *   host: '10.1.2.3',
 *   log: $bunyan,
 *   options: {
 *     // anything that can be passed into node_redis options
 *   },
 *   port: 6379,
 *   retries: 3
 * };
 *
 * connect(opts, function (err, mahi) {
 *   assert.ifError(err);
 *   ...
 * });
 *
 */
function createRedisClient(opts, cb) {
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        cb = once(cb);

        var log = opts.log;
        var _opts = {
                connectTimeout: opts.connectTimeout || 2000,
                domain: opts.domain,
                host: opts.host,
                log: log,
                options: extend({max_attempts: 1}, opts.options),
                port: opts.port
        };

        var retry = backoff.call(connect, _opts, function (err, client) {
                retry.removeAllListeners('backoff');
                var attempts = retry.getResults().length;
                log.debug('client acquired after %d attempts', attempts);
                cb(err, client);
        });

        retry.setStrategy(new backoff.ExponentialStrategy({
                initialDelay: opts.minTimeout || 100,
                maxDelay: opts.maxTimeout || 60000
        }));
        retry.failAfter(opts.retries || Infinity);

        retry.on('backoff', function (number, delay) {
                var level = utils.getLogLevel(number);
                log[level]({
                        attempt: number,
                        delay: delay
                }, 'connection attempted');
        });

        retry.start();
}



///--- Mahi APIs

/**
 * The constructor for a Mahi client.  This constructor handles
 * DNS resolution, retry/backoff for connections, etc.  You should
 * call it like this:
 *
 * var opts = {
 *   cache: {
 *     max: 1000,
 *     maxAge: 300
 *   },
 *   connectTimeout: 1000,
 *   host: '10.1.2.3',
 *   log: $bunyan,
 *   redis_options: {
 *     // anything that can be passed into node_redis options
 *   },
 *   port: 6379,
 *   retry: {
 *     retries: 3
 *   }
 * };
 *
 * var mahi = libmanta.createMahiClient(opts);
 * mahi.once('error', function (err) {
 *   log.fatal(err, 'I am screwed');
 *   process.exit(1);
 * });
 *
 * mahi.once('connect', function () {
 *   log.info('mahi good to go!');
 *   ...
 * });
 */
function MahiClient(opts) {
        assert.object(opts, 'options');
        assert.string(opts.host, 'options.host');
        assert.object(opts.log, 'options.log');
        assert.optionalObject(opts.cache, 'options.cache');
        opts.cache = opts.cache || {};
        assert.optionalNumber(opts.connectTimeout, 'options.connectTimeout');
        assert.optionalNumber(opts.checkInterval, 'options.checkInterval');
        assert.optionalNumber(opts.port, 'options.port');
        assert.optionalObject(opts.redis_options, 'options.redis_options');
        assert.optionalNumber(opts.retries, 'options.retries');
        assert.optionalNumber(opts.minTimeout, 'options.minTimeout');
        assert.optionalNumber(opts.maxTimeout, 'options.maxTimeout');

        EventEmitter.call(this);

        this.cache = LRU({
                max: opts.cache.max || 1000,
                maxAge: opts.cache.maxAge || 300
        });
        this.checkInterval = opts.checkInterval || 10000;
        this.host = opts.host;
        this.log = opts.log.child({component: 'mahi'}, true);
        this.port = opts.port || 6379;
        this._whatami = 'MahiClient';
        this._url = 'tcp://' + this.host + ':' + this.port;

        var log = this.log;
        var self = this;
        var rOpts = {
                connectTimeout: opts.connectTimeout || 4000,
                domain: domain.create(),
                host: self.host,
                log: self.log,
                maxTimeout: opts.maxTimeout || Infinity,
                minTimeout: opts.minTimeout || 100,
                options: opts.redis_options || {},
                port: self.port,
                retries: opts.retries || 10
        };
        var _connect = createRedisClient.bind(this, rOpts, onNewRedisClient);

        function onNewRedisClient(init_err, client) {
                if (init_err) {
                        self.emit('error', init_err);
                        cleanup();
                        return;
                }

                function cleanup(err) {

                        if (!self._deadbeef) {
                                log.warn({
                                        err: err
                                }, 'redis error or close, reconnecting...');
                        }
                        if (client) {
                                client.removeAllListeners('end');
                                client.removeAllListeners('error');
                                client.stream.setTimeout(0);
                                client.end();
                        }
                        self.redis = null;
                        if (!self._deadbeef)
                                process.nextTick(_connect);
                }

                function redis_check() {
                        var _cb = once(function check_cb(err, replies) {
                                clearTimeout(self.checkTimer);
                                if (err) {
                                        log.warn(err, 'redis_check: failed');
                                        cleanup();
                                        return;
                                } else if (!replies) {
                                        log.warn('redis_check: no reply');
                                        cleanup();
                                        return;
                                }
                                log.debug('redis_check: ok');
                                client.stream.setTimeout(self.checkInterval,
                                                         redis_check);
                        });

                        self.checkTimer = setTimeout(function () {
                                _cb(new HealthCheckError('mahi', 'timeout'));
                        }, rOpts.connectTimeout);

                        client.info(_cb);
                }

                client.on('drain', self.emit.bind(self, 'drain'));
                client.on('end', cleanup);
                client.on('error', cleanup);

                self.redis = client;

                self.emit('connect');
        }

        // See:
        // https://github.com/mranney/node_redis/pull/310
        // Basically, redis violates the domain contract, and so we have
        // to create a domain, and pass it and add it immediately upon
        // creation, or else redis will emit 'error' to nowhere, causing
        // the program to crash
        rOpts.domain.add(redis);
        rOpts.domain.on('error', this.emit.bind(this, 'error'));
        rOpts.domain.run(_connect);
}
util.inherits(MahiClient, EventEmitter);


/**
 * Destroys the connection to redis.
 */
MahiClient.prototype.close = function close() {
        this._deadbeef = true;
        clearTimeout(this.checkTimer);
        if (this.redis) {
                this.redis.once('end', this.emit.bind(this, 'close'));
                this.redis.quit();
                this.redis = null;
        }
};


/**
 * Returns a user from redis given the login name.
 *
 * `opts` lets you override the logger (for req_id); it is optional.
 *
 * The returned object will look like this:
 *
 * {
 *    uuid: '56f237ac-8cb0-4468-896f-5aa00ff8ffc9',
 *    groups: [ 'operators' ],
 *    keys: {
 *        '24:ae:9d:...': 'PEM ENCODED PUBLIC KEY'
 *    },
 *    login: 'poseidon'
 * }
 *
 * mahi.userFromLogin('poseidon', function (err, user) {
 *   assert.ifError(err);
 *
 *   console.log('%j', user);
 * });
 */
MahiClient.prototype.userFromLogin = function userFromLogin(login, opts, cb) {
        assert.string(login, 'login');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        cb = once(cb);

        if (!this.redis) {
                cb(new NotConnectedError('mahi', this._url));
                return;
        }

        var log = opts.log || this.log;
        var record;
        var self = this;

        if ((record = this.cache.get(login))) {
                log.debug('mahi.userFromLogin: user=%s was cached', login);
                cb(null, record);
                return;
        }

        this.redis.get('/login/' + login, function (err, data) {
                if (err) {
                        cb(err);
                        return;
                } else if (!data) {
                        cb(new UserDoesNotExistError(login));
                        return;
                }

                try {
                        record = JSON.parse(data);
                        record.login = login;
                        record.groups = Object.keys(record.groups || {});
                } catch (e) {
                        log.error({
                                err: e,
                                login: login
                        }, 'Invalid JSON data in redis');
                        cb(new InvalidDataError(e, 'invalid JSON in mahi'));
                        return;
                }

                log.debug({
                        login: login,
                        user: record
                }, 'mahi.userFromLogin: done');
                self.cache.set(login, record);
                cb(null, record);
        });
};


/**
 * Returns a user from redis given the uuid
 *
 * `opts` lets you override the logger (for req_id); it is optional.
 *
 * The returned object will look like this:
 *
 * {
 *    uuid: '56f237ac-8cb0-4468-896f-5aa00ff8ffc9',
 *    groups: [ 'operators' ],
 *    keys: {
 *        '24:ae:9d:...': 'PEM ENCODED PUBLIC KEY'
 *    },
 *    login: 'poseidon'
 * }
 *
 * mahi.userFromUUID('56f237ac-8cb0-4468-896f-5aa00ff8ffc9', function (err, u) {
 *   assert.ifError(err);
 *
 *   console.log('%j', u);
 * });
 */
MahiClient.prototype.userFromUUID = function userFromUUID(uuid, opts, cb) {
        assert.string(uuid, 'uuid');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        cb = once(cb);

        if (!this.redis) {
                cb(new NotConnectedError('mahi', this._url));
                return;
        }

        var log = opts.log || this.log;
        var login;
        var self = this;

        if ((login = this.cache.get(uuid))) {
                log.debug('mahi.userFromUUID: uuid=%s was cached', uuid);
                this.userFromLogin(login, opts, cb);
                return;
        }

        this.redis.get('/uuid/' + uuid, function (err, _login) {
                if (err) {
                        cb(err);
                        return;
                } else if (!_login) {
                        cb(new UserDoesNotExistError(uuid));
                        return;
                }

                log.debug({
                        login: _login,
                        uuid: uuid
                }, 'mahi.userFromUUID: login found');
                self.cache.set(uuid, _login);
                self.userFromLogin(_login, opts, cb);
        });


};


/**
 * Returns the given set as an array from redis
 *
 * `opts` lets you override the logger (for req_id); it is optional.
 *
 * The returned object will look like this:
 *
 * [
 *      'key1',
 *      'key2',
 *      ...
 *      'keyn'
 * ]
 *
 * mahi.setMembers('uuid', function (err, uuids) {
 *   assert.ifError(err);
 *   assert.ok(uuids.length > 0);
 *
 *   for (var i = 0; i < uuids.length; i++) {
 *      console.log(uuids[i]);
 *   }
 * });
 */
MahiClient.prototype.setMembers = function setMembers(set, opts, cb) {
        assert.string(set, 'set');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        cb = once(cb);

        var log = opts.log || this.log;

        this.redis.smembers(set, function (err, members) {
                if (err) {
                        cb(err);
                        return;
                } else if (members.length === 0) {
                        cb(new EmptySetError(set));
                        return;
                }

                log.debug({
                        set: set,
                        members: members
                }, 'mahi.setMembers: done');
                cb(null, members);
        });
};


MahiClient.prototype.toString = function toString() {
        var str = sprintf('[object MahiClient <tcp://%s:%d>]',
                          this.host, this.port);

        return (str);
};



///--- Exports

module.exports = {
        createMahiClient: function createMahiClient(opts) {
                return (new MahiClient(opts));
        },

        MahiClient: MahiClient
};
