// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var once = require('once');
var uuid = require('node-uuid');

var libmanta = require('../lib');

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var before = helper.before;
var after = helper.after;
var test = helper.test;



///--- Helpers

function makeKey(customer, path) {
        return ('/' + customer + '/stor' + (path || ''));
}


function makeOpts(opts) {
        opts = opts || {};
        var id = opts.objectId || uuid();
        var owner = opts.owner || uuid();
        var key = makeKey(owner, (opts.path || '/' + id));

        var _opts = {
                objectId: id,
                owner: owner,
                key: key,
                requestId: uuid(),
                type: opts.type || 'object'
        };

        switch (_opts.type) {
        case 'object':
                _opts.contentLength = Math.floor(Math.random() * 1025);
                _opts.contentMD5 = 'MHhkZWFkYmVlZg==';
                _opts.contentType = 'text/plain';
                _opts.sharks = [ {
                        url: 'http://foo.bar.com',
                        server_uuid: uuid(),
                        zone_uuid: uuid()
                } ];
                break;

        case 'link':
                _opts.link = makeOpts();
                break;

        default:
                break;
        }

        return (_opts);
}



///--- Tests

before(function (cb) {
        cb = once(cb);
        if (!process.env.INDEX_URLS) {
                cb(new Error('INDEX_URLS=$1,$2,... must be specified'));
                return;
        }

        var urls = process.env.INDEX_URLS.split(',').filter(function (s) {
                return (s && s.length > 0);
        });

        this.ring = libmanta.createIndexRing({
                log: helper.createLogger(),
                replicas: 100,
                urls: urls,
                connectTimeout: 1000,
                retry: {
                        retries: 1,
                        minTimeout: 1,
                        maxTimeout: 10,
                        factor: 1
                },
                noReconnect: true
        });
        this.ring.once('error', cb);
        this.ring.once('ready', cb);
});


after(function (cb) {
        if (!this.ring) {
                cb();
                return;
        }

        this.ring.once('close', cb);
        this.ring.close();
});


test('getClientByKey matches getClient', function (t) {
        var k = '/foo/bar/baz.txt';
        var c1 = this.ring.getClientByKey(k);
        t.ok(c1);
        var c2 = this.ring.getClient('/foo/bar');
        t.ok(c1);
        t.ok(c2);
        t.equal(c1.url, c2.url);
        t.end();
});


test('ensure we still hash ok', function (t) {
        var k = '/foo/bar/baz.txt';
        var c1 = this.ring.getClientByKey(k);
        t.ok(c1);
        var c2 = this.ring.getClient('/foo/bar');
        t.ok(c2);
        t.equal(c1.url, c2.url);
        t.end();
});


test('check for "root" directory', function (t) {
        var k = '/ffa80dc0-bbf9-11e1-afa7-0800200c9a66/stor';
        var c1 = this.ring.getClientByKey(k);
        t.ok(c1);
        var c2 = this.ring.getClient(k);
        t.ok(c2);
        t.equal(c1.url, c2.url);
        t.end();
});


test('putMetadata (object) not root', function (t) {
        var opts = makeOpts();
        this.ring.putMetadata(opts, function (err, md) {
                t.ifError(err);
                t.ok(md);
                // We check dirname as that's what's hashed, lazily
                // assume the other stuff is copied right
                t.equal(md.dirname, '/' + opts.owner + '/stor');
                t.end();
        });
});


test('putMetadata (object) root', function (t) {
        var opts = makeOpts({path: ''});
        this.ring.putMetadata(opts, function (err, md) {
                t.ifError(err);
                t.ok(md);
                t.equal(md.dirname, '/' + opts.owner + '/stor');
                t.end();
        });
});


test('putMetadata (link) not root', function (t) {
        var opts = makeOpts({type: 'link'});
        this.ring.putMetadata(opts, function (err, md) {
                t.ifError(err);
                t.ok(md);
                t.equal(md.dirname, '/' + opts.owner + '/stor');
                t.ok(md.createdFrom);
                t.notEqual(md.createdFrom, opts.key);
                t.end();
        });
});


test('putMetadata (directory) root', function (t) {
        var opts = makeOpts({type: 'directory'});
        this.ring.putMetadata(opts, function (err, md) {
                t.ifError(err);
                t.ok(md);
                t.equal(md.dirname, '/' + opts.owner + '/stor');
                t.end();
        });
});

// Need resharding
// test('add a node, and ensure we get an update', function (t) {
//         this.ring.once('update', function (ring) {
//                 t.ok(ring);
//                 t.end();
//         });
//         this.ring.addShard('tcp://2.moray.' + LAB_HOST + '.joyent.us:2020');
// });
