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
                        manta_storage_id: '1.stor.ring.test'
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
        var self = this;

        cb = once(cb);

        if (!process.env.ELECTRIC_MORAY) {
                cb(new Error('ELECTRIC_MORAY must be specified'));
                return;
        }

        var electricMoray = process.env.ELECTRIC_MORAY;

        this.ring = libmanta.createMorayClient({
                log: helper.createLogger(),
                host: electricMoray,
                port: 2020
        });

        this.ring.once('error', cb);
        this.ring.once('connect', cb);
});


after(function (cb) {
        if (this.ring)
                this.ring.close();
        cb();
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
