/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var once = require('once');

var libmanta = require('../lib');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');


///--- Globals

var before = helper.before;
var after = helper.after;
var test = helper.test;
var makeOpts = helper.makeOpts;

// For put / get test and common options (uuid's)
var gOpts = makeOpts({type: 'directory'});


///--- Setup

before(function (cb) {
    cb = once(cb);

    if (!process.env.ELECTRIC_MORAY) {
        cb(new Error('ELECTRIC_MORAY must be specified'));
        return;
    }

    this.ring = libmanta.createMorayClient({
        log: helper.createLogger(),
        morayOptions: {
            srvDomain: process.env.ELECTRIC_MORAY
        }
    });

    this.ring.once('error', cb);
    this.ring.once('connect', cb);
});


///--- Teardown

after(function (cb) {
    if (this.ring) {
        this.ring.close();
    }
    cb();
});


///--- Tests

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
    this.ring.putMetadata(gOpts, function (err, md) {
        t.ifError(err);
        t.ok(md);
        t.equal(md.dirname, '/' + gOpts.owner + '/stor');
        t.end();
    });
});

// Get back what we put in the putMetadata test above
test('getMetadata (directory) root', function (t) {
    this.ring.getMetadata(gOpts, function (err, md) {
        t.ifError(err);
        t.ok(md);
        t.equal(md.key, '/' + gOpts.owner + '/stor/' + gOpts.objectId);
        t.end();
    });
});
