/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var once = require('once');
var libuuid = require('libuuid');
var vasync = require('vasync');

var libmanta = require('../lib');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');


///--- Globals

var before = helper.before;
var after = helper.after;
var test = helper.test;
var makeOpts = helper.makeOpts;


///--- Tests of read-only client

before(function (cb) {
    cb = once(cb);

    if (!process.env.ELECTRIC_MORAY) {
        cb(new Error('ELECTRIC_MORAY must be specified'));
        return;
    }

    /*
     * Create both a read-only and standard
     * moray client.
     */
    var barrier = vasync.barrier();
    var setupErr = null;

    this.tKey = process.env.TEST_GET_KEY;

    barrier.on('drain', function () {
        if (setupErr) {
            cb(setupErr);
        }
        cb();
    });

    barrier.start('read-only ring');
    this.roRing = libmanta.createMorayClient({
        readOnly: true,
        log: helper.createLogger(),
        morayOptions: {
            srvDomain: process.env.ELECTRIC_MORAY
        }
    });
    this.roRing.once('connect', function () {
        barrier.done('read-only ring');
    });
    this.roRing.once('error', function (err) {
        setupErr = err;
        barrier.done('read-only ring');
    });

    barrier.start('ring');
    this.ring = libmanta.createMorayClient({
        log: helper.createLogger(),
        morayOptions:  {
            srvDomain: process.env.ELECTRIC_MORAY
        }
    });
    this.ring.once('connect', function () {
        barrier.done('ring');
    });
    this.ring.once('error', function (err) {
        setupErr = err;
        barrier.done('ring');
    });

});


after(function (cb) {
    if (this.roRing) {
        this.roRing.close();
    }

    if (this.ring) {
        this.ring.close();
    }
    cb();
});

test('negative test: putMetadata on read-only client', function (t) {
    var opts = makeOpts();
    t.throws(
        function () {
            this.roRing.putMetadata(opts, function (err, md) {
                t.ifError(err);
                t.ok(md);
            });
        },
        'Error: putMetadata should throw with a read-only client');
    t.end();
});

test('negative test: putFinalizingMetadata on read-only client', function (t) {
    var opts = makeOpts();
    opts.md = new Object();
    opts.md.uploadId = 'Some strings';
    opts.md.finalizingType = 'that will';
    opts.md.owner = 'bypass';
    opts.md.requestId = 'all the';
    opts.md.objectPath = 'standard options';
    opts.md.objectId = 'of putFinalizingmetadata';
    t.throws(
        function () {
            this.roRing.putFinalizingMetadata(opts, function (err, md) {
                t.ifError(err);
                t.ok(md);
            });
        },
        'Error: putMetadata should throw with a read-only client');
    t.end();
});

test('getMetadata: read-only client', function (t) {
    // Use the read-write client to write an object we then read back
    var pOpts = makeOpts({type: 'directory'});
    var roRing = this.roRing;
    this.ring.putMetadata(pOpts, function (err, md) {
        var opts = {
            key: pOpts.key,
            requestId: libuuid.create()
        };
        roRing.getMetadata(opts, function (err2, md2) {
            t.ifError(err2);
            t.ok(md2);
            t.end();
        });
    });
});
