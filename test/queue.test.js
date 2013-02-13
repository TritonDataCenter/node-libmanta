// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var fs = require('fs');
var once = require('once');

var libmanta = require('../lib');


if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');


///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;



///--- Tests

test('run queue', function (t) {
        var bytes = 0;
        var q = libmanta.createQueue({
                limit: 5,
                worker: function stat(f, cb) {
                        fs.stat(f, function (err, stats) {
                                if (err) {
                                        cb(err);
                                        return;
                                }

                                if (stats.isFile())
                                        bytes += stats.size;

                                cb();
                        });
                }
        });
        t.ok(q);

        q.on('error', function (err) {
                t.ifError(err);
                t.end();
        });

        q.once('end', function () {
                t.ok(bytes);
                t.end();
        });

        fs.readdir('/tmp', function (err, files) {
                t.ifError(err);
                if (err) {
                        q.close();
                        t.end();
                        return;
                }
                files.forEach(function (f) {
                        q.push('/tmp/' + f);
                });
                q.close();
        });
});
