/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var once = require('once');
var libuuid = require('libuuid');



///--- Helpers

function createLogger(name, stream) {
    var log = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'warn'),
        name: name || process.argv[1],
        stream: stream || process.stdout,
        src: true,
        serializers: bunyan.stdSerializers
    });
    return (log);
}

///--- Helpers

function makeKey(customer, path) {
    return ('/' + customer + '/stor' + (path || ''));
}


function makeOpts(opts) {
    opts = opts || {};
    var id = opts.objectId || libuuid.create();
    var owner = opts.owner || libuuid.create();
    var key = makeKey(owner, (opts.path || '/' + id));

    var _opts = {
        objectId: id,
        owner: owner,
        key: key,
        requestId: libuuid.create(),
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

///--- Exports

module.exports = {

    after: function after(teardown) {
        module.parent.exports.tearDown = function _teardown(callback) {
            try {
                teardown.call(this, callback);
            } catch (e) {
                console.error('after:\n' + e.stack);
                process.exit(1);
            }
        };
    },

    before: function before(setup) {
        module.parent.exports.setUp = function _setup(callback) {
            try {
                setup.call(this, callback);
            } catch (e) {
                console.error('before:\n' + e.stack);
                process.exit(1);
            }
        };
    },

    test: function test(name, tester) {
        module.parent.exports[name] = function _(t) {
            t.end = once(t.done.bind(t));
            t.notOk = function notOk(ok, message) {
                return (t.ok(!ok, message));
            };
            tester.call(this, t);
        };
    },

    createLogger: createLogger,

    makeOpts: makeOpts
};
