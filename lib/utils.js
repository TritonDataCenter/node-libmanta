// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');



///--- Globals

var PATH_LOGIN_RE = /^\/([a-zA-Z][a-zA-Z0-9_\-\.@%]+)\//;



///--- APIs

/**
 * Useful for backoff logging - pass in an attempt, and it returns the
 * appropriate bunyan log level as a key. i.e.,:
 *
 * log[getLogLevel(attempt)]('foo...');
 */
function getLogLevel(attempt) {
    var l;

    if (attempt === 0) {
        l = 'info';
    } else if (attempt < 5) {
        l = 'warn';
    } else {
        l = 'error';
    }

    return (l);
}


// Just shuffles an array in place, and returns what you passed in
function shuffle(array) {
    var current;
    var tmp;
    var top = array.length;

    if (top) {
        while (--top) {
            current = Math.floor(Math.random() * (top + 1));
            tmp = array[current];
            array[current] = array[top];
            array[top] = tmp;
        }
    }

    return (array);
}


/**
 * Given an resource like `/mark/stor/my%resume.pdf`, this API will convert to
 * `/mark/stor/my resume.pdf`; if you optionally pass in an `account` object as
 * returned from mahi.js (in this lib), the path will be converted to
 * `/d3d38d1a-e26c-11e2-9a8a-475696be42f1/stor/my resume.pdf`, which is the
 * correct normal form everwhere in the system.
 *
 * Note that because `decodeURIComponent` may throw, this function takes a
 * callback, even though it is not blocking.
 */
function normalizeMantaPath(opts, cb) {
    assert.object(opts, 'options');
    assert.string(opts.path, 'options.path');
    assert.optionalObject(opts.account, 'options.account');

    var err = null;
    var key;

    try {
        key = opts.path.split('/').map(decodeURIComponent).join('/');

        if (opts.account)
            key = key.replace(PATH_LOGIN_RE, '/' + opts.account.uuid + '/');

    } catch (e) {
        err = e;
    }

    cb(err, key);
}



///--- Exports

module.exports = {
    getLogLevel: getLogLevel,
    normalizeMantaPath: normalizeMantaPath,
    shuffle: shuffle
};