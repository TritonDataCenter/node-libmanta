// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');

var logger = require('./logger');
var Ring = require('./ring');



///--- Internal Functions

function assertArguments(opts) {
        assert.object(opts, 'options');
        assert.optionalString(opts.algorithm, 'options.algorithm');
        assert.optionalNumber(opts.conectTimeout, 'options.connectTimeout');
        assert.optionalObject(opts.log, 'options.log');
        assert.number(opts.replicas, 'options.replicas');
        assert.optionalObject(opts.schema, 'options.schema');
        assert.arrayOfString(opts.urls, 'options.urls');
}


function clone(obj) {
        if (!obj)
                return (obj);

        var copy = {};
        Object.keys(obj).forEach(function (k) {
                copy[k] = obj[k];
        });
        return (copy);
}



///--- API

module.exports = {

        /**
         * Creates a new consistent hash ring over a bunch of moray shards.
         *
         * See ./ring.js for more information.
         *
         * @param {object} options =>
         *                   - algorithm: hash algorithm ('sha1', 'sha256', ...)
         *                   - connectTimeout: for moray
         *                   - checkInterval: moray conn pool health freq
         *                   - maxClients: max sockets per moray
         *                   - maxIdleTime: per moray before reaping
         *                   - noInitialize: do not connect to moray
         *                   - replicas: number of c-hash replicas to make
         *                   - urls: array of moray URLs
         * @return {Ring}
         */
        createIndexRing: function createIndexRing(options) {
                assertArguments(options);

                var opts = clone(options);
                opts.log = options.log || logger();

                var ring = new Ring(opts);
                return (ring);
        }
};
