// Copyright 2012 Joyent, Inc.  All rights reserved.
//
// This file exists purely as a place to create a common default bunyan
// Logger instance
//

var bunyan = require('bunyan');



///--- Globals

var LOG;



///--- API

module.exports = function logger() {
        if (!LOG) {
                LOG = bunyan.createLogger({
                        name: 'libindex',
                        stream: process.stderr,
                        level: (process.env.LOG_LEVEL || 'warn'),
                        serializers: {
                                err: bunyan.stdSerializers.err
                        }
                });
        }

        return (LOG);
};
