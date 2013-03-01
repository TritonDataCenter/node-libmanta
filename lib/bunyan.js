// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var bunyan = require('bunyan');
var restify = require('restify');



///--- API

function serializeMarlin(m) {
        var str = '';

        if (m && m.ma_client) {
                str += 'tcp://';
                str += m.ma_client.host;
                str += ':' + m.ma_client.port;
        }

        return (str);
}



///--- Exports

module.exports = {
        bunyan: {
                serializers: {
                        err: bunyan.stdSerializers.err,
                        marlin: serializeMarlin,
                        req: restify.bunyan.serializers.req,
                        res: restify.bunyan.serializers.res,
                        client_req: restify.bunyan.serializers.client_req,
                        client_res: restify.bunyan.serializers.client_res
                }
        }
};
