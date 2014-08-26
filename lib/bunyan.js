/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

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
