// Copyright 2013 Joyent, Inc.  All rights reserved.

var fs = require('fs');
var util = require('util');

var assert = require('assert-plus');
var verror = require('verror');



///--- Globals

var WError = verror.WError;



///--- Errors

function ConnectTimeoutError(cause, service, timeout) {
        if (arguments.length === 2) {
                timeout = service;
                service = cause;
                cause = {};
        }

        WError.call(this, cause, '%s: connect timeout after %d ms',
                    service, timeout);

        this.name = this.constructor.name;
}
util.inherits(ConnectTimeoutError, WError);


function HealthCheckError(cause, service, reason) {
        if (arguments.length === 2) {
                reason = service;
                service = cause;
                cause = {};
        }

        WError.call(this, cause, '%s: health check failed %s', service, reason);

        this.name = this.constructor.name;
}
util.inherits(HealthCheckError, WError);


function InvalidDataError(cause, msg) {
        if (arguments.length === 1) {
                msg = cause;
                cause = {};
        }

        WError.call(this, cause, msg);

        this.name = this.constructor.name;
}
util.inherits(InvalidDataError, WError);


function NameResolutionError(cause, service) {
        if (arguments.length === 1) {
                service = cause;
                cause = {};
        }

        WError.call(this, cause, '%s: not found in DNS', service);

        this.name = this.constructor.name;
}
util.inherits(NameResolutionError, WError);


function NotConnectedError(cause, service, remote) {
        if (arguments.length === 2) {
                remote = service;
                service = cause;
                cause = {};
        }

        WError.call(this, cause, '%s: not connected to %s', service, remote);

        this.name = this.constructor.name;
}
util.inherits(NotConnectedError, WError);


function UserDoesNotExistError(cause, u) {
        if (arguments.length === 1) {
                u = cause;
                cause = {};
        }

        WError.call(this, cause, '%s is not a manta user', u);

        this.name = this.constructor.name;
}
util.inherits(UserDoesNotExistError, WError);



///--- Exports

// Auto export all Errors defined in this file
fs.readFileSync(__filename, 'utf8').split('\n').forEach(function (l) {
        /* JSSTYLED */
        var match = /^function\s+(\w+)\(.*/.exec(l);
        if (match !== null && Array.isArray(match) && match.length > 1) {
                if (/\w+Error$/.test(match[1])) {
                        module.exports[match[1]] = eval(match[1]);
                }
        }
});

Object.keys(module.exports).forEach(function (k) {
        global[k] = module.exports[k];
});
