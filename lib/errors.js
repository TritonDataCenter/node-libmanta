/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

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


function EmptySetError(cause, set) {
    if (arguments.length === 1) {
        set = cause;
        cause = {};
    }

    WError.call(this, cause, '%s is an empty set', set);

    this.name = this.constructor.name;
}
util.inherits(EmptySetError, WError);


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
