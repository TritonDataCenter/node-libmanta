// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert');
var sprintf = require('util').format;



///--- Messages

var ARG_REQUIRED = '%s is required';
var ARRAY_TYPE_REQUIRED = '%s ([%s]) required';
var TYPE_REQUIRED = '%s is required';



///--- Internal Functions

function _() {
        return (sprintf.apply(null, arguments));
}


function assertArgument(name, type, arg) {
        if (arg === undefined)
                throw new TypeError(_(ARG_REQUIRED, name));


        if (typeof (arg) !== type)
                throw new TypeError(_(TYPE_REQUIRED, name, type));


        return (true);
}



///--- API

function array(name, type, arr) {
        var ok = true;

        if (!Array.isArray(arr))
                throw new TypeError(_(ARRAY_TYPE_REQUIRED, name, type));

        for (var i = 0; i < arr.length; i++) {
                if (typeof (arr[i]) !== type) {
                        ok = false;
                        break;
                }
        }

        if (!ok)
                throw new TypeError(_(ARRAY_TYPE_REQUIRED, name, type));

}


function optional(name, type, arg) {
        if (arg !== undefined)
                assertArgument(name, type, arg);
}


function bool(name, arg) {
        assertArgument(name, 'boolean', arg);
}


function func(name, arg) {
        assertArgument(name, 'function', arg);
}


function number(name, arg) {
        assertArgument(name, 'number', arg);
}


function object(name, arg) {
        assertArgument(name, 'object', arg);
}


function string(name, arg) {
        assertArgument(name, 'string', arg);
}



///--- Exports

module.exports = {

        array: array,
        bool: bool,
        func: func,
        number: number,
        object: object,
        optional: optional,
        string: string

};


Object.keys(assert).forEach(function (k) {
        module.exports[k] = assert[k];
});