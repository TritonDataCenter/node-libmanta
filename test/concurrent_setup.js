/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * concurrent_setup.js: test program to exercise libmanta Moray client setup
 * concurrently.
 */

var assert = require('assert');
var bunyan = require('bunyan');
var cmdutil = require('cmdutil');
var getopt = require('posix-getopt');
var moray = require('../lib/moray');
var vasync = require('vasync');

var nconcurrent = 16;
var clientOptionTemplate = {
    'host': undefined,
    'port': 2020,
    'connectTimeout': 1000,
    'retry': undefined
};

function main()
{
    var log, clients, i;
    var parser, option, p, args;

    log = new bunyan({
        'name': 'concurrent_setup',
        'level': process.env['LOG_LEVEL'] || 'debug'
    });
    clientOptionTemplate.log = log;

    cmdutil.configure({
        'synopses': [ '[-c | --concurrency CONCURRENCY] HOSTNAME' ],
        'usageMessage': 'Exercise libmanta Moray client setup'
    });

    parser = new getopt.BasicParser('c:(concurrency)', process.argv);
    while ((option = parser.getopt()) !== undefined) {
        switch (option.option) {
        case 'c':
            p = parseInt(option.optarg, 10);
            if (isNaN(p) || p <= 0) {
                cmdutil.usage('bad value for -c/--concurrency: %s',
                    option.optarg);
            }
            nconcurrent = p;
            break;

        default:
            cmdutil.usage();
            break;
        }
    }

    args = process.argv.slice(parser.optind());
    if (args.length != 1) {
        cmdutil.usage('expected exactly one hostname');
    }

    clientOptionTemplate.host = args[0];
    clients = [];

    assert.ok(nconcurrent > 0);
    for (i = 0; i < nconcurrent; i++) {
        clients.push(i);
    }

    log.info('setting up');
    vasync.forEachParallel({
        'inputs': clients,
        'func': makeClient
    }, function (err, results) {
        if (err) {
            throw (err);
        }

        log.info('tearing down');
        results.successes.forEach(function (client) {
            client.close();
        });
    });
}

function makeClient(which, callback)
{
    var log, client, options, k;

    options = {};
    for (k in clientOptionTemplate) {
        options[k] = clientOptionTemplate[k];
    }

    log = clientOptionTemplate.log.child({ 'whichClient': which });
    options.log = log;

    log.info('creating client');
    client = moray.createMorayClient(options);
    client.on('connect', function () {
        log.info('client connected');
        callback(null, client);
    });
}

main();
