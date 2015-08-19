/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * Common definitions for the trigger stress test.
 */

var mod_assertplus = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_moray = require('moray');
var mod_path = require('path');
var mod_url = require('url');
var mod_vasync = require('vasync');

var tablePrefix = 'trigger_stress';
var funcPrefix = 'test_count';

exports.tablePrefix = tablePrefix;
exports.tableData = tablePrefix + '_data';
exports.tableCounts = tablePrefix + '_counts';
exports.funcPrefix = funcPrefix;
exports.maxVersions = 20;

exports.setup = setup;
exports.funcName = funcName;
exports.resetBucketState = resetBucketState;
exports.fetchState = fetchState;
exports.installTrigger = installTrigger;
exports.TriggerStressTester = TriggerStressTester;

/*
 * Instantiate a logger and Moray client based on parameters from the
 * environment.
 */
function setup()
{
    var url, host, port;
    var log, client;

    log = new mod_bunyan({
        'name': mod_path.basename(process.argv[1]),
        'level': process.env['LOG_LEVEL'] || 'info'
    });

    if (!process.env['MORAY_URL']) {
        throw (new Error(
            'MORAY_URL must be specified in the environment.'));
    }

    url = mod_url.parse(process.env['MORAY_URL']);
    host = url['hostname'];
    port = parseInt(url['port'], 10);
    if (isNaN(port)) {
        throw (new Error('MORAY_URL port is not a number'));
    }

    client = mod_moray.createClient({
        'log': log,
        'host': host,
        'port': port,
        'connectTimeout': 3000,
        'retry': null,
        'reconnect': false
    });

    return ({
        'log': log,
        'client': client
    });
}

/*
 * Return the name for our user-defined trigger procedure at version "version".
 */
function funcName(version)
{
    return (funcPrefix + '_v' + version);
}

/*
 * Resets testing state:
 *    - Drops both Moray buckets (which drops any PostgreSQL triggers)
 *    - Drops any user-defined functions that are created as part of the test
 *    - Creates both Moray buckets
 */
function resetBucketState(client, usercallback)
{
    mod_vasync.waterfall([
        function deleteDataBucket(callback) {
            client.delBucket(exports.tableData, function (err) {
                if (err && err.name == 'BucketNotFoundError')
                    err = null;
                callback(err);
            });
        },

        function deleteCountBucket(callback) {
            client.delBucket(exports.tableCounts, function (err) {
                if (err && err.name == 'BucketNotFoundError')
                    err = null;
                callback(err);
            });
        },

        function delFunctions(callback) {
            var stmts, sqlstr, req, i;

            stmts = [];
            for (i = 0; i < exports.maxVersions; i++) {
                stmts.push('DROP FUNCTION IF EXISTS ' +
                    funcName(i + 1) + '();');
            }

            sqlstr = stmts.join('\n');
            req = client.sql(sqlstr);
            req.on('end', function () { callback(); });
        },

        function createDataBucket(callback) {
            client.putBucket(exports.tableData, {
                'index': {
                'aNumber': {
                    'type': 'number'
                }
                }
            }, function (err) { callback(err); });
        },

        function createCountBucket(callback) {
            client.putBucket(exports.tableCounts, {
                'index': {
                    'version': {
                        'type': 'number'
                    },
                    'ninserts': {
                        'type': 'number'
                    },
                    'ndeletes': {
                        'type': 'number'
                    }
                }
            }, function (err) { callback(err); });
        }

    ], usercallback);
}

/*
 * Fetch information about the current testing state.  This is not necessarily a
 * consistent snapshot if operations are ongoing.
 *
 * Result object:
 *
 *    tg_buckets    list of found bucket names
 *
 *    tg_funcs      list of found function names
 *
 *    tg_triggers   list of found triggers, each having properties:
 *
 *        tgr_table, tgr_name, tgr_event, tgr_action
 *
 *    tg_nobjects   count of objects in the "data" table
 *
 *    tg_counts     list of per-version count objects, each having properties:
 *
 *        tgc_version, tgc_ninserted, tgc_ndeleted, tgc_net
 *
 *                  This list includes version "-1", which is the "global"
 *                  count.
 *
 *    tg_vcount     net sum of per-version counts
 *
 *    tg_gcount     net sum of global count
 *
 *    tg_consistent boolean indicating whether the state appears to be
 *                  consistent
 */
function fetchState(client, usercallback)
{
    var rv;

    rv = {
        'tg_buckets': [],
        'tg_funcs': [],
        'tg_triggers': [],
        'tg_nobjects': null,
        'tg_counts': [],
        'tg_vcount': 0,
        'tg_gcount': 0,
        'tg_consistent': null
    };

    mod_vasync.waterfall([
        function listBuckets(callback) {
            client.listBuckets(function (err, buckets) {
                if (err) {
                    callback(err);
                    return;
                }

                buckets.forEach(function (b) {
                    if (b.name == exports.tableData ||
                        b.name == exports.tableCounts) {
                            rv.tg_buckets.push(b.name);
                    }
                });

                callback();
            });
        },

        function listProcedures(callback) {
            var sqlstr, req;

            sqlstr = [
                'SELECT proname ',
                '       FROM pg_proc ',
                '       WHERE proname like \'' +
                exports.funcPrefix + '%\''
            ].join('\n');
            req = client.sql(sqlstr);

            req.on('record', function (row) {
                rv.tg_funcs.push(row.proname + '()');
            });
            req.on('end', function () {
                callback();
            });
        },

        function listTriggers(callback) {
            var sqlstr, req;

            sqlstr = [
                'SELECT event_manipulation, ',
                '       trigger_name, action_statement ',
                '       FROM information_schema.triggers ',
                '       WHERE event_object_table = \'' +
                exports.tableData + '\''
            ].join('\n');
            req = client.sql(sqlstr);

            req.on('record', function (row) {
                rv.tg_triggers.push({
                    'tgr_table': exports.tableData,
                    'tgr_name': row.trigger_name,
                    'tgr_event': row.event_manipulation,
                    'tgr_action': row.action_statement
                });
            });

            req.on('end', function () {
                callback();
            });
        },

        function countDataRows(callback) {
            var req;

            req = client.findObjects(exports.tableData,
                '(aNumber=*)', { 'limit': 1 });

            req.on('record', function (record) {
                rv.tg_nobjects = record._count;
            });

            req.on('end', function () {
                if (rv.tg_nobjects === null)
                        rv.tg_nobjects = 0;
                callback();
            });
        },

        function summarizeCountRows(callback) {
            var req;

            req = client.findObjects(exports.tableCounts, '(version=*)', {
                'sort': {
                    'attribute': 'version',
                    'order': 'ASC'
                }
            });

            req.on('record', function (record) {
                var value = record.value;
                var net = value.ninserts - value.ndeletes;
                if (value.version != -1)
                    rv.tg_vcount += net;
                else
                    rv.tg_gcount += net;
                rv.tg_counts.push({
                    'tgc_version': value.version,
                    'tgc_ninserted': value.ninserts,
                    'tgc_ndeleted': value.ndeletes,
                    'tgc_net': net
                });
            });

            req.on('end', function () {
                callback();
            });
        }
    ], function (err) {
        if (err) {
            usercallback(err);
            return;
        }

        rv.tg_consistent = (rv.tg_vcount === rv.tg_gcount &&
            rv.tg_vcount === rv.tg_nobjects);
        usercallback(null, rv);
    });
}

/*
 * Install the given version of our test trigger.
 *
 *     moray        Moray client
 *
 *     version      Version of the trigger to use
 *
 *     concurrency  Number of concurrent operations to run (used for testing).
 *                  If this number is more than one, additional concurrent
 *                  operations are executed with versions randomly selected,
 *                  but possibly including "version" or valid versions less
 *                  than "version".
 */
function installTrigger(args, callback)
{
    var versions, v, i;

    mod_assertplus.object(args, 'args');
    mod_assertplus.number(args.version, 'args.version');
    mod_assertplus.number(args.concurrency, 'args.concurrency');
    mod_assertplus.object(args.moray, 'args.moray');
    mod_assertplus.func(callback, 'callback');

    versions = [ args.version ];
    for (i = 1; i < args.concurrency; i++) {
        if (Math.floor(2 * Math.random()) === 0) {
            v = args.version;
        } else {
            v = Math.max(1, args.version - 1);
        }
        versions.push(v);
    }

    return (mod_vasync.forEachParallel({
        'inputs': versions,
        'func': function doInstallOne(vi, subcb) {
            installTriggerOne({
                'moray': args.moray,
                'version': vi
            }, subcb);
        }
    }, callback));
}

function installTriggerOne(args, callback)
{
    var version, client;

    mod_assertplus.object(args, 'args');
    mod_assertplus.number(args.version, 'args.version');
    mod_assertplus.object(args.moray, 'args.moray');
    mod_assertplus.func(callback, 'callback');

    version = args.version.toString();
    client = args.moray;

    mod_vasync.forEachParallel({
        'inputs': [
            mod_path.join(
                '..', '..', '..', 'lib', 'trigger_update.plpgsql'),
            'function-version-N.plpgsql'
        ],
        'func': function readFileContents(relpath, subcb) {
            var path, fileoptions;
            path = mod_path.join(__dirname, relpath);
            fileoptions = { 'encoding': 'utf8' };
            mod_fs.readFile(path, fileoptions, subcb);
        }
    }, function (err, results) {
        var sql, req, rows;

        if (err) {
            callback(err);
            return;
        }

        sql = [
            results.operations[0].result,
            results.operations[1].result.replace(/NNNN/g, version)
        ].join('\n');

        req = client.sql(sql);
        rows = [];
        req.on('error', callback);
        req.on('record', function (row) {
            rows.push(row);
        });
        req.on('end', function () { callback(null, rows); });
    });
}

/*
 * Runs a stress-test that PUTs and DELETEs Moray objects.
 *
 *     log              bunyan-style logger
 *
 *     client           Moray client
 *
 *     concurrency      maximum concurrent requests
 *
 *     bucket           Moray bucket name
 *
 *     objPrefix        Object name prefix
 *
 *     nobjects         Maximum number of distinct objects to use
 *
 *     allowConflicts   If true, when selecting an operation to do next, either
 *                      "put" or "delete" is selected randomly, and an object
 *                      name is selected by combining "objPrefix" with a random
 *                      number between 1 and "nobjects - 1".  In this mode,
 *                      the final count of distinct objects is not knowable from
 *                      the results of the requests (i.e., this function cannot
 *                      tell you how many objects should be present at the end),
 *                      but the trigger-based count should match the actual
 *                      count of objects.
 *
 *                      If false, when selecting an operation to do next, the
 *                      object name is selected in the same way, but only one
 *                      operation will be allowed on a given object, and the
 *                      operation selected is whichever one we did not do last
 *                      time.  The first operation is a "delete".  In this mode,
 *                      if the bucket was empty to begin with, the expected
 *                      count of objects is exactly the delta between
 *                      successfully completed insert and delete operations.
 */
function TriggerStressTester(args)
{
    mod_assertplus.object(args, 'args');
    mod_assertplus.object(args.log, 'args.log');
    mod_assertplus.object(args.client, 'args.client');
    mod_assertplus.number(args.concurrency, 'args.concurrency');
    mod_assertplus.string(args.bucket, 'args.bucket');
    mod_assertplus.string(args.objPrefix, 'args.objPrefix');
    mod_assertplus.number(args.nobjects, 'args.nobjects');
    mod_assertplus.bool(args.allowConflicts, 'args.allowConflicts');

    this.tst_log = args.log;
    this.tst_client = args.client;
    this.tst_concurrency = args.concurrency;
    this.tst_bucket = args.bucket;
    this.tst_objprefix = args.objPrefix;
    this.tst_nobjects = args.nobjects;
    this.tst_allowconflicts = args.allowConflicts;

    this.tst_stopped = false;
    this.tst_pending = {};
    this.tst_last = {};
    this.tst_barrier = null;
    this.tst_ndeleted = 0;
    this.tst_ninserted = 0;
    this.tst_nop = 0;
}

TriggerStressTester.prototype.start = function ()
{
    var i;

    this.tst_barrier = mod_vasync.barrier();

    for (i = 0; i < this.tst_concurrency; i++) {
        this.doWork();
    }
};

TriggerStressTester.prototype.doWork = function ()
{
    var barrier, client, log, which, opname, objname, action;
    var self = this;

    if (this.tst_stopped)
        return;

    do {
        which = Math.floor(Math.random() * this.tst_nobjects);
        objname = this.tst_objprefix + which;
    } while (!this.tst_allowconflicts &&
        this.tst_pending.hasOwnProperty(objname));

    if (this.tst_allowconflicts) {
        action = Math.floor(Math.random() * 2) === 0 ?
            'putobject' : 'delobject';
    } else {
        action = this.tst_last.hasOwnProperty(objname) &&
            this.tst_last[objname] == 'delobject' ?
            'putobject' : 'delobject';
    }

    barrier = this.tst_barrier;
    client = this.tst_client;
    log = this.tst_log;
    opname = 'operation ' + (++this.tst_nop);
    this.tst_pending[objname] = opname;
    barrier.start(opname);

    if (action == 'delobject') {
        client.delObject(this.tst_bucket, objname, function (err) {
            delete (self.tst_pending[objname]);

            if (err && err.name != 'ObjectNotFoundError') {
                log.warn(err);
            } else if (!err) {
                self.tst_ndeleted++;
                self.tst_last[objname] = 'delobject';
            } else {
                self.tst_last[objname] = 'delobject';
            }

            self.doWork();
            barrier.done(opname);
        });
    } else {
        mod_assertplus.equal(action, 'putobject');
        client.putObject(this.tst_bucket, objname, {
            'aNumber': which
        }, function (err) {
            delete (self.tst_pending[objname]);

            /* Work around MORAY-320 */
            if (err && err.name != 'UniqueAttributeError') {
                log.warn(err);
            } else if (!err) {
                self.tst_ninserted++;
                self.tst_last[objname] = 'putobject';
            }

            self.doWork();
            barrier.done(opname);
        });
    }
};

TriggerStressTester.prototype.stop = function (callback)
{
    this.tst_stopped = true;
    this.tst_barrier.start('stop');
    this.tst_barrier.once('drain', callback);
    this.tst_barrier.done('stop');
};

TriggerStressTester.prototype.stats = function ()
{
    return ({
        'ninserted': this.tst_ninserted,
        'ndeleted': this.tst_ndeleted
    });
};
