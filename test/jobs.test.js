// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var libmanta = require('../lib');


if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');


///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;



///--- Tests

test('transform job', function (t) {
        var input = {
                jobId: '9eff6942-8eea-4e87-b143-94f30c5a8f8e',
                jobName: 'metering-storage-hourly-2013-02-12-20',
                auth: {
                        login: 'poseidon',
                        uuid: '56f237ac-8cb0-4468-896f-5aa00ff8ffc9',
                        groups: [
                                'operators'
                        ],
                        token: 'void *'
                },
                authToken: 'void *',
                owner: '56f237ac-8cb0-4468-896f-5aa00ff8ffc9',
                phases: [ {
                        type: 'storage-map',
                        assets: [
                                '/poseidon/stor/usage/assets/bin/storage-map',
                                '/poseidon/stor/usage/assets/lib/carrier.js',
                                '/poseidon/stor/usage/assets/lib/storage-map.js'
                        ],
                        exec: 'some command'
                }, {
                        type: 'reduce',
                        assets: [
                                '/poseidon/stor/usage/assets/bin/storage-red1',
                                '/poseidon/stor/usage/assets/lib/carrier.js',
                                '/poseidon/stor/usage/assets/lib/stor-red1.js'
                        ],
                        exec: 'some reduce command',
                        count: 2
                }, {
                        type: 'reduce',
                        assets: [
                                '/poseidon/stor/usage/assets/bin/storage-red2',
                                '/poseidon/stor/usage/assets/lib/carrier.js',
                                '/poseidon/stor/usage/assets/lib/sum-columns.js'
                        ],
                        exec: 'yet another reduce command'
                } ],
                state: 'done',
                timeCreated: '2013-02-13T01:24:39.379Z',
                timeAssigned: '2013-02-13T01:24:39.412Z',
                stats: {
                        nAssigns: 1,
                        nErrors: 6,
                        nInputsRead: 1,
                        nJobOutputs: 0,
                        nTasksDispatched: 8,
                        nTasksCommittedOk: 2,
                        nTasksCommittedFail: 6
                },
                worker: 'ab111766-4646-435f-83e4-4c5158aac360',
                timeInputDone: '2013-02-13T01:24:39.616Z',
                timeInputDoneRead: '2013-02-13T01:24:39.703Z',
                timeDone: '2013-02-13T01:24:54.829Z'
        };

        var expect = {
                id: '9eff6942-8eea-4e87-b143-94f30c5a8f8e',
                name: 'metering-storage-hourly-2013-02-12-20',
                state: 'done',
                cancelled: false,
                inputDone: true,
                stats: {
                        errors: 6,
                        outputs: 0,
                        retries: 0,
                        tasks: 8,
                        tasksDone: 8
                },
                timeCreated: '2013-02-13T01:24:39.379Z',
                timeDone: '2013-02-13T01:24:54.829Z',
                phases:
                [ {
                        type: 'storage-map',
                        assets: [
                                '/poseidon/stor/usage/assets/bin/storage-map',
                                '/poseidon/stor/usage/assets/lib/carrier.js',
                                '/poseidon/stor/usage/assets/lib/storage-map.js'
                        ],
                        exec: 'some command'
                }, {
                        type: 'reduce',
                        assets: [
                                '/poseidon/stor/usage/assets/bin/storage-red1',
                                '/poseidon/stor/usage/assets/lib/carrier.js',
                                '/poseidon/stor/usage/assets/lib/stor-red1.js'
                        ],
                        exec: 'some reduce command',
                        count: 2
                }, {
                        type: 'reduce',
                        assets: [
                                '/poseidon/stor/usage/assets/bin/storage-red2',
                                '/poseidon/stor/usage/assets/lib/carrier.js',
                                '/poseidon/stor/usage/assets/lib/sum-columns.js'
                        ],
                        exec: 'yet another reduce command'
                } ]
        };

        t.deepEqual(libmanta.translateJob(input), expect);
        delete expect.phases;
        t.deepEqual(libmanta.translateJob(input, true), expect);

        t.end();
});
