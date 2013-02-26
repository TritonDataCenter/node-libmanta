// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');



///--- API

// Given a "raw" job record from marlin, generate a user-acceptable
// form of the job (this whitelists out internal details).  `summary`
// simply indicates whether or not to tack on all the phase details
// Returns an object
function translateJob(job, summary) {
        assert.object(job, 'job');
        assert.optionalBool(summary, 'summary');

        var obj = {
                id: job.jobId,
                name: job.jobName,
                state: job.state,
                cancelled: job.timeCancelled ? true : false,
                inputDone: job.timeInputDone ? true : false,
                stats: {},
                timeCreated: job.timeCreated,
                timeDone: job.timeDone
        };

        if (obj.cancelled) {
                obj.state = 'done';
                obj.inputDone = true;
        }

        if (job.stats) {
                var js = job.stats;
                obj.stats.errors = js.nErrors || 0;
                obj.stats.outputs = js.nOutputs || 0;
                obj.stats.retries = js.nRetries || 0;
                obj.stats.tasks = js.nTasksDispatched || 0;
                obj.stats.tasksDone =
                        (js.nTasksCommittedOk || 0) +
                        (js.nTasksCommittedFail || 0);
        }

        if (!summary)
                obj.phases = job.phases;

        return (obj);
}



///--- Exports

module.exports = {
        translateJob: translateJob
};