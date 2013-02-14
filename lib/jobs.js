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
                if (job.stats.nErrors)
                        obj.stats.errors = job.stats.nErrors;
                if (job.stats.nOutputs)
                        obj.stats.outputs = job.stats.nOutputs;
                if (job.stats.nRetries)
                        obj.stats.retries = job.stats.nRetries || 0;
                if (job.stats.nTasksDispatched)
                        obj.stats.tasks = job.stats.nTasksDispatched;
                obj.stats.tasksDone = (job.stats.nTasksCommittedOk || 0) +
                        (job.stats.nTasksCommittedFail || 0);
        }

        if (!summary)
                obj.phases = job.phases;

        return (obj);
}



///--- Exports

module.exports = {
        translateJob: translateJob
};