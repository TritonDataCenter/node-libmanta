// Copyright (c) 2013 Joyent, Inc.  All rights reserved.



///--- APIs

/**
 * Useful for backoff logging - pass in an attempt, and it returns the
 * appropriate bunyan log level as a key. i.e.,:
 *
 * log[getLogLevel(attempt)]('foo...');
 */
function getLogLevel(attempt) {
        var l;

        if (attempt === 0) {
                l = 'info';
        } else if (attempt < 5) {
                l = 'warn';
        } else {
                l = 'error';
        }

        return (l);
}

// Just shuffles an array in place, and returns what you passed in
function shuffle(array) {
        var current;
        var tmp;
        var top = array.length;

        if (top) {
                while (--top) {
                        current = Math.floor(Math.random() * (top + 1));
                        tmp = array[current];
                        array[current] = array[top];
                        array[top] = tmp;
                }
        }

        return (array);
}



///--- Exports

module.exports = {
        getLogLevel: getLogLevel,
        shuffle: shuffle
};