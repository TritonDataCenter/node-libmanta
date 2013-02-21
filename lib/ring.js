// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var sprintf = require('util').format;

var assert = require('assert-plus');



///--- Internal Helpers

function increment(start, size, orig) {
        var next = (start + 1) % size;

        if (next === orig)
                return (false);

        return (next);
}



///--API

function Ring(opts) {
        if (typeof (opts) === 'object') {
                assert.number(opts.size, 'options.size');
        } else if (typeof (opts) === 'number') {
                opts = { size: opts };
        } else {
                throw new TypeError('size (number) required');
        }

        this.count = 0;
        this.start = 0;
        this.size = opts.size;
        this.ring = new Array(opts.size);
}


Ring.prototype.empty = function empty() {
        return (this.count === 0);
};


Ring.prototype.filter = function filter(f, thisp) {
        assert.func(f, 'callback');
        assert.optionalObject(thisp, 'this');

        var i = this.start;
        var res = [];

        do {
                var val = this.ring[i];
                if (f.call(thisp, val, i, this))
                        res.push(val);

                if (++i === this.ring.length)
                        i = 0;
        } while (i !== this.start);

        return (res);
};


Ring.prototype.full = function full() {
        return (this.count === this.size);
};


Ring.prototype.peek = function peek() {
        return (this.ring[this.start]);
};


Ring.prototype.pop = function pop() {
        var obj = this.ring[this.start];
        this.start = (this.start + 1) % this.size;
        this.count--;

        return (obj);
};


Ring.prototype.push = function push(obj) {
        if (obj === undefined)
                return (false);

        var end = (this.start + this.count) % this.size;
        this.ring[end] = obj;

        if (this.count === this.size) {
                this.start = (this.start + 1) % this.size;
        } else {
                this.count++;
        }

        return (this.count);
};


Ring.prototype.toString = function toString() {
        var s = sprintf('[object Ring <size=%d, count=%d>]',
                        this.size,
                        this.count);

        return (s);
};



///--- Exports

module.exports = {
        Ring: Ring,
        createRing: function createRing(opts) {
                return (new Ring(opts));
        }
};



///-- Tests

// var r = new Ring(100000);
// assert.ok(r.empty());
// assert.ok(!r.full());

// for (var i = 0; i < 100000; i++)
//         assert.ok(r.push(i + 1) !== false);

// assert.ok(!r.empty());
// assert.ok(r.full());

// var foo = r.filter(function (i) {
//         return (i % 2 === 0);
// });

// assert.ok(foo);
// foo.forEach(function (i) {
//         assert.equal(i % 2, 0);
// });
