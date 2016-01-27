'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


exports.type = function (type, value, options) {

    options = options || {};

    const modifier = function () { };                       // Return function because 1. typeof fastest 2. illegal database type
    modifier.type = type;
    modifier.flags = Hoek.clone(options);

    if (value !== undefined) {
        modifier.value = value;
    }

    return modifier;
};


exports.wrap = function (changes) {

    Hoek.assert(changes && typeof changes === 'object', 'Invalid changes object');

    const without = [];
    const wrapped = internals.wrap(changes, without, []);

    if (!without.length) {
        return wrapped;
    }

    return function (item) {

        const exclude = item.without(without);
        if (wrapped) {
            return exclude.merge(wrapped);
        }

        return exclude;
    };
};


internals.wrap = function (changes, without, path) {

    if (typeof changes === 'function' &&
        changes.type) {

        if (changes.type === 'unset') {
            without.push(internals.select(path));
            return undefined;
        }

        let row = RethinkDB.row(path[0]);
        for (let i = 1; i < path.length; ++i) {
            row = row(path[i]);
        }

        if (changes.type === 'append') {
            if (changes.flags.single || !Array.isArray(changes.value)) {
                return row.append(changes.value);
            }

            for (let i = 0; i < changes.value.length; ++i) {
                row = row.append(changes.value[i]);
            }

            return row;
        }

        // type: increment

        return row.add(changes.value);
    }

    if (typeof changes !== 'object' ||
        changes === null) {

        return changes;
    }

    let ref = changes;                                      // Try using the same object

    const keys = Object.keys(changes);
    for (let i = 0; i < keys.length; ++i) {
        const key = keys[i];
        const value = changes[key];
        const wrapped = internals.wrap(value, without, path.concat(key));
        if (wrapped !== value) {
            if (ref === changes) {
                ref = Hoek.shallow(changes);                // Shallow clone before making changes
            }

            if (wrapped === undefined) {
                delete ref[key];
            }
            else {
                ref[key] = wrapped;
            }
        }
    }

    if (!Object.keys(ref).length) {
        return undefined;
    }

    return ref;
};


internals.select = function (path) {

    const selection = {};
    let current = selection;
    for (let i = 0; i < path.length; ++i) {
        const key = path[i];
        current[key] = (i === path.length - 1 ? true : {});
        current = current[key];
    }

    return selection;
};
