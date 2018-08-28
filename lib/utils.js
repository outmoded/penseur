'use strict';

// Load modules

const Hoek = require('hoek');


// Declare internals

const internals = {};


exports.diff = function (base, compare, options) {

    options = options || {};

    if (!base) {
        if (!options.whitelist) {
            return compare;
        }

        const picked = internals.pick(options.whitelist, compare);
        return (Object.keys(picked).length ? picked : null);
    }

    let keys = options.whitelist || Object.keys(compare);
    if (options.deleted &&
        !options.whitelist) {

        keys = Hoek.unique(keys.concat(Object.keys(base)));
    }

    const changes = {};
    keys.forEach((key) => {

        const value = compare[key];
        if (value === undefined) {                                          // Monitored key not present
            if (base[key] !== undefined &&
                options.deleted) {

                changes[key] = null;
            }
            return;
        }

        if (value !== null &&
            base[key] !== null &&
            typeof value === 'object' &&
            typeof base[key] === 'object') {

            if (Array.isArray(value) ||
                Array.isArray(base[key])) {

                if (Hoek.deepEqual(base[key], value)) {
                    return;
                }

                if (options.arrays === false ||                             // Defaults to true
                    !Array.isArray(value) ||
                    !Array.isArray(base[key])) {

                    changes[key] = value;
                    return;
                }
            }

            const change = exports.diff(base[key], value, { arrays: options.arrays, deleted: options.deleted });
            if (change) {
                changes[key] = change;
            }

            return;
        }

        if (value !== base[key]) {
            changes[key] = value;
        }
    });

    if (!Object.keys(changes).length) {
        return null;
    }

    return changes;
};


internals.pick = function (keys, source) {

    const result = {};
    keys.forEach((key) => {

        if (source[key] !== undefined) {
            result[key] = source[key];
        }
    });

    return result;
};
