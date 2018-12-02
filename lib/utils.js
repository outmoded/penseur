'use strict';

const Hoek = require('hoek');


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

        keys = [...new Set(keys.concat(Object.keys(base)))];                // Unique keys
    }

    const changes = {};
    for (const key of keys) {
        const value = compare[key];
        if (value === undefined) {                                          // Monitored key not present
            if (base[key] !== undefined &&
                options.deleted) {

                changes[key] = null;
            }

            continue;
        }

        if (value !== null &&
            base[key] !== null &&
            typeof value === 'object' &&
            typeof base[key] === 'object') {

            if (Array.isArray(value) ||
                Array.isArray(base[key])) {

                if (Hoek.deepEqual(base[key], value)) {
                    continue;
                }

                if (options.arrays === false ||                             // Defaults to true
                    !Array.isArray(value) ||
                    !Array.isArray(base[key])) {

                    changes[key] = value;
                    continue;
                }
            }

            const change = exports.diff(base[key], value, { arrays: options.arrays, deleted: options.deleted });
            if (change) {
                changes[key] = change;
            }

            continue;
        }

        if (value !== base[key]) {
            changes[key] = value;
        }
    }

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
