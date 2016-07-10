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
        if (value === undefined) {                          // Monitored key not present
            if (base[key] !== undefined &&
                options.deleted) {

                changes[key] = null;
            }
            return;
        }

        if (typeof value === 'object') {
            const change = exports.diff(base[key], value, { deleted: options.deleted });
            if (change) {
                changes[key] = change;
            }
        }
        else if (value !== base[key]) {
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
