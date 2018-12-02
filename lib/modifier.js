'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');

const Special = require('./special');


// Declare internals

const internals = {};


exports.wrap = function (changes, table) {

    const pairs = {};

    if (Array.isArray(changes)) {
        for (const change of changes) {
            const key = change[table.primary];
            const id = (typeof key === 'string' ? key : JSON.stringify(key));
            const existing = pairs[id];
            if (!existing) {
                pairs[id] = change;
            }
            else {
                const base = Hoek.clone(existing);
                pairs[id] = Hoek.merge(base, change, true, false);
            }
        }
    }

    return function (item) {

        const each = (change) => {

            const without = [];
            const wrapped = internals.wrap(change, without, item, []);

            let update = item;
            if (without.length) {
                update = update.without(without);
            }

            if (wrapped) {
                update = update.merge(wrapped);
            }

            return RethinkDB.branch(RethinkDB.eq(item, null), RethinkDB.error('No document found'), update);
        };

        if (!Array.isArray(changes)) {
            return each(changes);
        }

        for (const id in pairs) {
            pairs[id] = each(pairs[id]);
        }

        return RethinkDB.expr(pairs)(item(table.primary).coerceTo('string'));
    };
};


internals.wrap = function (changes, without, item, path) {

    if (Special.isSpecial(changes)) {
        const { type, value, flags } = changes;

        // Unset

        if (type === 'unset') {
            without.push(internals.select(path));
            return undefined;
        }

        // Override

        if (type === 'override') {
            without.push(internals.select(path));
            return internals.wrap(value, without, item, path);
        }

        // Append

        const current = internals.path(item, path);
        let changed = current;

        if (type === 'append') {
            if (flags.single ||
                !Array.isArray(value)) {

                changed = changed.append(value);
            }
            else {
                for (const v of value) {
                    changed = changed.append(v);
                }
            }

            if (flags.unique) {
                let match;

                const upath = flags.unique.path;
                if (upath) {
                    const innerValue = Hoek.reach(value, upath);
                    if (flags.unique.match === 'any') {
                        match = current.offsetsOf((v) => internals.path(v, upath).eq(innerValue)).count().eq(0);
                    }
                    else {
                        match = internals.path(current, upath).nth(-1).ne(innerValue);
                    }
                }
                else {
                    if (flags.unique.match === 'any') {
                        match = current.offsetsOf(value).count().eq(0);
                    }
                    else {
                        match = current.nth(-1).ne(value);
                    }
                }

                changed = RethinkDB.branch(current.count().eq(0), changed, RethinkDB.branch(match, changed, current));
            }

            if (flags.create) {
                const check = internals.path(item, path, 1);
                changed = RethinkDB.branch(check.hasFields(path[path.length - 1]), changed, [value]);
            }

            return changed;
        }

        // Increment

        return changed.add(value);
    }

    if (typeof changes !== 'object' ||
        changes === null) {

        return changes;
    }

    let ref = changes;                                      // Try using the same object

    for (const key in changes) {
        const value = changes[key];
        const wrapped = internals.wrap(value, without, item, path.concat(key));
        if (wrapped === value) {
            continue;
        }

        if (ref === changes) {
            ref = Object.assign({}, changes);               // Shallow clone before making changes
        }

        if (wrapped === undefined) {
            delete ref[key];
        }
        else {
            ref[key] = wrapped;
        }
    }

    if (Object.keys(changes).length &&
        !Array.isArray(ref) &&
        !Object.keys(ref).length) {

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


internals.path = function (ref, path, offset = 0) {

    for (let i = 0; i < path.length - offset; ++i) {
        ref = ref(path[i]);
    }

    return ref;
};
