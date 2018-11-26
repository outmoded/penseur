'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');


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

    if (typeof changes === 'function' &&
        changes.type) {

        if (changes.type === 'unset') {
            without.push(internals.select(path));
            return undefined;
        }

        if (changes.type === 'override') {
            without.push(internals.select(path));
            return internals.wrap(changes.value, without, item, path);
        }

        let row = item(path[0]);
        for (let i = 1; i < path.length; ++i) {
            row = row(path[i]);
        }

        if (changes.type === 'append') {
            if (changes.flags.single ||
                !Array.isArray(changes.value)) {

                row = row.append(changes.value);
            }
            else {
                for (let i = 0; i < changes.value.length; ++i) {
                    row = row.append(changes.value[i]);
                }
            }

            if (changes.flags.create) {
                let check = item;
                for (let i = 0; i < path.length - 1; ++i) {
                    check = check(path[i]);
                }

                return RethinkDB.branch(check.hasFields(path[path.length - 1]), row, [changes.value]);
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
        !Object.keys(ref).length &&
        !Array.isArray(ref)) {

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
