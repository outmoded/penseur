'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


exports.wrap = function (changes, table) {

    return function (item) {

        const each = (change) => {

            const without = [];
            const wrapped = internals.wrap(change, without, item, []);

            let update = null;
            if (wrapped) {
                if (without.length) {
                    update = item.without(without).merge(wrapped);
                }
                else {
                    update = item.merge(wrapped);
                }
            }
            else {
                update = item.without(without);
            }

            return RethinkDB.branch(RethinkDB.eq(item, null), RethinkDB.error('No document found'), update);
        };

        if (!Array.isArray(changes)) {
            return each(changes);
        }

        const pairs = {};
        changes.forEach((change) => {

            pairs[change[table.primary]] = each(change);
        });

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
        const wrapped = internals.wrap(value, without, item, path.concat(key));
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

    if (keys.length &&
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
