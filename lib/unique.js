'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');


// Declare internals

const internals = {};


exports.compile = function (table, options) {

    if (!options) {
        return false;
    }

    Hoek.assert(table._id, `Cannot enforce uniqueness without local id allocation: ${table.name}`);

    const settings = {
        verified: false,
        tables: [],
        rules: []
    };

    options.forEach((field) => {

        const name = field.table || `penseur_unique_${table.name}_${field.path.join('_')}`;
        const unique = {
            path: field.path,
            table: table._db._generateTable(name),
            key: field.key
        };

        settings.rules.push(unique);
        settings.tables.push(name);
    });

    return settings;
};


exports.reserve = async function (table, items, updateId, callback) {

    if (!table._unique) {
        return;
    }

    // Find unique changes

    const reserve = [];
    const release = [];

    items = [].concat(items);
    for (let i = 0; i < items.length; ++i) {
        const item = items[i];

        for (let j = 0; j < table._unique.rules.length; ++j) {
            const rule = table._unique.rules[j];

            const values = internals.reach(item, rule.path);
            if (values !== undefined) {
                if (values.length) {
                    reserve.push({ rule, values, id: typeof updateId !== 'boolean' ? updateId : item[table.primary] });
                }

                if (updateId !== false &&
                    !values._bypass) {

                    release.push(rule);
                }
            }
        }
    }

    // Prepare cleanup operation

    let cleanup = null;
    if (release.length) {
        cleanup = async (changes) => {

            const change = changes[0];                                  // Always includes one change
            for (let i = 0; i < release.length; ++i) {
                const rule = release[i];

                if (!change.old_val) {
                    continue;
                }

                let released = internals.reach(change.old_val, rule.path);
                if (!released) {
                    continue;
                }

                const taken = internals.reach(change.new_val, rule.path);
                if (taken) {
                    released = released.filter((value) => taken.indexOf(value) === -1);
                }

                if (!released.length) {
                    continue;
                }

                await rule.table.remove(released);
            }
        };
    }

    // Reserve new values

    if (!reserve.length) {
        return cleanup;
    }

    await exports.verify(table);

    for (let i = 0; i < reserve.length; ++i) {
        const field = reserve[i];

        // Try to get existing reservations

        let values = field.values;
        const existing = await field.rule.table.get(values);

        if (existing) {
            const existingIds = [];
            for (let j = 0; j < existing.length; ++j) {
                const item = existing[j];
                if (item[field.rule.key] !== field.id) {
                    throw Boom.internal(`Action will violate unique restriction on ${item[table.primary]} in table ${field.rule.table.name}`);
                }

                existingIds.push(item[table.primary]);
            }

            values = values.filter((value) => existingIds.indexOf(value) === -1);
        }

        const reservations = [];
        const now = Date.now();
        values.forEach((value) => {

            const rsv = { id: value, created: now };
            rsv[field.rule.key] = field.id;
            reservations.push(rsv);
        });

        await field.rule.table.insert(reservations);
    }

    return cleanup;
};


internals.reach = function (obj, path) {

    // Optimize common cases

    let ref = undefined;
    if (path.length === 1) {
        ref = obj[path[0]];
    }
    else if (path.length === 2) {
        ref = obj[path[0]] && obj[path[0]][path[1]];
    }
    else {

        // Any longer path

        ref = obj;
        for (let i = 0; i < path.length; ++i) {
            const key = path[i];

            if (ref[key] === undefined) {
                return undefined;
            }

            ref = ref[key];
        }
    }

    // Normalize value

    if (ref === undefined) {
        return undefined;
    }

    if (Array.isArray(ref)) {
        return ref;
    }

    if (typeof ref === 'object') {
        const keys = Object.keys(ref);
        let unset = false;
        const taken = keys.filter((key) => {

            if (typeof ref[key] !== 'function') {
                return true;
            }

            if (ref[key].type !== 'unset') {
                return true;
            }

            unset = true;
            return false;
        });

        return taken.length ? taken : (unset ? [] : undefined);
    }

    if (typeof ref === 'function') {
        if (ref.type === 'unset') {
            return [];
        }

        if (ref.type === 'append') {
            if (Array.isArray(ref.value) &&
                ref.flags.single) {

                throw Boom.internal('Cannot add an array as single value to unique index value');
            }

            const result = Array.isArray(ref.value) ? ref.value : [ref.value];
            result._bypass = true;
            return result;
        }

        throw Boom.internal('Cannot increment unique index value');    // type: increment
    }

    return [ref];
};


exports.verify = async function (table) {

    if (!table._unique ||
        table._unique.verified) {

        return;
    }

    for (let i = 0; i < table._unique.tables.length; ++i) {
        const name = table._unique.tables[i];
        const create = {};
        create[name] = { purge: false, secondary: false };
        try {
            await table._db._createTable(create);
        }
        catch (err) {
            err.message = `Failed creating unique table: ${name}`;
            throw err;
        }
    }

    table._unique.verified = true;
};
