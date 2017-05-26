'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');
const Items = require('items');


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


exports.reserve = function (table, items, updateId, callback) {

    if (!table._unique) {
        return Hoek.nextTick(callback)();
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
                if (values.isBoom) {
                    return callback(values);
                }

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

    const cleanup = (!release.length ? null : (changes, finalize) => {

        const change = changes[0];                                  // Always includes one change
        const eachRule = (rule, next) => {

            if (!change.old_val) {
                return next();
            }

            let released = internals.reach(change.old_val, rule.path);
            if (!released) {
                return next();
            }

            const taken = internals.reach(change.new_val, rule.path);
            if (taken) {
                released = released.filter((value) => taken.indexOf(value) === -1);
            }

            if (!released.length) {
                return next();
            }

            return rule.table.remove(released, next);
        };

        return Items.serial(release, eachRule, finalize);
    });

    // Reserve new values

    if (!reserve.length) {
        return Hoek.nextTick(callback)(null, cleanup);
    }

    exports.verify(table, (err) => {

        if (err) {
            return callback(err);
        }

        const each = (field, next) => {

            // Try to get existing reservations

            let values = field.values;
            field.rule.table.get(values, (err, existing) => {

                if (err) {
                    return next(err);
                }

                if (existing) {
                    const existingIds = [];
                    for (let i = 0; i < existing.length; ++i) {
                        const item = existing[i];
                        if (item[field.rule.key] !== field.id) {
                            return next(Boom.internal(`Action will violate unique restriction on ${item.id} in table ${field.rule.table.name}`));
                        }

                        existingIds.push(item.id);
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

                field.rule.table.insert(reservations, next);
            });
        };

        Items.serial(reserve, each, (err) => callback(err, cleanup));
    });
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

                return Boom.internal('Cannot add an array as single value to unique index value');
            }

            const result = Array.isArray(ref.value) ? ref.value : [ref.value];
            result._bypass = true;
            return result;
        }

        return Boom.internal('Cannot increment unique index value');    // type: increment
    }

    return [ref];
};


exports.verify = function (table, callback) {

    if (!table._unique ||
        table._unique.verified) {

        return Hoek.nextTick(callback)();
    }

    const each = (name, next) => {

        const create = {};
        create[name] = { purge: false, secondary: false };
        table._db._createTable(create, (err) => {

            if (err) {
                err.message = `Failed creating unique table: ${name}`;
                return next(err);
            }

            return next();
        });
    };

    Items.serial(table._unique.tables, each, (err) => {

        if (err) {
            return callback(err);
        }

        table._unique.verified = true;
        return callback();
    });
};
