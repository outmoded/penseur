'use strict';

// Load Modules

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

    const fields = [];
    const byHolder = (updateId ? [] : null);
    [].concat(items).forEach((item) => {

        table._unique.rules.forEach((rule) => {

            const value = internals.reach(item, rule.path);
            if (value !== undefined) {
                fields.push({ rule, value, id: updateId !== null ? updateId : item.id });

                if (updateId) {
                    byHolder.push(rule);
                }
            }
        });
    });

    if (!fields.length) {
        return Hoek.nextTick(callback)();
    }

    exports.verify(table, (err) => {

        if (err) {
            return callback(err);
        }

        const each = (field, next) => {

            const reservation = { id: field.value, created: Date.now() };
            reservation[field.rule.key] = field.id;
            field.rule.table.insert(reservation, next);
        };

        return Items.serial(fields, each, (err) => {

            if (err) {
                return callback(err);
            }

            return callback(null, (changes, finalize) => {

                const change = changes[0];                                  // Always includes one change
                const item = change.old_val;
                const eachRule = (rule, next) => {

                    const oldValue = internals.reach(item, rule.path);
                    return rule.table.remove(oldValue, next);
                };

                return Items.serial(byHolder, eachRule, finalize);
            });
        });
    });
};


internals.reach = function (obj, path) {

    // Optimize common cases

    let ref = undefined;
    if (path.length === 1) {
        ref = obj[path[0]];
    }
    else if (path.length === 2) {
        ref = obj[path[0]][path[1]];
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

    if (typeof ref === 'object') {
        return undefined;
    }

    return ref;
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
