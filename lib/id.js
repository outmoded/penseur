'use strict';

// Load Modules

const Boom = require('boom');
const Crypto = require('crypto');
const Hoek = require('hoek');
const Items = require('items');
const Radix62 = require('radix62');


// Declare internals

const internals = {
    byteToHex: []
};


internals.buildCache = function () {

    for (let i = 0; i < 256; ++i) {
        const hex = (i < 16 ? '0' : '') + i.toString(16);
        internals.byteToHex[i] = hex;
    }
};

internals.buildCache();


exports.type = function (type) {

    Hoek.assert(['uuid', 'increment'].indexOf(type) !== -1, `Unknown id type: ${type}`);

    const identifier = function () { };                         // Return function because 1. typeof fastest 2. illegal database type
    identifier.type = type;

    return identifier;
};


exports.wrap = function (table, items, callback) {

    const result = [];
    const identifiers = [];
    [].concat(items).forEach((item) => {

        const keys = Object.keys(item);
        let identifier = null;
        for (let i = 0; i < keys.length; ++i) {
            const key = keys[i];
            const value = item[key];
            if (typeof value === 'function') {
                identifier = { item: Hoek.shallow(item), key, type: value.type, flags: value.flags };
                break;
            }
        }

        if (identifier) {
            result.push(identifier.item);
            identifiers.push(identifier);
        }
        else {
            result.push(item);
        }
    });

    if (!identifiers.length) {
        return Hoek.nextTick(callback)(null, items);
    }

    const each = (identifier, next) => {

        internals[identifier.type](table, (err, id) => {

            if (err) {
                return next(err);
            }

            identifier.item[identifier.key] = id;
            return next();
        });
    };

    Items.serial(identifiers, each, (err) => {

        if (err) {
            return callback(err);
        }

        return callback(null, Array.isArray(items) ? result : result[0]);
    });
};


internals.uuid = function (table, callback) {

    // Based on node-uuid - https://github.com/broofa/node-uuid - Copyright (c) 2010-2012 Robert Kieffer - MIT License

    const buf = Crypto.randomBytes(16);

    buf[6] = (buf[6] & 0x0f) | 0x40;            // Per RFC 4122 (4.4) - set bits for version and `clock_seq_hi_and_reserved`
    buf[8] = (buf[8] & 0x3f) | 0x80;

    const b = internals.byteToHex;

    let i = 0;
    const id = b[buf[i++]] + b[buf[i++]] + b[buf[i++]] + b[buf[i++]] + '-' +
                b[buf[i++]] + b[buf[i++]] + '-' +
                b[buf[i++]] + b[buf[i++]] + '-' +
                b[buf[i++]] + b[buf[i++]] + '-' +
                b[buf[i++]] + b[buf[i++]] + b[buf[i++]] + b[buf[i++]] + b[buf[i++]] + b[buf[i++]];

    return Hoek.nextTick(callback)(null, id);
};


internals.increment = function (table, callback) {

    exports.verify(table, (err, allocated) => {

        if (err) {
            return callback(err);
        }

        if (allocated) {
            return callback(null, internals.radix(allocated, table._id.radix));
        }

        table._id.table.next(table._id.record, table._id.key, 1, (err, value) => {

            if (err) {
                err.message = `Failed allocating increment id: ${table.name}`;
                return callback(err);
            }

            return callback(null, internals.radix(value, table._id.radix));
        });
    });
};


internals.radix = function (value, radix) {

    if (radix <= 36) {
        return value.toString(radix);
    }

    return Radix62.to(value);
};


exports.verify = function (table, callback) {

    if (!table._id) {
        return Hoek.nextTick(callback)(Boom.badImplementation(`Cannot allocated an incremented id on a table without id settings: ${table.name}`));
    }

    if (table._id.verified) {
        return Hoek.nextTick(callback)();
    }

    const create = {};
    create[table._id.table.name] = { purge: false, secondary: false };
    table._db._createTable(create, (err) => {

        if (err) {
            err.message = `Failed creating increment id table: ${table.name}`;
            return callback(err);
        }

        table._id.table.get(table._id.record, (err, record) => {

            if (err) {
                err.message = `Failed verifying increment id record: ${table.name}`;
                return callback(err);
            }

            // Record found

            if (record) {
                if (record[table._id.key] === undefined) {

                    // Set key

                    const changes = {};
                    changes[table._id.key] = table._id.initial;
                    table._id.table.update(table._id.record, changes, (err) => {

                        if (err) {
                            err.message = `Failed initializing key-value pair to increment id record: ${table.name}`;
                            return callback(err);
                        }

                        table._id.verified = true;
                        return callback(null, table._id.initial);
                    });

                    return;
                }
                else if (!Hoek.isInteger(record[table._id.key])) {
                    return callback(Boom.internal(`Increment id record contains non-integer value: ${table.name}`));
                }

                table._id.verified = true;
                return callback();
            }

            // Insert record

            const item = { id: table._id.record };
            item[table._id.key] = table._id.initial;
            table._id.table.insert(item, (err, key) => {

                if (err) {
                    err.message = `Failed inserting increment id record: ${table.name}`;
                    return callback(err);
                }

                table._id.verified = true;
                return callback(null, table._id.initial);
            });
        });
    });
};
