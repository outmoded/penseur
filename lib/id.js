'use strict';

// Load Modules

const Crypto = require('crypto');
const Boom = require('boom');
const Hoek = require('hoek');
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


exports.normalize = function (ids, allowArray) {

    if (!Array.isArray(ids)) {
        return internals.validate(ids);
    }

    if (!allowArray) {
        return new Error('Array of ids not supported');
    }

    if (!ids.length) {
        return new Error('Empty array of ids not supported');
    }

    const normalized = [];
    for (let i = 0; i < ids.length; ++i) {
        const id = internals.validate(ids[i]);
        normalized.push(id);
    }

    return normalized;
};


internals.validate = function (id) {

    if (id &&
        typeof id === 'object') {

        if (id.id === undefined) {
            throw new Error('Invalid object id');
        }

        id = id.id;
    }

    if (id === null ||
        id === undefined) {

        throw new Error('Invalid null or undefined id');
    }

    if (typeof id === 'string' &&
        id.length > 127) {

        throw new Error(`Invalid id length: ${id}`);
    }

    return id;
};


exports.compile = function (table, options) {

    if (!options) {
        return false;
    }

    const settings = {
        type: options.type,
        verified: options.type === 'uuid'                    // UUID requires no verification
    };

    if (settings.type === 'increment') {
        settings.table = table._db._generateTable(options.table);
        settings.record = options.record || table.name;
        settings.key = options.key;
        settings.initial = options.initial;
        settings.radix = options.radix;
    }

    return settings;
};


exports.wrap = async function (table, items) {

    if (!table._id) {
        return items;
    }

    const result = [];
    const identifiers = [];
    [].concat(items).forEach((item) => {

        if (item[table.primary] === undefined) {
            item = Hoek.shallow(item);
            identifiers.push(item);
        }

        result.push(item);
    });

    if (!identifiers.length) {
        return items;
    }

    for (let i = 0; i < identifiers.length; ++i) {
        const identifier = identifiers[i];
        identifier[table.primary] = await internals[table._id.type](table);
    }

    return (Array.isArray(items) ? result : result[0]);
};


exports.uuid = function () {

    // Based on node-uuid - https://github.com/broofa/node-uuid - Copyright (c) 2010-2012 Robert Kieffer - MIT License

    const b = internals.byteToHex;
    const buf = Crypto.randomBytes(16);

    buf[6] = (buf[6] & 0x0f) | 0x40;            // Per RFC 4122 (4.4) - set bits for version and clock_seq_hi_and_reserved
    buf[8] = (buf[8] & 0x3f) | 0x80;

    return (b[buf[0]] + b[buf[1]] + b[buf[2]] + b[buf[3]] + '-' +
        b[buf[4]] + b[buf[5]] + '-' +
        b[buf[6]] + b[buf[7]] + '-' +
        b[buf[8]] + b[buf[9]] + '-' +
        b[buf[10]] + b[buf[11]] + b[buf[12]] + b[buf[13]] + b[buf[14]] + b[buf[15]]);
};


internals.uuid = function (table) {

    return exports.uuid();
};


internals.increment = async function (table) {

    const allocated = await exports.verify(table, { allocate: true });

    if (allocated) {
        return internals.radix(allocated, table._id.radix);
    }

    try {
        const value = await table._id.table.next(table._id.record, table._id.key, 1);
        return internals.radix(value, table._id.radix);
    }
    catch (err) {
        err.message = `Failed allocating increment id: ${table.name}`;
        throw err;
    }
};


internals.radix = function (value, radix) {

    if (radix <= 36) {
        return value.toString(radix);
    }

    return Radix62.to(value);
};


exports.verify = async function (table, options) {

    if (!table._id ||
        table._id.verified) {

        return;
    }

    const create = {};
    create[table._id.table.name] = { purge: false, secondary: false };
    try {
        await table._db._createTable(create);
    }
    catch (err) {
        err.message = `Failed creating increment id table: ${table.name}`;
        throw err;
    }

    try {
        const record = await table._id.table.get(table._id.record);

        // Record found

        let initialId = table._id.initial - 1;
        let allocatedId = null;

        if (options.allocate) {
            ++initialId;
            allocatedId = initialId;
        }

        if (record) {
            if (record[table._id.key] === undefined) {

                // Set key

                const changes = {};
                changes[table._id.key] = initialId;
                try {
                    await table._id.table.update(table._id.record, changes);
                }
                catch (err) {
                    err.message = `Failed initializing key-value pair to increment id record: ${table.name}`;
                    throw err;
                }

                table._id.verified = true;
                return allocatedId;
            }

            if (!Hoek.isInteger(record[table._id.key])) {
                throw Boom.internal(`Increment id record contains non-integer value: ${table.name}`);
            }

            table._id.verified = true;
            return;
        }

        // Insert record

        const item = { id: table._id.record };
        item[table._id.key] = initialId;
        try {
            const key = await table._id.table.insert(item)
            table._id.verified = true;
            return allocatedId;
        }
        catch (err) {
            err.message = `Failed inserting increment id record: ${table.name}`;
            throw err;
        }
    }
    catch (err) {
        err.message = `Failed verifying increment id record: ${table.name}`;
        throw err;
    }
};
