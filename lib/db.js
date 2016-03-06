'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');
const Items = require('items');
const Joi = require('joi');
const RethinkDB = require('rethinkdb');
const Criteria = require('./criteria');
const Modifier = require('./modifier');
const Table = require('./table');


// Declare internals

const internals = {};


internals.schema = Joi.object({

    // Rethink Connection

    host: Joi.string(),
    port: Joi.number(),
    authKey: Joi.string(),
    timeout: Joi.number(),
    ssl: {
        ca: Joi.any()
    },

    // Penseur options

    test: Joi.boolean(),
    extended: Joi.any(),
    onConnect: Joi.func().default(Hoek.ignore),
    onError: Joi.func().default(Hoek.ignore),
    onDisconnect: Joi.func().default(Hoek.ignore),
    reconnect: Joi.boolean().default(true)
});


exports = module.exports = class {

    constructor(name, options) {

        this._settings = Hoek.clone(Joi.attempt(options || {}, internals.schema, 'Invalid options'));
        this.name = name;
        this._connection = null;

        if (this._settings.test) {
            this.disable = internals.disable;
            this.enable = internals.enable;
        }

        this.tables = {};
    }

    connect(callback) {

        const settings = this._connectionOptions || {};
        if (!this._connectionOptions) {
            ['host', 'port', 'db', 'authKey', 'timeout', 'ssl'].forEach((item) => {

                if (this._settings[item] !== undefined) {
                    settings[item] = this._settings[item];
                }
            });
        }

        RethinkDB.connect(settings, (err, connection) => {

            if (err) {
                return callback(err);
            }

            this._connection = connection;
            this._connectionOptions = settings;

            this._connection.on('error', (err) => this._settings.onError(err));
            this._connection.on('timeout', () => this._settings.onError(new Error('Database connection timeout')));
            this._connection.once('close', () => {

                this._settings.onDisconnect(this._settings.reconnect && !!this._connectionOptions);
                if (this._connectionOptions) {
                    this.connect(Hoek.ignore);
                }
            });

            this._settings.onConnect();
            return callback(null);
        });
    }

    close(next) {

        next = next || Hoek.ignore;

        this._connectionOptions = null;     // Stop reconnections

        if (!this._connection) {
            return next();
        }

        // Close change stream cursors

        const tables = Object.keys(this.tables);
        for (let i = 0; i < tables.length; ++i) {
            const table = this.tables[tables[i]];
            for (let j = 0; j < table._cursors.length; ++j) {
                table._cursors[j].close();
            }

            table._cursors = [];
        }

        // Close connection

        this._connection.close((err) => {     // Explicit callback to avoid generating a promise

            this._connection.removeAllListeners();
            this._connection = null;

            return next(err);
        });
    }

    table(tables, options) {

        options = options || {};

        const Proto = options.extended || this._settings.extended || Table;

        tables = [].concat(tables);

        for (let i = 0; i < tables.length; ++i) {
            const table = tables[i];
            if (this.tables[table]) {
                return;
            }

            const record = new Proto(table, this);

            // Decorate object with tables

            this.tables[table] = record;
            if (!this[table] &&
                table[0] !== '_') {                 // Do not override prototype or private members

                this[table] = record;
            }
        }
    }

    establish(tables, callback) {

        // Connect if not connected already

        if (!this._connection) {
            this.connect((err) => {

                if (err) {
                    return callback(err);
                }

                return this.establish(tables, callback);
            });

            return;
        }

        RethinkDB.dbList().run(this._connection, (err, names) => {

            if (err) {
                return callback(err);
            }

            if (names.indexOf(this.name) === -1) {

                // Create new database

                RethinkDB.dbCreate(this.name).run(this._connection, (err, created) => {

                    if (err) {
                        return callback(err);
                    }

                    return this._createTable(tables, callback);
                });
            }
            else {

                // Reuse existing

                return this._createTable(tables, callback);
            }
        });
    }

    _createTable(tables, callback) {

        let names = tables;
        const tablesOptions = {};
        if (!Array.isArray(tables)) {
            names = Object.keys(tables);
            for (let i = 0; i < names.length; ++i) {
                const name = names[i];
                tablesOptions[name] = (!tables[name] ? false : (tables[name] === true ? {} : tables[name]));
            }
        }

        RethinkDB.db(this.name).tableList().run(this._connection, (err, existing) => {

            if (err) {
                return callback(err);
            }

            const each = (name, next) => {

                if (tablesOptions[name] === false) {
                    return next();
                }

                const options = tablesOptions[name] || {};
                this.table(name, options);

                const finalize = (err, indexes) => {

                    if (err) {
                        return next(err);
                    }

                    if (indexes) {
                        return this.tables[name].index(indexes, next);
                    }

                    return next();
                };

                // Create new table

                if (existing.indexOf(name) === -1) {
                    return RethinkDB.db(this.name).tableCreate(name).run(this._connection, (err) => finalize(err, options.index));
                }

                // Reuse existing table

                const recreate = () => {

                    RethinkDB.db(this.name).table(name).indexList().run(this._connection, (err, currentIndexes) => {

                        if (err) {
                            return next(err);
                        }

                        const requestedIndexes = [].concat(options.index || []);
                        const intersection = Hoek.intersect(currentIndexes, requestedIndexes);
                        const creatIndexes = internals.difference(requestedIndexes, intersection);
                        const removeIndesex = internals.difference(currentIndexes, intersection);

                        const eachIndex = (index, nextIndex) => {

                            RethinkDB.db(this.name).table(name).indexDrop(index).run(this._connection, nextIndex);
                        };

                        Items.serial(removeIndesex, eachIndex, (err) => finalize(err, creatIndexes));
                    });
                };

                if (options.purge === false) {              // Defaults to true
                    return recreate();
                }

                this.tables[name].empty((err) => {

                    if (err) {
                        return next(err);
                    }

                    return recreate();
                });
            };

            Items.serial(names, each, callback);
        });
    }

    fields(criteria) {

        return new Criteria(criteria, 'fields');
    }

    or(values) {

        return Criteria.rule('or', values);
    }

    contains(values, options) {

        return Criteria.rule('contains', values, options);
    }

    increment(value) {

        return Modifier.type('increment', value);
    }

    append(value, options) {

        return Modifier.type('append', value, options);     // { single: false }
    }

    unset() {

        return Modifier.type('unset');
    }

    isModifier(value) {

        return (typeof value === 'function' && !!value.type);
    }
};


internals.difference = function (superset, subset) {

    const result = [];
    for (let i = 0; i < superset.length; ++i) {
        if (subset.indexOf(superset[i]) === -1) {
            result.push(superset[i]);
        }
    }

    return result;
};


internals.disable = function (table, method, options) {

    options = options || {};

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    if (method === 'changes' &&
        options.updates) {

        this.tables[table].changes = function (criteria, each, callback) {

            process.nextTick(() => each(Boom.internal('Simulated database error')));
            return callback(null, { close: Hoek.ignore });
        };

        return;
    }

    this.tables[table][method] = internals.disabled;
};


internals.disabled = function () {

    const callback = arguments[arguments.length - 1];
    return callback(Boom.internal('Simulated database error'));
};


internals.enable = function (table, method) {

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    this.tables[table][method] = Table.prototype[method];
};
