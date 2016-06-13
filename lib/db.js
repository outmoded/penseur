'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');
const Items = require('items');
const Joi = require('joi');
const RethinkDB = require('rethinkdb');
const Id = require('./id');
const Table = require('./table');
const Unique = require('./unique');


// Declare internals

const internals = {};


internals.unique = Joi.object({
    path: Joi.array().items(Joi.string()).min(1).single().required(),
    table: Joi.string(),                                                    // Defaults to penseur_unique_{table}_{unique_path_path}
    key: Joi.string().default('holder')
});


internals.schema = {
    db: Joi.object({

        // Rethink Connection

        host: Joi.string(),
        port: Joi.number(),
        authKey: Joi.string(),
        user: Joi.string(),
        password: Joi.string(),
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
    }),
    table: Joi.object({
        extended: Joi.any(),
        purge: Joi.boolean().default(true),
        secondary: Joi.array().items(Joi.string()).single().default([]).allow(false),
        id: [
            {
                type: 'uuid'
            },
            {
                type: 'increment',
                table: Joi.string().default('penseur_id_allocate'),
                record: Joi.string(),                                       // Defaults to table name
                key: Joi.string().default('value'),
                initial: Joi.number().integer().min(0).default(1),
                radix: Joi.number().integer().min(2).max(36).allow(62).default(10)
            }
        ],
        unique: Joi.array().items(internals.unique).min(1).single(),
        primary: Joi.string().max(127)
    })
        .allow(true, false)
};


exports = module.exports = internals.Db = class {

    constructor(name, options) {

        this._settings = Hoek.clone(Joi.attempt(options || {}, internals.schema.db, 'Invalid database options'));
        this.name = name;
        this._connection = null;
        this._connectionOptions = null;
        this._feeds = {};                       // uuid -> { table, criteria, options }

        if (this._settings.test) {
            this.disable = internals.disable;
            this.enable = internals.enable;
        }

        this.tables = {};
    }

    connect(callback) {

        this._connect((err) => {

            if (err) {
                return callback(err);
            }

            this._exists((err, exists) => {

                if (err) {
                    return callback(err);
                }

                if (!exists) {
                    this.close();
                    return callback(Boom.internal(`Missing database: ${this.name}`));
                }

                this._verify((err) => {

                    if (err) {
                        return callback(err);
                    }

                    // Reconnect changes feeds

                    const each = (feedId, next) => {

                        const feed = this._feeds[feedId];
                        return feed.table.changes(feed.criteria, feed.options, next);
                    };

                    Items.serial(Object.keys(this._feeds), each, callback);
                });
            });
        });
    }

    _connect(callback) {

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

                this._settings.onDisconnect(this._willReconnect());
                if (this._willReconnect()) {
                    const loop = (err) => {

                        if (!err) {
                            return;
                        }

                        setImmediate(() => {                        // Prevents stack overflow if connect() fails on same tick

                            this._settings.onError(err);
                            this.connect(loop);
                        });
                    };

                    this.connect(loop);
                }
            });

            this._settings.onConnect();
            return callback();
        });
    }

    _willReconnect() {

        return (this._settings.reconnect && !!this._connectionOptions);
    }

    close(next) {

        next = next || Hoek.ignore;

        this._connectionOptions = null;     // Stop reconnections

        if (!this._connection) {
            return next();
        }

        // Close change stream cursors

        Object.keys(this.tables).forEach((name) => {

            const table = this.tables[name];
            table._cursors.forEach((cursor) => cursor.close());
            table._cursors = [];
        });

        // Close connection

        this._connection.close((err) => {     // Explicit callback to avoid generating a promise

            if (this._connection) {
                this._connection.removeAllListeners();
                this._connection = null;
            }

            return next(err);
        });
    }

    table(tables, options) {

        const byName = this._normalizeTables(tables, options);
        const names = Object.keys(byName);
        names.forEach((name) => {

            if (this.tables[name]) {
                return;
            }

            let tableOptions = byName[name];
            if (!tableOptions) {
                return;
            }

            if (tableOptions === true) {
                tableOptions = {};
            }

            // Decorate object with tables

            const record = this._generateTable(name, tableOptions);
            this.tables[name] = record;
            if (!this[name] &&
                name[0] !== '_') {                  // Do not override prototype or private members

                this[name] = record;
            }
        });
    }

    _normalizeTables(tables, options) {

        const normalize = (opts) => {

            if (opts.id &&
                typeof opts.id === 'string') {

                opts = Hoek.shallow(opts);
                opts.id = { type: opts.id };
            }

            return opts;
        };

        // String or array of strings

        if (typeof tables === 'string' ||
            Array.isArray(tables)) {

            options = Joi.attempt(normalize(options || {}), internals.schema.table, 'Invalid table options');

            const byName = {};
            [].concat(tables).forEach((table) => {

                byName[table] = options;
            });

            return byName;
        }

        // Object { name: { options } }

        Hoek.assert(!options, 'Cannot specify options with tables object');

        tables = Hoek.shallow(tables);
        const names = Object.keys(tables);
        names.forEach((name) => {

            tables[name] = Joi.attempt(normalize(tables[name]), internals.schema.table, `Invalid table options: ${name}`);
        });

        return tables;
    }

    _generateTable(name, options) {

        options = options || {};
        const Proto = options.extended || this._settings.extended || Table;
        return new Proto(name, this, options);
    }

    establish(tables, callback) {

        if (!this._connection) {
            this._connect((err) => {

                if (err) {
                    return callback(err);
                }

                return this.establish(tables, callback);
            });

            return;
        }

        const byName = this._normalizeTables(tables);
        this.table(byName);

        const finalize = (err) => {

            if (err) {
                return callback(err);
            }

            return this._createTable(byName, (err) => {

                if (err) {
                    return callback(err);
                }

                return this._verify(callback);
            });
        };

        this._exists((err, exists) => {

            if (err ||
                exists) {

                return finalize(err);
            }

            RethinkDB.dbCreate(this.name).run(this._connection, finalize);
        });
    }

    _exists(callback) {

        RethinkDB.dbList().run(this._connection, (err, names) => {

            if (err) {
                return callback(err);
            }

            return callback(null, names.indexOf(this.name) !== -1);
        });
    }

    _createTable(tables, callback) {

        RethinkDB.db(this.name).tableList().map((table) => RethinkDB.db(this.name).table(table).config()).run(this._connection, (err, configs) => {

            if (err) {
                return callback(err);
            }

            const existing = {};
            configs.forEach((config) => {

                existing[config.name] = config;
            });

            const each = (name, next) => {

                let tableOptions = tables[name];
                if (tableOptions === false) {
                    return next();
                }

                if (tableOptions === true) {
                    tableOptions = {};
                }

                const finalize = (err, indexes) => {

                    if (err) {
                        return next(err);
                    }

                    if (indexes) {
                        return this.tables[name].index(indexes, next);
                    }

                    return next();
                };

                // Check primary key

                const primaryKey = tableOptions.primary || 'id';
                const existingConfig = existing[name];
                let drop = false;
                if (existingConfig &&
                    existingConfig.primary_key !== primaryKey) {

                    drop = RethinkDB.db(this.name).tableDrop(name);
                }

                // Create new table

                if (!existingConfig ||
                    drop) {

                    const create = RethinkDB.db(this.name).tableCreate(name, { primaryKey });
                    const change = (drop ? RethinkDB.and(drop, create) : create);
                    return change.run(this._connection, (err) => finalize(err, tableOptions.secondary));
                }

                // Reuse existing table

                const recreate = () => {

                    if (tableOptions.secondary === false) {         // Defaults to []
                        return finalize();
                    }

                    const requestedIndexes = [].concat(tableOptions.secondary || []);
                    const intersection = Hoek.intersect(existingConfig.indexes, requestedIndexes);
                    const createIndexes = internals.difference(requestedIndexes, intersection);
                    const removeIndexes = internals.difference(existingConfig.indexes, intersection);

                    const eachIndex = (index, nextIndex) => {

                        RethinkDB.db(this.name).table(name).indexDrop(index).run(this._connection, nextIndex);
                    };

                    Items.parallel(removeIndexes, eachIndex, (err) => finalize(err, createIndexes));
                };

                if (tableOptions.purge === false) {              // Defaults to true
                    return recreate();
                }

                this.tables[name].empty((err) => {

                    if (err) {
                        return next(err);
                    }

                    return recreate();
                });
            };

            const names = Object.keys(tables);
            Items.parallel(names, each, callback);
        });
    }

    _verify(callback) {

        const each = (name, next) => {

            const table = this.tables[name];
            Id.verify(table, { allocate: false }, (err) => {

                if (err) {
                    return next(err);
                }

                return Unique.verify(table, next);
            });
        };

        Items.parallel(Object.keys(this.tables), each, callback);
    }

    // Criteria

    static or(values) {

        return internals.special('or', values);
    }

    static contains(values, options) {

        return internals.special('contains', values, options);
    }

    static not(values) {

        return internals.special('or', [].concat(values), { not: true });
    }

    // Criteria or Modifier

    static unset() {

        return internals.special('unset');
    }

    // Modifier

    static increment(value) {

        return internals.special('increment', value);
    }

    static append(value, options) {

        return internals.special('append', value, options);     // { single: false }
    }
};


internals.Db.prototype.or = internals.Db.or;
internals.Db.prototype.contains = internals.Db.contains;
internals.Db.prototype.not = internals.Db.not;
internals.Db.prototype.unset = internals.Db.unset;
internals.Db.prototype.increment = internals.Db.increment;
internals.Db.prototype.append = internals.Db.append;


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

            const error = Boom.internal('Simulated database error');
            error.flags = Hoek.applyToDefaults({ willReconnect: true, disconnected: true }, options.flags || {});
            process.nextTick(() => each(error));
            return callback(null, { close: Hoek.ignore });
        };

        return;
    }

    this.tables[table][method] = internals.disabled(options);
};


internals.disabled = function (options) {

    const value = options.value;

    return function () {

        const callback = arguments[arguments.length - 1];
        if (value !== undefined) {
            return callback(null, value);
        }

        return callback(Boom.internal('Simulated database error'));
    };
};


internals.enable = function (table, method) {

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    this.tables[table][method] = Table.prototype[method];
};


internals.special = function (type, value, options) {

    options = options || {};

    const special = function () { };                // Return function because 1. typeof fastest 2. illegal database type
    special.type = type;
    special.flags = Hoek.clone(options);

    if (value !== undefined) {
        special.value = value;
    }

    return special;
};
