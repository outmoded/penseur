'use strict';

// Load Modules

const Boom = require('boom');
const Bounce = require('bounce');
const Hoek = require('hoek');
const Joi = require('joi');
const RethinkDB = require('rethinkdb');

const Id = require('./id');
const Special = require('./special');
const Table = require('./table');
const Unique = require('./unique');


// Declare internals

const internals = {
    comparators: {
        '<': 'lt',
        '>': 'gt',
        '=': 'eq',
        '==': 'eq',
        '<=': 'le',
        '>=': 'ge',
        '!=': 'ne',
        lt: 'lt',
        gt: 'gt',
        eq: 'eq',
        ne: 'ne',
        le: 'le',
        ge: 'ge'
    }
};


internals.unique = Joi.object({
    path: Joi.array().items(Joi.string()).min(1).single().required(),
    table: Joi.string(),                                                    // Defaults to penseur_unique_{table}_{unique_path_path}
    key: Joi.string().default('holder')
});


internals.secondaryIndex = Joi.object({
    name: Joi.string().required(),
    source: Joi.alternatives([
        Joi.array().items(Joi.string()).min(2),         // List of row fields
        Joi.func()                                      // A function (row) => { }
    ]),
    options: Joi.object({
        multi: Joi.boolean().default(false),
        geo: Joi.boolean().default(false)
    })
        .default({})
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

        test: [Joi.boolean(), Joi.object()],
        extended: Joi.any(),
        onConnect: Joi.func().default(Hoek.ignore),
        onError: Joi.func().default(Hoek.ignore),
        onDisconnect: Joi.func().default(Hoek.ignore),
        reconnect: Joi.boolean().default(true),
        reconnectTimeout: Joi.number().integer().min(1).allow(false).default(100)
    }),
    table: Joi.object({
        extended: Joi.any(),
        purge: Joi.boolean().default(true),
        secondary: Joi.array().items(Joi.string(), internals.secondaryIndex).single().default([]).allow(false),
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
        primary: Joi.string().max(127),
        geo: Joi.boolean().default(false)
    })
        .allow(true, false)
};


exports = module.exports = internals.Db = class {

    constructor(name, options) {

        this._settings = Hoek.cloneWithShallow(Joi.attempt(options || {}, internals.schema.db, 'Invalid database options'), ['test']);
        this.name = name;
        this._connection = null;
        this._connectionOptions = null;
        this._feeds = {};                       // uuid -> { table, criteria, options }

        if (this._settings.test) {
            this.disable = internals.disable;
            this.enable = internals.enable;
        }

        this.tables = {};
        this.r = RethinkDB;
    }

    async connect() {

        await this._connect();
        const exists = await this._exists();

        if (!exists) {
            this.close();
            throw Boom.internal(`Missing database: ${this.name}`);
        }

        await this._verify();

        // Reconnect changes feeds

        const feeds = Object.keys(this._feeds);
        for (let i = 0; i < feeds.length; ++i) {
            const feedId = feeds[i];
            const feed = this._feeds[feedId];
            await feed.table.changes(feed.criteria, feed.options);
        }
    }

    async _connect() {

        const settings = this._connectionOptions || {};
        if (!this._connectionOptions) {
            ['host', 'port', 'db', 'authKey', 'timeout', 'ssl', 'user', 'password'].forEach((item) => {

                if (this._settings[item] !== undefined) {
                    settings[item] = this._settings[item];
                }
            });
        }

        const connection = await RethinkDB.connect(settings);
        this._connection = connection;
        this._connectionOptions = settings;

        this._connection.on('error', (err) => this._settings.onError(err));
        this._connection.on('timeout', () => this._settings.onError(Boom.internal('Database connection timeout')));
        this._connection.once('close', async () => {

            const reconnect = this._willReconnect();
            this._settings.onDisconnect(reconnect);

            if (!reconnect) {
                return;
            }

            let first = true;
            const loop = async (err) => {

                first = false;
                await Hoek.wait(this._settings.reconnectTimeout && !first ? this._settings.reconnectTimeout : 0);

                this._settings.onError(err);
                try {
                    await this.connect();
                }
                catch (err) {
                    Bounce.rethrow(err, 'system');
                    await loop(err);
                }
            };

            try {
                await this.connect();
            }
            catch (err) {
                Bounce.rethrow(err, 'system');
                await loop(err);
            }
        });

        this._settings.onConnect();
    }

    _willReconnect() {

        return (this._settings.reconnect && !!this._connectionOptions);
    }

    async close() {

        this._connectionOptions = null;     // Stop reconnections

        if (!this._connection) {
            return;
        }

        // Close change stream cursors

        Object.keys(this.tables).forEach((name) => {

            const table = this.tables[name];
            table._cursors.forEach((cursor) => cursor.close());
            table._cursors = [];
        });

        // Close connection

        await this._connection.close();
        if (this._connection) {
            this._connection.removeAllListeners();
            this._connection = null;
        }
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

        if (!tables) {
            const byName = {};
            Object.keys(this.tables).forEach((name) => {

                byName[name] = this.tables[name]._settings;
            });

            return byName;
        }

        const normalize = (opts) => {

            if (opts.id &&
                typeof opts.id === 'string') {

                opts = Object.assign({}, opts);     // Shallow cloned
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

        tables = Object.assign({}, tables);         // Shallow cloned
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

    async establish(tables) {

        if (!this._connection) {
            await this._connect();
            return this.establish(tables);
        }

        const byName = this._normalizeTables(tables);
        this.table(byName);

        const exists = await this._exists();
        if (!exists) {
            await RethinkDB.dbCreate(this.name).run(this._connection);
        }

        await this._createTable(byName);
        return this._verify();
    }

    async _exists() {

        const names = await RethinkDB.dbList().run(this._connection);
        return (names.indexOf(this.name) !== -1);
    }

    async _createTable(tables) {

        const configs = await RethinkDB.db(this.name).tableList().map((table) => RethinkDB.db(this.name).table(table).config()).run(this._connection);

        const existing = {};
        configs.forEach((config) => {

            existing[config.name] = config;
        });

        const names = Object.keys(tables);
        for (let i = 0; i < names.length; ++i) {
            const name = names[i];

            let tableOptions = tables[name];
            if (tableOptions === false) {
                continue;
            }

            if (tableOptions === true) {
                tableOptions = {};
            }

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
                await change.run(this._connection);
            }
            else {

                // Reuse existing table

                if (tableOptions.purge !== false) {              // Defaults to true
                    await this.tables[name].empty();
                }

                if (tableOptions.secondary !== false) {         // false means leave as-is (vs null or empty array which drops existing)
                    for (let j = 0; j < existingConfig.indexes.length; ++j) {
                        const index = existingConfig.indexes[j];
                        await RethinkDB.db(this.name).table(name).indexDrop(index).run(this._connection);
                    }
                }
            }

            if (!tableOptions.secondary) {
                continue;
            }

            await this.tables[name].index(tableOptions.secondary);
        }
    }

    async _verify() {

        const names = Object.keys(this.tables);
        for (let i = 0; i < names.length; ++i) {
            const name = names[i];
            const table = this.tables[name];
            await Id.verify(table, { allocate: false });
            await Unique.verify(table);
        }
    }

    async run(request, options = {}) {

        // Extract table name from ReQL object

        let ref = request;
        let table;
        while (ref.args.length) {
            if (ref.args[1]) {
                table = ref.args[1].data;
            }

            ref = ref.args[0];
        }

        const track = { table, action: 'run' };

        try {
            return await this._run(request, track, options);
        }
        catch (err) {
            throw Table.error(err, track);
        }
    }

    async _run(request, track, options) {

        if (!this._connection) {
            throw new Boom('Database disconnected');
        }

        if (this._settings.test &&
            typeof this._settings.test === 'object') {

            const { table, action, inputs = null } = track;
            this._settings.test[table] = this._settings.test[table] || [];
            this._settings.test[table].push({ action, inputs });
        }

        const result = await request.run(this._connection, options);
        if (result === null) {
            return null;
        }

        if (result.errors) {
            throw new Boom(result.first_error);
        }

        // Single item

        if (typeof result.toArray !== 'function' ||
            Array.isArray(result)) {

            return internals.empty(result);
        }

        // Cursor

        const results = await result.toArray();
        result.close();
        return internals.empty(results);
    }

    // Criteria

    static or(values) {

        return new Special('or', values);
    }

    static contains(values, options) {

        return new Special('contains', values, options);
    }

    static match(values, options) {

        return new Special('match', values, options);
    }

    static near(coordinates, distance, unit) {

        return new Special('near', coordinates, { distance, unit });
    }

    static not(values) {

        return new Special('or', [].concat(values), { not: true });
    }

    static is(operator, value, ...and) {

        const comparator = internals.comparators[operator];
        Hoek.assert(comparator, `Unknown comparator: ${operator}`);
        Hoek.assert(value !== undefined, 'Missing value argument');

        const flags = { comparator };
        if (and.length) {
            Hoek.assert(!(and.length % 2), 'Cannot have odd number of arguments');

            flags.and = [];
            for (let i = 0; i < and.length; i += 2) {
                const c = internals.comparators[and[i]];
                const v = and[i + 1];

                Hoek.assert(v !== undefined, 'Missing value argument');
                Hoek.assert(c, `Unknown comparator: ${and[i]}`);

                flags.and.push({ comparator: c, value: v });
            }
        }

        return new Special('is', value, flags);
    }

    static by(index, values) {

        return new Special('by', [].concat(values), { index });
    }

    static empty() {

        return new Special('empty');
    }

    // Criteria or Modifier

    static unset() {

        return new Special('unset');
    }

    // Modifier

    static increment(value) {

        return new Special('increment', value);
    }

    static append(value, options = {}) {                        // { single: false, create: false, unique: false }

        if (options.unique) {                                   // true, false, 'any', 'last', { match, path }
            Hoek.assert(options.single || !Array.isArray(value), 'Cannot append multiple values with unique requirements');

            if (typeof options.unique !== 'object') {
                options = Object.assign({}, options);           // Shallow clone

                if (options.unique === true) {
                    options.unique = { match: 'any' };          // match: any, last
                }
                else {
                    options.unique = { match: options.unique };
                }
            }

            options.unique.match = options.unique.match || 'any';
        }

        return new Special('append', value, options);
    }

    static override(value) {

        return new Special('override', value);
    }
};


internals.Db.prototype.append = internals.Db.append;
internals.Db.prototype.by = internals.Db.by;
internals.Db.prototype.contains = internals.Db.contains;
internals.Db.prototype.empty = internals.Db.empty;
internals.Db.prototype.increment = internals.Db.increment;
internals.Db.prototype.is = internals.Db.is;
internals.Db.prototype.match = internals.Db.match;
internals.Db.prototype.near = internals.Db.near;
internals.Db.prototype.not = internals.Db.not;
internals.Db.prototype.or = internals.Db.or;
internals.Db.prototype.override = internals.Db.override;
internals.Db.prototype.unset = internals.Db.unset;


internals.disable = function (table, method, options) {

    options = options || {};

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    if (method === 'changes' &&
        options.updates) {

        this.tables[table].changes = function (criteria, each) {

            const error = Table.error(Boom.internal('Simulated database error'), { table, action: method });
            error.flags = Hoek.applyToDefaults({ willReconnect: true, disconnected: true }, options.flags || {});
            process.nextTick(() => each(error));
            return { close: Hoek.ignore };
        };

        return;
    }

    this.tables[table][method] = internals.disabled(table, method, options);
};


internals.disabled = function (table, method, options) {

    const value = options.value;

    return function () {

        if (value !== undefined) {
            if (value instanceof Error) {
                return Promise.reject(value);
            }

            return value;
        }

        return Promise.reject(Table.error(Boom.internal('Simulated database error'), { table, action: method }));
    };
};


internals.enable = function (table, method) {

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    this.tables[table][method] = Table.prototype[method];
};


internals.empty = function (results) {

    if (!results ||
        !Array.isArray(results)) {

        return results;
    }

    return (results.length ? results : null);
};
