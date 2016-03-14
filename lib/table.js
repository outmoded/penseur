'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');
const Items = require('items');
const RethinkDB = require('rethinkdb');
const Cursor = require('./cursor');
const Criteria = require('./criteria');
const Id = require('./id');
const Modifier = require('./modifier');


// Declare internals

const internals = {};


exports = module.exports = class {

    constructor(name, db, options) {

        this.name = name;
        this._db = db;
        this._id = null;

        if (options.id) {
            this._id = {
                type: options.id.type,
                verified: options.id.type === 'uuid'                    // UUID requires no verification
            };

            if (this._id.type === 'increment') {
                this._id.table = this._db._generateTable(options.id.table);
                this._id.record = options.id.record || name;
                this._id.key = options.id.key;
                this._id.initial = options.id.initial;
                this._id.radix = options.id.radix;
            }
        }

        this._table = RethinkDB.db(this._db.name).table(name);
        this._cursors = [];
    }

    get(id, callback) {

        if (Array.isArray(id)) {
            this._run(this._table.getAll(RethinkDB.args(id)), 'get', id, callback);
            return;
        }

        this._run(this._table.get(id), 'get', id, callback);
    }

    query(criteria, callback) {

        this._run(Criteria.wrap(criteria).select(this._table), 'query', criteria, callback);
    }

    single(criteria, callback) {

        this._run(Criteria.wrap(criteria).select(this._table), 'single', criteria, callback, (ignore, result) => {

            if (!result) {
                return callback(null, null);
            }

            if (result.length !== 1) {
                return this._error('single', 'Found multiple items', criteria, callback);
            }

            return callback(null, result[0]);
        });
    }

    count(criteria, callback) {

        this._run(Criteria.wrap(criteria).select(this._table).count(), 'count', { criteria: criteria }, callback);
    }

    insert(items, callback) {

        Id.wrap(this, items, (err, wrapped) => {

            if (err) {
                return callback(err);
            }

            this._run(this._table.insert(wrapped), 'insert', wrapped, callback, (ignore, result) => {

                // Single item

                if (!Array.isArray(wrapped)) {
                    return callback(null, wrapped.id !== undefined ? wrapped.id : result.generated_keys[0]);
                }

                // Items array

                const generated = result.generated_keys || [];
                if (generated.length === wrapped.length) {
                    return callback(null, result.generated_keys);
                }

                // Mixed array

                const ids = [];
                let g = 0;
                for (let i = 0; i < wrapped.length; ++i) {
                    if (wrapped[i].id !== undefined) {
                        ids.push(wrapped[i].id);
                    }
                    else {
                        ids.push(result.generated_keys[g++]);
                    }
                }

                return callback(null, ids);
            });
        });
    }

    update(id, changes, callback) {

        const diag = { id, changes };
        const wrapped = Modifier.wrap(changes);
        this._run(this._table.get(id)[typeof wrapped === 'object' ? 'update' : 'replace'](wrapped), 'update', diag, callback, (ignore, result) => {

            if (!result.replaced &&
                !result.unchanged) {

                return this._error('update', 'No item found to update', diag, callback);
            }

            return callback(null);
        });
    }

    next(id, field, value, callback) {

        const changes = {};
        changes[field] = RethinkDB.row(field).add(value);

        const diag = { id, field, value };
        this._run(this._table.get(id).update(changes, { returnChanges: true }), 'next', diag, callback, (ignore, result) => {

            if (!result.replaced) {
                return this._error('next', 'No item found to update', diag, callback);
            }

            const inc = result.changes[0].new_val[field];
            return callback(null, inc);
        });
    }

    remove(criteria, callback) {

        const isSingle = (typeof criteria !== 'object');
        const selection = (isSingle ? this._table.get(criteria)
                                  : (Array.isArray(criteria) ? this._table.getAll(RethinkDB.args(criteria))
                                                             : this._table.filter(criteria)));

        this._run(selection.delete(), 'remove', criteria, callback, (ignore, result) => {

            if (isSingle &&
                !result.deleted) {

                return this._error('remove', 'No item found to remove', criteria, callback);
            }

            return callback(null);
        });
    }

    empty(callback) {

        this._run(this._table.delete(), 'empty', null, (err, result) => {

            return callback(err, result ? result.deleted : 0);
        });
    }

    sync(callback) {

        if (!this._db._connection) {
            return Hoek.nextTick(callback)(new Error('Database disconnected'));
        }

        this._table.sync().run(this._db._connection, (err, result) => {

            if (err) {
                return this._error('sync', err, null, callback);
            }

            return callback();
        });
    }

    index(names, callback) {

        names = [].concat(names);
        const each = (name, next) => {

            this._run(this._table.indexCreate(name), 'index', null, (err, result) => {

                return next(err);
            });
        };

        Items.serial(names, each, callback);
    }

    changes(criteria, each, callback) {

        callback = callback || each;

        if (!this._db._connection) {
            return Hoek.nextTick(callback)(new Error('Database disconnected'));
        }

        let request = this._table;
        if (criteria !== '*') {
            if (typeof criteria !== 'object') {
                request = this._table.get(criteria);
            }
            else if (Array.isArray(criteria)) {
                request = this._table.getAll(RethinkDB.args(criteria));
            }
            else {
                request = this._table.filter(criteria);
            }
        }

        request.changes().run(this._db._connection, { includeStates: false, includeInitial: false }, (err, cursor) => {

            if (err) {
                return this._error('changes', err, criteria, callback);
            }

            this._cursors.push(cursor);

            cursor.each((err, item) => {

                if (err) {
                    return this._error('changes', err, criteria, each);
                }

                return each(null, { before: item.old_val || null, after: item.new_val || null });
            });

            return callback(null, new Cursor(cursor));
        });
    }

    _run(request, action, inputs, callback, next) {

        if (!this._db._connection) {
            return Hoek.nextTick(callback)(new Error('Database disconnected'));
        }

        next = next || callback;                                        // next() must never return an error

        request.run(this._db._connection, (err, result) => {

            if (err) {
                return this._error(action, err, inputs, callback);
            }

            // Single item

            if (!result ||
                typeof result.toArray !== 'function') {

                return next(null, result);
            }

            // Cursor

            const cursor = result;
            cursor.toArray((err, results) => {

                if (err) {
                    return this._error(action, err, inputs, callback);
                }

                cursor.close();
                return next(null, results.length ? results : null);
            });
        });
    }

    _error(action, err, inputs, callback) {

        return callback(Boom.internal('Database error', { error: err, table: this.name, action: action, inputs: inputs }));
    }
};
