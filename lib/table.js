'use strict';

// Load Modules

const Boom = require('boom');
const RethinkDB = require('rethinkdb');
const Cursor = require('./cursor');
const Criteria = require('./criteria');


// Declare internals

const internals = {};


exports = module.exports = class {

    constructor(name, db) {

        this._name = name;
        this._db = db;

        this._table = RethinkDB.db(this._db._name).table(name);
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

        this._run(this._table.insert(items), 'insert', items, callback, (ignore, result) => {

            // Single item

            if (!Array.isArray(items)) {
                return callback(null, items.id !== undefined ? items.id : result.generated_keys[0]);
            }

            // Items array

            const generated = result.generated_keys || [];
            if (generated.length === items.length) {
                return callback(null, result.generated_keys);
            }

            // Mixed array

            const ids = [];
            let g = 0;
            for (let i = 0; i < items.length; ++i) {
                if (items[i].id !== undefined) {
                    ids.push(items[i].id);
                }
                else {
                    ids.push(result.generated_keys[g++]);
                }
            }

            return callback(null, ids);
        });
    }

    update(id, changes, callback) {

        this._run(this._table.get(id).update(changes), 'update', { id: id, changes: changes }, callback, (ignore, result) => {

            if (!result.replaced &&
                !result.unchanged) {

                return this._error('update', 'No item found to update', { id: id, changes: changes }, callback);
            }

            return callback(null);
        });
    }

    increment(id, field, value, callback) {

        const changes = {};
        changes[field] = RethinkDB.row(field).add(value);
        this._run(this._table.get(id).update(changes, { returnChanges: true }), 'increment', { id: id, field: field, value: value }, callback, (ignore, result) => {

            if (!result.replaced) {
                return this._error('increment', 'No item found to update', { id: id, field: field, value: value }, callback);
            }

            const inc = result.changes[0].new_val[field];
            return callback(null, inc);
        });
    }

    append(id, field, value, callback) {

        const changes = {};
        changes[field] = RethinkDB.row(field).append(value);
        this._run(this._table.get(id).update(changes), 'append', { id: id, field: field, value: value }, callback, (ignore, result) => {

            if (!result.replaced) {
                return this._error('append', 'No item found to update', { id: id, field: field, value: value }, callback);
            }

            return callback(null);
        });
    }

    unset(id, fields, callback) {

        const changes = (item) => item.without(fields);

        this._run(this._table.get(id).replace(changes), 'unset', { id: id, fields: fields }, callback, (ignore, result) => {

            if (!result.replaced &&
                !result.unchanged) {

                return this._error('unset', 'No item found to update', { id: id, fields: fields }, callback);
            }

            return callback(null);
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

        this._table.sync().run(this._db._connection, (err, result) => {

            if (err) {
                return this._error('sync', err, null, callback);
            }

            return callback();
        });
    }

    changes(criteria, each, callback) {

        callback = callback || each;

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

        request.changes().run(this._db._connection, { includeStates: true }, (err, cursor) => {

            if (err) {
                return this._error('changes', err, criteria, callback);
            }

            this._cursors.push(cursor);

            let isReady = false;
            cursor.each((err, item) => {

                if (err) {
                    return this._error('changes', err, criteria, each);
                }

                if (item.state) {
                    isReady = (item.state === 'ready');
                    return;
                }

                if (isReady) {
                    return each(null, { before: item.old_val || null, after: item.new_val || null });
                }
            });

            return callback(null, new Cursor(cursor));
        });
    }

    _run(request, action, inputs, callback, next) {

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

        return callback(Boom.internal('Database error', { error: err, table: this._name, action: action, inputs: inputs }));
    }
};
