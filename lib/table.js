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
const Unique = require('./unique');


// Declare internals

const internals = {
    changeTypes: {
        add: 'insert',
        remove: 'remove',
        change: 'update',
        initial: 'initial'
        // uninitial: can only happen in sorted requests which are not supported
    }
};


exports = module.exports = class {

    constructor(name, db, options) {

        this.name = name;
        this.primary = options.primary || 'id';
        this._db = db;
        this._id = Id.compile(this, options.id);
        this._unique = Unique.compile(this, options.unique);

        this._table = RethinkDB.db(this._db.name).table(name);
        this._cursors = [];
    }

    get(id, index, callback) {

        if (!callback) {
            callback = index;
            index = null;
        }

        if (index) {
            this._run(this._table.getAll(Array.isArray(id) ? RethinkDB.args(id) : id, { index }), 'get', id, callback);
            return;
        }

        if (Array.isArray(id)) {
            for (let i = 0; i < id.length; ++i) {
                if (id[i].toString().length > 127) {
                    return this._error('get', 'Invalid id length', id, Hoek.nextTick(callback));
                }
            }

            this._run(this._table.getAll(RethinkDB.args(id)), 'get', id, callback);
            return;
        }

        if (id.toString().length > 127) {
            return this._error('get', 'Invalid id length', id, Hoek.nextTick(callback));
        }

        this._run(this._table.get(id), 'get', id, callback);
    }

    all(callback) {

        this._run(this._table, 'all', null, callback);
    }

    exist(id, callback) {

        if (id.toString().length > 127) {
            return this._error('exist', 'Invalid id length', id, Hoek.nextTick(callback));
        }

        this._run(this._table.get(id).ne(null), 'exist', id, callback);
    }

    query(criteria, options, callback) {

        if (!callback) {
            callback = options;
            options = {};
        }

        let selection = Criteria.select(criteria, this._table);

        if (options.sort) {
            const sort = (typeof options.sort === 'string' || Array.isArray(options.sort) ? { key: options.sort } : options.sort);
            const order = (sort.order === 'descending' ? 'desc' : 'asc');
            selection = selection.orderBy(RethinkDB[order](Criteria.row(sort.key)));
        }

        if (options.from) {
            selection = selection.skip(options.from);
        }

        if (options.count) {
            selection = selection.limit(options.count);
        }

        if (options.filter) {
            selection = selection.pluck(options.filter);
        }

        this._run(selection, 'query', { criteria, options }, callback);
    }

    single(criteria, callback) {

        this._run(Criteria.select(criteria, this._table), 'single', criteria, callback, (ignore, result) => {

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

        this._run(Criteria.select(criteria, this._table).count(), 'count', { criteria }, callback);
    }

    insert(items, options, callback) {

        if (!callback) {
            callback = options;
            options = {};
        }

        const diag = { items, options };

        Id.wrap(this, items, (err, wrapped) => {

            if (err) {
                return this._error('insert', err, diag, callback);
            }

            Unique.reserve(this, wrapped, null, (err) => {

                if (err) {
                    return this._error('insert', err, diag, callback);
                }

                this._run(this._table.insert(wrapped, { conflict: options.merge ? 'update' : 'error' }), 'insert', diag, callback, (ignore, result) => {

                    // Single item

                    if (!Array.isArray(wrapped)) {
                        return callback(null, wrapped[this.primary] !== undefined ? wrapped[this.primary] : result.generated_keys[0]);
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
                        if (wrapped[i][this.primary] !== undefined) {
                            ids.push(wrapped[i][this.primary]);
                        }
                        else {
                            ids.push(result.generated_keys[g++]);
                        }
                    }

                    return callback(null, ids);
                });
            });
        });
    }

    update(id, changes, callback) {

        Hoek.assert(changes && typeof changes === 'object', 'Invalid changes object');

        const diag = { id, changes };

        Unique.reserve(this, changes, id, (err, postUnique) => {

            if (err) {
                return this._error('update', err, diag, callback);
            }

            const wrapped = Modifier.wrap(changes);
            const opts = { returnChanges: !!postUnique };
            this._run(this._table.get(id).replace(wrapped, opts), 'update', diag, callback, (ignore, result) => {

                if (!postUnique) {
                    return callback(null);
                }

                return postUnique(result.changes, callback);
            });
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
            return this._error('sync', 'Database disconnected', null, Hoek.nextTick(callback));
        }

        this._table.sync().run(this._db._connection, (err, result) => {

            if (err) {
                return this._error('sync', err, null, callback);
            }

            return callback(null);
        });
    }

    index(names, callback) {

        names = [].concat(names);
        const each = (name, next) => {

            this._run(this._table.indexCreate(name), 'index', null, (err, result) => {

                return next(err);
            });
        };

        Items.parallel(names, each, callback);
    }

    changes(criteria, options, callback) {

        if (!this._db._connection) {
            return this._error('changes', 'Database disconnected', criteria, Hoek.nextTick(callback));
        }

        if (typeof options !== 'object') {
            options = { handler: options };
        }

        Hoek.assert(typeof options.handler === 'function', 'Invalid options.handler handler');

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

        const feedId = (options.reconnect !== false ? Id.uuid() : null);    // Defaults to true
        if (feedId) {
            this._db._feeds[feedId] = { table: this, criteria, options };   // Keep original criteria reference to allow modifications during reconnection
        }

        const settings = {
            includeTypes: true,
            includeStates: false,
            includeInitial: options.initial || false
        };

        request.changes().run(this._db._connection, settings, (err, dbCursor) => {

            if (err) {
                return this._error('changes', err, criteria, callback);
            }

            const cursor = new Cursor(dbCursor, this, feedId);
            const each = (item, next) => {

                const type = internals.changeTypes[item.type];
                if (type === 'initial' &&
                    item.new_val === null) {

                    return next();                          // Initial result for missing id
                }

                const update = {
                    id: item.old_val ? item.old_val[this.primary] : item.new_val[this.primary],
                    type,
                    before: item.old_val || null,
                    after: item.new_val || null
                };

                options.handler(null, update);
                return next();
            };

            dbCursor.eachAsync(each, (err) => {

                // Changes cursor ends only with an error

                if (err.msg === 'Cursor is closed.') {
                    return;
                }

                const disconnected = (err.msg === 'Connection is closed.');
                const willReconnect = (disconnected && feedId && this._db._willReconnect()) || false;
                cursor.close(false);

                if (feedId &&
                    !willReconnect) {

                    delete this._db._feeds[feedId];
                }

                return this._error('changes', err, criteria, options.handler, { disconnected, willReconnect });
            });

            return callback(null, cursor);
        });
    }

    _run(request, action, inputs, callback, next) {

        if (!this._db._connection) {
            return this._error(action, 'Database disconnected', inputs, Hoek.nextTick(callback));
        }

        next = next || callback;                                        // next() must never return an error

        request.run(this._db._connection, (err, result) => {

            if (err) {
                return this._error(action, err, inputs, callback);
            }

            if (result === null) {
                return next(null, null);
            }

            if (result.errors) {
                return this._error(action, result.first_error, inputs, callback);
            }

            // Single item

            if (typeof result.toArray !== 'function' ||
                Array.isArray(result)) {

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

    _error(action, err, inputs, callback, flags) {

        const message = (typeof err === 'string' ? err : 'Database error');
        const data = { error: err, table: this.name, action, inputs };
        const error = Boom.internal(message, data);

        if (flags) {
            error.flags = flags;
        }

        return callback(error);
    }
};
