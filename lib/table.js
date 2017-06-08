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

        this.raw = RethinkDB.db(this._db.name).table(name);
        this._cursors = [];
    }

    get(ids, options, callback) {

        if (!callback) {
            callback = options;
            options = {};
        }

        const diag = { ids, options };

        const batch = Array.isArray(ids);
        ids = Id.normalize(ids, true);
        if (ids instanceof Error) {
            return this._error('get', ids.message, diag, Hoek.nextTick(callback));
        }

        if (batch) {
            return this._run(this._refine(this.raw.getAll(RethinkDB.args(ids)), options), 'get', diag, callback);
        }

        return this._run(this._refine(this.raw.get(ids), options), 'get', diag, callback);
    }

    _refine(selection, options, fullTable) {

        if (options.sort ||
            options.from !== undefined ||           // Consider explicit start from zero as sorted
            options.count) {

            const sort = (options.sort ? (typeof options.sort === 'string' || Array.isArray(options.sort) ? { key: options.sort } : options.sort) : { key: this.primary });
            const descending = (sort.order === 'descending');
            const key = (typeof sort.key === 'string' ? (!descending && sort.key === this.primary && fullTable ? { index: sort.key } : sort.key) : Criteria.row(sort.key));
            const by = (descending ? RethinkDB.desc(key) : key);
            selection = selection.orderBy(by);
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

        return selection;
    }

    all(options, callback) {

        if (!callback) {
            callback = options;
            options = {};
        }

        const selection = this.raw;

        if (options.chunks) {
            return this._chunks(selection, null, 'all', options, callback);
        }

        this._run(this._refine(selection, options, true), 'all', null, callback);
    }

    exist(id, callback) {

        const diag = { id };
        id = Id.normalize(id, false);
        if (id instanceof Error) {
            return this._error('exist', id.message, diag, Hoek.nextTick(callback));
        }

        this._run(this.raw.get(id).ne(null), 'exist', diag, callback);
    }

    distinct(criteria, fields, callback) {

        if (!callback) {
            callback = fields;
            fields = criteria;
            criteria = null;
        }

        fields = [].concat(fields);

        const selection = Criteria.select(criteria, this).pluck(fields).distinct();
        this._run(selection, 'distinct', { criteria, fields }, (err, result) => {

            if (err) {
                return callback(err);
            }

            if (!result) {
                return callback(null, null);
            }

            if (fields.length === 1) {
                return callback(null, result.map((item) => item[fields[0]]));
            }

            return callback(null, result);
        });
    }

    query(criteria, options, callback) {

        if (!callback) {
            callback = options;
            options = {};
        }

        const diag = { criteria, options };
        const selection = Criteria.select(criteria, this);

        if (options.chunks) {
            return this._chunks(selection, diag, 'query', options, callback);
        }

        this._run(this._refine(selection, options), 'query', diag, callback);
    }

    _chunks(selection, diag, action, options, callback) {

        Hoek.assert(options.from === undefined && options.count === undefined, 'Cannot use chunks option with from or count');
        Hoek.assert(callback.each, 'Must use apiece callback with each handler when specifying chunks option');

        const settings = Hoek.clone(options);
        delete settings.chunks;
        settings.count = options.chunks;

        if (!options.sort) {
            settings.sort = { key: this.primary };
        }

        let count = null;
        const step = (err, results) => {

            if (err) {
                return callback(err);
            }

            if (results) {
                results.forEach(step.each);
            }

            if (count !== null &&
                count < options.chunks) {

                return callback();
            }

            count = 0;
            if (settings.from === undefined) {
                settings.from = 0;
            }
            else {
                settings.from += options.chunks;
            }

            this._run(this._refine(selection, settings, action === 'all'), action, diag, step);
        };

        step.each = (item) => {

            ++count;
            return callback.each(item);
        };

        return step();
    }

    single(criteria, callback) {

        const diag = { criteria };
        this._run(Criteria.select(criteria, this), 'single', diag, (err, result) => {

            if (err) {
                return callback(err);
            }

            if (!result) {
                return callback(null, null);
            }

            if (result.length !== 1) {
                return this._error('single', 'Found multiple items', diag, callback);
            }

            return callback(null, result[0]);
        });
    }

    count(criteria, callback) {

        this._run(Criteria.select(criteria, this).count(), 'count', { criteria }, callback);
    }

    insert(items, options, callback) {

        if (!callback) {
            callback = options;
            options = {};
        }

        if (!options.chunks ||
            !Array.isArray(items) ||
            items.length === 1) {

            return this._insert(items, options, callback);
        }

        // Split items into batches

        let result = [];
        const each = (batch, next) => {

            this._insert(batch, options, (err, ids) => {

                if (err) {
                    return next(err);
                }

                result = result.concat(ids);
                return next();
            });
        };

        const batches = [];
        let left = items;
        while (left.length) {
            batches.push(left.slice(0, options.chunks));
            left = left.slice(options.chunks);
        }

        Items.serial(batches, each, (err) => callback(err, result));
    }

    _insert(items, options, callback) {

        const diag = { items, options };

        Id.wrap(this, items, (err, wrapped) => {

            if (err) {
                return this._error('insert', err, diag, callback);
            }

            Unique.reserve(this, wrapped, options.merge === true, (err, postUnique) => {

                if (err) {
                    return this._error('insert', err, diag, callback);
                }

                const opt = {
                    conflict: options.merge ? 'update' : 'error',
                    returnChanges: !!postUnique
                };

                this._run(this.raw.insert(wrapped, opt), 'insert', diag, (err, result) => {

                    if (err) {
                        return callback(err);
                    }

                    const allocated = (err) => {

                        if (err) {
                            return callback(err);
                        }

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
                    };

                    if (!postUnique) {
                        return allocated();
                    }

                    return postUnique(result.changes, allocated);
                });
            });
        });
    }

    update(ids, changes, callback) {

        const diag = { ids, changes };

        if (Array.isArray(ids)) {
            Hoek.assert(ids.length, 'Cannot pass empty array');

            if (!callback) {                                                                    // (updates, callback)
                callback = changes;
                changes = ids;
                ids = changes.map((item) => item.id);
            }
            else {
                if (typeof ids[0] === 'object') {                                               // (updates, options, callback)
                    const options = changes;
                    changes = ids;

                    if (options.chunks &&
                        changes.length !== 1) {

                        // Split items into batches

                        const batches = [];
                        let left = changes;
                        while (left.length) {
                            batches.push(left.slice(0, options.chunks));
                            left = left.slice(options.chunks);
                        }

                        const each = (batch, next) => this._update(batch.map((item) => item.id), batch, diag, next);
                        return Items.serial(batches, each, (err) => callback(err));
                    }

                    ids = changes.map((item) => item.id);
                }
                else {                                                                         // (ids, changes, callback)
                    Hoek.assert(!Array.isArray(changes), 'Changes cannot be an array when ids is an array');
                }
            }
        }
        else {                                                                                  // (id, changes, callback)
            Hoek.assert(changes && typeof changes === 'object', 'Invalid changes object');
        }

        return this._update(ids, changes, diag, callback);
    }

    _update(ids, changes, diag, callback) {

        const batch = Array.isArray(ids);

        ids = Id.normalize(ids, true);
        if (ids instanceof Error) {
            return this._error('update', ids.message, diag, Hoek.nextTick(callback));
        }

        Unique.reserve(this, changes, (batch ? true : ids), (err, postUnique) => {

            if (err) {
                return this._error('update', err, diag, callback);
            }

            const wrapped = Modifier.wrap(changes, this);
            const opts = { returnChanges: !!postUnique };
            const query = (batch ? this.raw.getAll(RethinkDB.args(ids)) : this.raw.get(ids));
            this._run(query.replace(wrapped, opts), 'update', diag, (err, result) => {

                if (err) {
                    return callback(err);
                }

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
        id = Id.normalize(id, false);
        if (id instanceof Error) {
            return this._error('next', id.message, diag, Hoek.nextTick(callback));
        }

        this._run(this.raw.get(id).update(changes, { returnChanges: true }), 'next', diag, (err, result) => {

            if (err) {
                return callback(err);
            }

            if (!result.replaced) {
                return this._error('next', 'No item found to update', diag, callback);
            }

            const inc = result.changes[0].new_val[field];
            return callback(null, inc);
        });
    }

    remove(criteria, callback) {

        const diag = { criteria };

        const isBatch = Array.isArray(criteria);
        const isIds = (isBatch || typeof criteria !== 'object' || criteria.id !== undefined);
        if (isIds) {
            criteria = Id.normalize(criteria, true);
            if (criteria instanceof Error) {
                return this._error('remove', criteria.message, diag, Hoek.nextTick(callback));
            }
        }

        const selection = (!isIds ? this.raw.filter(criteria)
                                  : (isBatch ? this.raw.getAll(RethinkDB.args(criteria))
                                             : this.raw.get(criteria)));

        this._run(selection.delete(), 'remove', diag, (err, result) => {

            if (err) {
                return callback(err);
            }

            if (isIds &&
                !isBatch &&
                !result.deleted) {

                return this._error('remove', 'No item found to remove', diag, callback);
            }

            return callback(null);
        });
    }

    empty(callback) {

        this._run(this.raw.delete(), 'empty', null, (err, result) => {

            return callback(err, result ? result.deleted : 0);
        });
    }

    sync(callback) {

        if (!this._db._connection) {
            return this._error('sync', 'Database disconnected', null, Hoek.nextTick(callback));
        }

        this.raw.sync().run(this._db._connection, (err, result) => {

            if (err) {
                return this._error('sync', err, null, callback);
            }

            return callback(null);
        });
    }

    index(indexes, callback) {

        const names = [];
        const each = (index, next) => {

            if (typeof index === 'string') {
                index = { name: index };
            }

            const { name, source, options } = index;
            names.push(name);

            const args = [name];
            if (source) {
                args.push(Array.isArray(source) ? source.map((row) => RethinkDB.row(row)) : source);
            }

            args.push(options);
            this._run(this.raw.indexCreate.apply(this.raw, args), 'index', null, next);
        };

        Items.parallel([].concat(indexes), each, (err) => {

            if (err) {
                return callback(err);
            }

            this._run(this.raw.indexWait(RethinkDB.args(names)), 'indexWait', null, callback);
        });
    }

    changes(criteria, options, callback) {

        if (!this._db._connection) {
            return this._error('changes', 'Database disconnected', criteria, Hoek.nextTick(callback));
        }

        if (typeof options !== 'object') {
            options = { handler: options };
        }

        Hoek.assert(typeof options.handler === 'function', 'Invalid options.handler handler');

        let request = this.raw;
        if (criteria !== '*') {
            if (typeof criteria !== 'object') {
                request = this.raw.get(criteria);
            }
            else if (Array.isArray(criteria)) {
                request = this.raw.getAll(RethinkDB.args(criteria));
            }
            else {
                request = this.raw.filter(criteria);
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

    _run(request, action, inputs, callback) {

        return this._db._run(request, {}, this.name, action, inputs, callback);
    }

    _error(action, err, inputs, callback, flags) {

        return internals.error(this.name, action, err, inputs, callback, flags);
    }

    static _error(table, action, err, inputs, callback, flags) {

        return internals.error(table, action, err, inputs, callback, flags);
    }
};


internals.error = function (table, action, err, inputs, callback, flags) {

    const message = (typeof err === 'string' ? err : err.message);
    const data = { error: err, table, action, inputs };

    if (err instanceof Error) {
        data.error.stack = err.stack;
        data.error.message = err.message;
    }

    const error = Boom.internal(message, data);

    if (flags) {
        error.flags = flags;
    }

    return callback(error);
};
