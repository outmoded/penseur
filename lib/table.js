'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');
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

    get(ids, options = {}) {

        const diag = { ids, options };

        const batch = Array.isArray(ids);
        try {
            ids = Id.normalize(ids, true);
        }
        catch (err) {
            return Promise.reject(this._error('get', err.message, diag));
        }

        const query = (batch ? this.raw.getAll(RethinkDB.args(ids)) : this.raw.get(ids));
        return this._run(this._refine(query, options), 'get', diag);
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

    all(options = {}) {

        return this._run(this._refine(this.raw, options, true), 'all');
    }

    exist(id) {

        const diag = { id };
        try {
            id = Id.normalize(id, false);
        }
        catch (err) {
            return Promise.reject(this._error('exist', err.message, diag));
        }

        return this._run(this.raw.get(id).ne(null), 'exist', diag);
    }

    async distinct(criteria, fields) {

        if (!fields) {
            fields = criteria;
            criteria = null;
        }

        fields = [].concat(fields);

        const selection = Criteria.select(criteria, this).pluck(fields).distinct();
        const result = await this._run(selection, 'distinct', { criteria, fields });
        if (!result) {
            return null;
        }

        if (fields.length === 1) {
            return result.map((item) => item[fields[0]]);
        }

        return result;
    }

    query(criteria, options = {}) {

        const diag = { criteria, options };
        const selection = Criteria.select(criteria, this);
        return this._run(this._refine(selection, options), 'query', diag);
    }

    async single(criteria) {

        const diag = { criteria };
        const result = await this._run(Criteria.select(criteria, this), 'single', diag);
        if (!result) {
            return null;
        }

        if (result.length !== 1) {
            return Promise.reject(this._error('single', 'Found multiple items', diag));
        }

        return result[0];
    }

    count(criteria) {

        return this._run(Criteria.select(criteria, this).count(), 'count', { criteria });
    }

    async insert(items, options = {}) {

        if (!options.chunks ||
            !Array.isArray(items) ||
            items.length === 1) {

            return this._insert(items, options);
        }

        // Split items into batches

        const batches = [];
        let left = items;
        while (left.length) {
            batches.push(left.slice(0, options.chunks));
            left = left.slice(options.chunks);
        }

        let result = [];

        for (let i = 0; i < batches.length; ++i) {
            const batch = batches[i];

            const ids = await this._insert(batch, options);
            result = result.concat(ids);
        }

        return result;
    }

    async _insert(items, options) {

        const diag = { items, options };

        let wrapped;
        let postUnique;

        try {
            wrapped = await Id.wrap(this, items);
            postUnique = await Unique.reserve(this, wrapped, options.merge === true);
        }
        catch (err) {
            throw this._error('insert', err, diag);
        }

        const opt = {
            conflict: options.merge ? 'update' : 'error',
            returnChanges: !!postUnique
        };

        const result = await this._run(this.raw.insert(wrapped, opt), 'insert', diag);
        if (postUnique) {
            await postUnique(result.changes);
        }

        // Single item

        if (!Array.isArray(wrapped)) {
            return (wrapped[this.primary] !== undefined ? wrapped[this.primary] : result.generated_keys[0]);
        }

        // Items array

        const generated = result.generated_keys || [];
        if (generated.length === wrapped.length) {
            return result.generated_keys;
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

        return ids;

    }

    async update(ids, changes) {

        const diag = { ids, changes };

        if (Array.isArray(ids)) {
            Hoek.assert(ids.length, 'Cannot pass empty array');

            if (!changes) {                                                                                     // (updates)
                changes = ids;
                ids = changes.map((item) => item.id);
            }
            else {
                if (typeof ids[0] === 'object') {                                                               // (updates, options)
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

                        for (let i = 0; i < batches.length; ++i) {
                            const batch = batches[i];
                            await this._update(batch.map((item) => item.id), batch, diag);
                        }

                        return;
                    }

                    ids = changes.map((item) => item.id);
                }
                else {                                                                                          // (ids, changes)
                    Hoek.assert(!Array.isArray(changes), 'Changes cannot be an array when ids is an array');
                }
            }
        }
        else {                                                                                                  // (id, changes)
            Hoek.assert(changes && typeof changes === 'object', 'Invalid changes object');
        }

        return this._update(ids, changes, diag);
    }

    async _update(ids, changes, diag) {

        const batch = Array.isArray(ids);

        try {
            ids = Id.normalize(ids, true);
        }
        catch (err) {
            throw this._error('update', err.message, diag);
        }

        let postUnique;
        try {
            postUnique = await Unique.reserve(this, changes, (batch ? true : ids));
        }
        catch (err) {
            throw this._error('update', err, diag);
        }

        const wrapped = Modifier.wrap(changes, this);
        const opts = { returnChanges: !!postUnique };
        const query = (batch ? this.raw.getAll(RethinkDB.args(ids)) : this.raw.get(ids));
        const result = await this._run(query.replace(wrapped, opts), 'update', diag);

        if (postUnique) {
            return postUnique(result.changes);
        }
    }

    async next(id, field, value) {

        const changes = {};
        changes[field] = RethinkDB.row(field).add(value);

        const diag = { id, field, value };
        try {
            id = Id.normalize(id, false);
        }
        catch (err) {
            throw this._error('next', err.message, diag);
        }

        const result = await this._run(this.raw.get(id).update(changes, { returnChanges: true }), 'next', diag);
        if (!result.replaced) {
            throw this._error('next', 'No item found to update', diag);
        }

        const inc = result.changes[0].new_val[field];
        return inc;
    }

    async remove(criteria) {

        const diag = { criteria };

        const isBatch = Array.isArray(criteria);
        const isIds = (isBatch || typeof criteria !== 'object' || criteria.id !== undefined);
        if (isIds) {
            try {
                criteria = Id.normalize(criteria, true);
            }
            catch (err) {
                throw this._error('remove', err.message, diag);
            }
        }

        const selection = (!isIds ? this.raw.filter(criteria)
            : (isBatch ? this.raw.getAll(RethinkDB.args(criteria))
                : this.raw.get(criteria)));

        const result = await this._run(selection.delete(), 'remove', diag);
        if (isIds &&
            !isBatch &&
            !result.deleted) {

            throw this._error('remove', 'No item found to remove', diag);
        }
    }

    async empty() {

        const result = await this._run(this.raw.delete(), 'empty');
        return result.deleted;
    }

    async sync() {

        if (!this._db._connection) {
            throw this._error('sync', 'Database disconnected');
        }

        try {
            await this.raw.sync().run(this._db._connection);
        }
        catch (err) {
            throw this._error('sync', err);
        }
    }

    async index(indexes) {

        const pending = [];
        const names = [];
        indexes = [].concat(indexes);
        for (let i = 0; i < indexes.length; ++i) {
            let index = indexes[i];
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
            pending.push(this._run(this.raw.indexCreate.apply(this.raw, args), 'index'));
        }

        await Promise.all(pending);
        return this._run(this.raw.indexWait(RethinkDB.args(names)), 'indexWait');
    }

    async changes(criteria, options) {

        if (!this._db._connection) {
            throw this._error('changes', 'Database disconnected', criteria);
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

        let dbCursor;
        try {
            dbCursor = await request.changes().run(this._db._connection, settings);
        }
        catch (err) {
            throw this._error('changes', err, criteria);
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

            return options.handler(this._error('changes', err, criteria, { disconnected, willReconnect }));
        });

        return cursor;
    }

    _run(request, action, inputs) {

        return this._db._run(request, {}, this.name, action, inputs);
    }

    _error(action, err, inputs, flags) {

        return internals.error(this.name, action, err, inputs, flags);
    }

    static _error(table, action, err, inputs, flags) {

        return internals.error(table, action, err, inputs, flags);
    }
};


internals.error = function (table, action, err, inputs, flags) {

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

    return error;
};
