'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');

const Cursor = require('./cursor');
const Criteria = require('./criteria');
const Geo = require('./geo');
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


exports = module.exports = internals.Table = class {

    constructor(name, db, options) {

        this.name = name;
        this.primary = options.primary || 'id';
        this._db = db;
        this._id = Id.compile(this, options.id);
        this._unique = Unique.compile(this, options.unique);
        this._geo = Geo.index(options);
        this._settings = Hoek.cloneWithShallow(options, ['extended']);      // Used by db.establish()

        this.raw = RethinkDB.db(this._db.name).table(name);
        this._cursors = [];
    }

    async get(ids, options = {}) {

        const track = { table: this.name, action: 'get', inputs: { ids, options } };

        try {
            const batch = Array.isArray(ids);
            ids = Id.normalize(ids, true);
            const query = (batch ? this.raw.getAll(RethinkDB.args(ids)) : this.raw.get(ids));
            const items = await this._db._run(this._refine(query, options), track);
            return this._read(items);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
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

    async all(options = {}) {

        const track = { table: this.name, action: 'all' };

        try {
            const items = await this._db._run(this._refine(this.raw, options, true), track);
            return this._read(items);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async exist(id) {

        const track = { table: this.name, action: 'exist', inputs: { id } };

        try {
            id = Id.normalize(id, false);
            return await this._db._run(this.raw.get(id).ne(null), track);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async distinct(criteria, fields) {

        if (!fields) {
            fields = criteria;
            criteria = null;
        }

        fields = [].concat(fields);

        const track = { table: this.name, action: 'distinct', inputs: { criteria, fields } };

        try {
            const selection = Criteria.select(criteria, this).pluck(fields).distinct();
            const result = await this._db._run(selection, track);
            if (!result) {
                return null;
            }

            if (fields.length === 1) {
                return result.map((item) => item[fields[0]]);
            }

            return result;
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async query(criteria, options = {}) {

        const track = { table: this.name, action: 'query', inputs: { criteria, options } };

        try {
            const selection = Criteria.select(criteria, this, options);
            const items = await this._db._run(this._refine(selection, options), track);
            return this._read(items);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async single(criteria) {

        const track = { table: this.name, action: 'single', inputs: { criteria } };

        try {
            const result = await this._db._run(Criteria.select(criteria, this), track);
            if (!result) {
                return null;
            }

            if (result.length !== 1) {
                throw new Boom('Found multiple items');
            }

            return this._read(result[0]);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async count(criteria = null) {

        const track = { table: this.name, action: 'count', inputs: { criteria } };

        try {
            return await this._db._run(Criteria.select(criteria, this).count(), track);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
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
            if (options.each) {
                await options.each(i, ids, this.name);
            }
        }

        return result;
    }

    async _insert(items, options) {

        const track = { table: this.name, action: 'insert', inputs: { items, options } };

        try {
            const wrapped = await Id.wrap(this, this._write(items));
            const postUnique = await Unique.reserve(this, wrapped, options.merge === true);

            const opt = {
                conflict: options.merge ? 'update' : 'error',
                returnChanges: !!postUnique
            };

            const result = await this._db._run(this.raw.insert(wrapped, opt), track);
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
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async update(ids, changes) {

        const track = { table: this.name, action: 'update', inputs: { ids, changes } };

        // (id, changes)

        if (!Array.isArray(ids)) {
            Hoek.assert(changes && typeof changes === 'object', 'Invalid changes object');
            return this._update(ids, changes, track);
        }

        if (!ids.length) {
            return;
        }

        // (updates)

        if (!changes) {
            const updates = ids;
            return this._update(updates.map((item) => item.id), updates, track);
        }

        // ([ids], changes)

        if (typeof ids[0] !== 'object') {
            Hoek.assert(!Array.isArray(changes), 'Changes cannot be an array when ids is an array');
            return this._update(ids, changes, track);
        }

        // ([updates], options)

        const options = changes;
        changes = ids;

        if (!options.chunks ||
            changes.length <= options.chunks) {

            ids = changes.map((item) => item.id);
            return this._update(ids, changes, track);
        }

        // Split items into batches

        const batches = [];
        let left = changes;
        while (left.length) {
            batches.push(left.slice(0, options.chunks));
            left = left.slice(options.chunks);
        }

        for (let i = 0; i < batches.length; ++i) {
            const batch = batches[i];
            const bIds = batch.map((item) => item.id);
            await this._update(bIds, batch, track);

            if (options.each) {
                await options.each(i, bIds, this.name);
            }
        }
    }

    static items(ids, changes) {

        // (id, changes)
        // ([ids], changes)

        if (!Array.isArray(ids) ||
            (ids[0] && typeof ids[0] !== 'object')) {

            return [changes];
        }

        // ([updates])
        // ([updates], options)

        return ids;
    }

    async _update(ids, changes, track) {

        try {
            const batch = Array.isArray(ids);
            ids = Id.normalize(ids, true);

            const postUnique = await Unique.reserve(this, changes, (batch ? true : ids));
            const wrapped = Modifier.wrap(changes, this);
            const opts = { returnChanges: !!postUnique };
            const query = (batch ? this.raw.getAll(RethinkDB.args(ids)) : this.raw.get(ids));
            const result = await this._db._run(query.replace(wrapped, opts), track);

            if (postUnique) {
                return postUnique(result.changes);
            }
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async next(id, field, value) {

        const track = { table: this.name, action: 'next', inputs: { id, field, value } };

        try {
            const changes = {};
            changes[field] = RethinkDB.row(field).add(value);

            id = Id.normalize(id, false);
            const result = await this._db._run(this.raw.get(id).update(changes, { returnChanges: true }), track);
            if (!result.replaced) {
                throw new Boom('No item found to update');
            }

            const inc = result.changes[0].new_val[field];
            return inc;
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async remove(criteria) {

        const track = { table: this.name, action: 'remove', inputs: { criteria } };

        try {
            const isBatch = Array.isArray(criteria);
            const isIds = (isBatch || typeof criteria !== 'object' || criteria.id !== undefined);
            if (isIds) {
                criteria = Id.normalize(criteria, true);
            }

            const selection = (!isIds ? this.raw.filter(criteria)
                : (isBatch ? this.raw.getAll(RethinkDB.args(criteria))
                    : this.raw.get(criteria)));

            const result = await this._db._run(selection.delete(), track);
            if (isIds &&
                !isBatch &&
                !result.deleted) {

                throw new Boom('No item found to remove');
            }
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async empty() {

        const track = { table: this.name, action: 'empty' };

        try {
            const result = await this._db._run(this.raw.delete(), track);
            return result.deleted;
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async sync() {

        const track = { table: this.name, action: 'sync' };

        try {
            if (!this._db._connection) {
                throw new Boom('Database disconnected');
            }

            await this.raw.sync().run(this._db._connection);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async index(indexes) {

        const track = { table: this.name, action: 'index' };

        try {
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
                pending.push(this._db._run(this.raw.indexCreate.apply(this.raw, args), track));
            }

            await Promise.all(pending);
            return this._db._run(this.raw.indexWait(RethinkDB.args(names)), track);
        }
        catch (err) {
            throw internals.Table.error(err, track);
        }
    }

    async changes(criteria, options) {

        const track = { table: this.name, action: 'changes', inputs: { criteria } };

        if (!this._db._connection) {
            throw internals.Table.error(new Boom('Database disconnected'), track);
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
            throw internals.Table.error(err, track);
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
                before: this._read(item.old_val) || null,
                after: this._read(item.new_val) || null
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

            return options.handler(internals.Table.error(err, track, { disconnected, willReconnect }));
        });

        return cursor;
    }

    _read(items) {

        if (this._geo) {
            return Geo.read(items, this);
        }

        return items;
    }

    _write(items) {

        if (this._geo) {
            return Geo.write(items, this);
        }

        return items;
    }

    static error(err, { table, action, inputs }, flags) {

        const error = Boom.boomify(err);
        error.data = { table, action, inputs };
        error.flags = flags;
        return error;
    }
};
