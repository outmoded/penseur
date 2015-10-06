// Load Modules

var Boom = require('boom');
var RethinkDB = require('rethinkdb');


// Declare internals

var internals = {};


exports = module.exports = internals.Table = function (name, db) {

    this._name = name;
    this._db = db;
    this._table = RethinkDB.db(this._db._name).table(name);
    this._cursors = [];
};


internals.Table.prototype.get = function (id, callback) {

    if (Array.isArray(id)) {
        this._run(this._table.getAll(RethinkDB.args(id)), 'get', id, callback);
        return;
    }

    this._run(this._table.get(id), 'get', id, callback);
};


internals.Table.prototype.query = function (criteria, callback) {

    this._run(internals.wrap(criteria).select(this._table), 'query', criteria, callback);
};


internals.Table.prototype.single = function (criteria, callback) {

    var self = this;

    this._run(internals.wrap(criteria).select(this._table), 'single', criteria, callback, function (ignore, result) {

        if (!result) {
            return callback(null, null);
        }

        if (result.length !== 1) {
            return self.error('single', 'Found multiple items', criteria, callback);
        }

        return callback(null, result[0]);
    });
};


internals.Table.prototype.count = function (criteria, callback) {

    this._run(internals.wrap(criteria).select(this._table).count(), 'count', { criteria: criteria }, callback);
};


internals.Table.prototype.insert = function (items, callback) {

    this._run(this._table.insert(items), 'insert', items, callback, function (ignore, result) {

        // Single item

        if (!Array.isArray(items)) {
            return callback(null, items.id !== undefined ? items.id : result.generated_keys[0]);
        }

        // Items array

        var generated = result.generated_keys || [];
        if (generated.length === items.length) {
            return callback(null, result.generated_keys);
        }

        // Mixed array

        var ids = [];
        for (var g = 0, i = 0, il = items.length; i < il; ++i) {
            if (items[i].id !== undefined) {
                ids.push(items[i].id);
            }
            else {
                ids.push(result.generated_keys[g++]);
            }
        }

        return callback(null, ids);
    });
};


internals.Table.prototype.update = function (id, changes, callback) {

    var self = this;

    this._run(this._table.get(id).update(changes), 'update', { id: id, changes: changes }, callback, function (ignore, result) {

        if (!result.replaced &&
            !result.unchanged) {

            return self.error('update', 'No item found to update', { id: id, changes: changes }, callback);
        }

        return callback(null);
    });
};


internals.Table.prototype.increment = function (id, field, value, callback) {

    var self = this;

    var changes = {};
    changes[field] = RethinkDB.row(field).add(value);
    this._run(this._table.get(id).update(changes, { returnChanges: true }), 'increment', { id: id, field: field, value: value }, callback, function (ignore, result) {

        if (!result.replaced) {
            return self.error('increment', 'No item found to update', { id: id, field: field, value: value }, callback);
        }

        var inc = result.changes[0].new_val[field];
        return callback(null, inc);
    });
};


internals.Table.prototype.append = function (id, field, value, callback) {

    var self = this;

    var changes = {};
    changes[field] = RethinkDB.row(field).append(value);
    this._run(this._table.get(id).update(changes), 'append', { id: id, field: field, value: value }, callback, function (ignore, result) {

        if (!result.replaced) {
            return self.error('append', 'No item found to update', { id: id, field: field, value: value }, callback);
        }

        return callback(null);
    });
};


internals.Table.prototype.unset = function (id, fields, callback) {

    var self = this;

    var changes = function (item) {

        return item.without(fields);
    };

    this._run(this._table.get(id).replace(changes), 'unset', { id: id, fields: fields }, callback, function (ignore, result) {

        if (!result.replaced &&
            !result.unchanged) {

            return self.error('unset', 'No item found to update', { id: id, fields: fields }, callback);
        }

        return callback(null);
    });
};


internals.Table.prototype.remove = function (criteria, callback) {

    var self = this;

    var isSingle = (typeof criteria !== 'object');
    var selection = (isSingle ? this._table.get(criteria)
                              : (Array.isArray(criteria) ? this._table.getAll(RethinkDB.args(criteria))
                                                         : this._table.filter(criteria)));

    this._run(selection.delete(), 'remove', criteria, callback, function (ignore, result) {

        if (isSingle &&
            !result.deleted) {

            return self.error('remove', 'No item found to remove', criteria, callback);
        }

        return callback(null);
    });
};


internals.Table.prototype.empty = function (callback) {

    this._run(this._table.delete(), 'empty', null, callback);
};


internals.Table.prototype._run = function (request, action, inputs, callback, next) {

    var self = this;

    next = next || callback;                                        // next() must never return an error

    request.run(this._db._connection, function (err, result) {

        if (err) {
            return self.error(action, err, inputs, callback);
        }

        // Single item

        if (!result ||
            typeof result.toArray !== 'function') {

            return next(null, result);
        }

        // Cursor

        var cursor = result;
        cursor.toArray(function (err, results) {

            if (err) {
                return self.error(action, err, inputs, callback);
            }

            cursor.close();
            return next(null, results.length ? results : null);
        });
    });
};


internals.Table.prototype.error = function (action, err, inputs, callback) {

    return callback(Boom.internal('Database error', { error: err, table: this._name, action: action, inputs: inputs }));
};


internals.Table.prototype.changes = function (criteria, each, callback) {

    var self = this;

    callback = callback || each;

    var request = this._table;
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

    request.changes().run(this._db._connection, { includeStates: true }, function (err, cursor) {

        if (err) {
            return self.error('changes', err, criteria, callback);
        }

        self._cursors.push(cursor);

        var isReady = false;
        cursor.each(function (err, item) {

            if (err) {
                return self.error('changes', err, criteria, each);
            }

            if (item.state) {
                isReady = (item.state === 'ready');
                return;
            }

            if (isReady) {
                return each(null, { before: item.old_val || null, after: item.new_val || null });
            }
        });

        return callback(null, new internals.Cursor(cursor));
    });
};


internals.Table.prototype.sync = function (callback) {

    var self = this;

    this._table.sync().run(this._db._connection, function (err, result) {

        if (err) {
            return self.error('sync', err, null, callback);
        }

        return callback();
    });
};


internals.Cursor = function (cursor) {

    this._cursor = cursor;
};


internals.Cursor.prototype.close = function () {

    this._cursor.close();
};


exports.Criteria = internals.Criteria = function (criteria, type) {

    this.criteria = criteria;
    this.type = type || 'filter';
};


internals.Criteria.prototype.select = function (table) {

    return table[this.type === 'fields' ? 'hasFields' : 'filter'](this.criteria);
};


internals.wrap = function (criteria) {

    if (criteria instanceof internals.Criteria) {
        return criteria;
    }

    return new internals.Criteria(criteria);
};
