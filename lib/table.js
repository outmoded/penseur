// Load Modules

var RethinkDB = require('rethinkdb');
var Boom = require('boom');


// Declare internals

var internals = {};


exports = module.exports = internals.Table = function (name, db) {

    this._name = name;
    this._db = db;
    this._table = RethinkDB.db(this._db._name).table(name);
};


internals.Table.prototype.get = function (id, callback) {

    var self = this;

    this._table.get(id).run(this._db._connection, function (err, item) {

        if (err) {
            return self.error('get', err, id, callback);
        }

        return callback(null, item);
    });
};


internals.Table.prototype.query = function (criteria, callback) {

    var self = this;

    this._table.filter(criteria).run(this._db._connection, function (err, cursor) {

        if (err) {
            return self.error('query', err, criteria, callback);
        }

        cursor.toArray(function (err, results) {

            if (err) {
                return self.error('query', err, criteria, callback);
            }

            cursor.close();
            return callback(null, results);
        });
    });
};


internals.Table.prototype.single = function (criteria, callback) {

    var self = this;

    this._table.filter(criteria).run(this._db._connection, function (err, cursor) {

        if (err) {
            return self.error('single', err, criteria, callback);
        }

        cursor.toArray(function (err, results) {

            if (err) {
                return self.error('single', err, criteria, callback);
            }

            cursor.close();

            if (results.length === 0) {
                return callback(null, null);
            }

            if (results.length !== 1) {
                return self.error('single', 'Found multiple items', criteria, callback);
            }

            return callback(null, results[0]);
        });
    });
};


internals.Table.prototype.count = function (criteria, type, callback) {

    var self = this;

    this._table[type === 'fields' ? 'hasFields' : 'filter'](criteria).count().run(this._db._connection, function (err, count) {

        if (err) {
            return self.error('count', err, { criteria: criteria, type: type }, callback);
        }

        return callback(null, count);
    });
};


internals.Table.prototype.insert = function (items, callback) {

    var self = this;

    this._table.insert(items).run(this._db._connection, function (err, result) {

        if (err) {
            return self.error('insert', err, items, callback);
        }

        return callback(null, result.generated_keys ? (items instanceof Array ? result.generated_keys : result.generated_keys[0]) : null);
    });
};


internals.Table.prototype.update = function (id, changes, callback) {

    var self = this;

    this._table.get(id).update(changes).run(this._db._connection, function (err, result) {

        if (err) {
            return self.error('update', err, { id: id, changes: changes }, callback);
        }

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
    this._table.get(id).update(changes, { returnChanges: true }).run(this._db._connection, function (err, result) {

        if (err) {
            return self.error('increment', err, { id: id, field: field, value: value }, callback);
        }

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
    this._table.get(id).update(changes).run(this._db._connection, function (err, result) {

        if (err) {
            return self.error('increment', err, { id: id, field: field, value: value }, callback);
        }

        if (!result.replaced) {
            return self.error('increment', 'No item found to update', { id: id, field: field, value: value }, callback);
        }

        return callback(null);
    });
};


internals.Table.prototype.unset = function (id, fields, callback) {

    var self = this;

    var changes = function (item) {

        return item.without(fields);
    };

    this._table.get(id).replace(changes).run(this._db._connection, function (err, result) {

        if (err) {
            return self.error('unset', err, { id: id, fields: fields }, callback);
        }

        if (!result.replaced &&
            !result.unchanged) {

            return self.error('unset', 'No item found to update', { id: id, fields: fields }, callback);
        }

        return callback(null);
    });
};


internals.Table.prototype.remove = function (criteria, callback) {

    var self = this;

    var isSingle = (typeof criteria === 'string');
    var selection = (isSingle ? this._table.get(criteria)
                              : (Array.isArray(criteria) ? this._table.getAll(RethinkDB.args(criteria))
                                                         : this._table.filter(criteria)));

    selection.delete().run(this._db._connection, function (err, result) {

        if (err) {
            return self.error('remove', err, criteria, callback);
        }

        if (isSingle &&
            !result.deleted) {

            return self.error('remove', 'No item found to remove', criteria, callback);
        }

        return callback(null);
    });
};


internals.Table.prototype.error = function (action, err, inputs, callback) {

    return callback(Boom.internal('Database error', { error: err, table: this._name, action: action, inputs: inputs }));
};
