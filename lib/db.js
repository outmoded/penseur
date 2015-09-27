// Load Modules

var Boom = require('boom');
var Hoek = require('hoek');
var Items = require('items');
var RethinkDB = require('rethinkdb');
var Table = require('./table');


// Declare internals

var internals = {};


exports = module.exports = internals.Db = function (name, options) {

    options = options || {};
    Hoek.assert(!options.db, 'Cannot set db option');

    this._settings = Hoek.clone(options);
    this._name = name;
    this._connection = null;

    if (this._settings.test) {
        this.disable = internals.disable;
        this.enable = internals.enable;
    }

    delete this._settings.test;                 // Always delete in case value is falsy

    this.tables = {};
};


internals.Db.prototype.connect = function (callback) {

    var self = this;

    RethinkDB.connect(this._settings, function (err, connection) {

        if (err) {
            return callback(err);
        }

        self._connection = connection;
        return callback(null);
    });
};


internals.Db.prototype.close = function (next) {

    next = next || Hoek.ignore;

    if (!this._connection) {
        return next();
    }

    // Close change stream cursors

    var tables = Object.keys(this.tables);
    for (var i = 0, il = tables.length; i < il; ++i) {
        var table = this.tables[tables[i]];
        for (var c = 0, cl = table._cursors.length; c < cl; ++c) {
            table._cursors[c].close();
        }

        table._cursors = [];
    }

    // Close connection

    this._connection.close(function (err) {     // Explicit callback to avoid generating a promise

        return next(err);
    });
};


internals.Db.prototype.table = function (tables) {

    tables = [].concat(tables);

    for (var i = 0, il = tables.length; i < il; ++i) {
        var table = tables[i];
        if (this.tables[table]) {
            return;
        }

        var record = new Table(table, this);

        // Decorate object with tables

        this.tables[table] = record;
        if (!this[table] &&
            table[0] !== '_') {                 // Do not override prototype or private members

            this[table] = record;
        }
    }
};


internals.Db.prototype.establish = function (tables, callback) {

    var self = this;

    // Connect if not connected already

    if (!this._connection) {
        this.connect(function (err) {

            if (err) {
                return callback(err);
            }

            return self.establish(tables, callback);
        });

        return;
    }

    RethinkDB.dbList().run(this._connection, function (err, names) {

        if (err) {
            return callback(err);
        }

        if (names.indexOf(self._name) === -1) {

            // Create new database

            RethinkDB.dbCreate(self._name).run(self._connection, function (err, created) {

                if (err) {
                    return callback(err);
                }

                return self._createTable(tables, callback);
            });
        }
        else {

            // Reuse existing

            return self._createTable(tables, callback);
        }
    });
};


internals.Db.prototype._createTable = function (tables, callback) {

    var self = this;

    RethinkDB.db(this._name).tableList().run(this._connection, function (err, names) {

        if (err) {
            return callback(err);
        }

        var each = function (table, next) {

            if (names.indexOf(table) === -1) {

                // Create new table

                RethinkDB.db(self._name).tableCreate(table).run(self._connection, function (err, result) {

                    self.table(table);
                    return next(err);
                });
            }
            else {

                // Empty existing table

                self.table(table);
                return self.tables[table].empty(next);
            }
        };

        Items.serial(tables, each, callback);
    });
};


internals.disable = function (table, method) {

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    this.tables[table][method] = internals.disabled;
};


internals.disabled = function () {

    var callback = arguments[arguments.length - 1];
    return callback(Boom.internal('Simulated database error'));
};


internals.enable = function (table, method) {

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    this.tables[table][method] = Table.prototype[method];
};


internals.Db.prototype.fields = function (criteria) {

    return new Table.Criteria(criteria, 'fields');
};
