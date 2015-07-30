// Load Modules

var RethinkDB = require('rethinkdb');
var Hoek = require('hoek');
var Items = require('items');
var Table = require('./table');


// Declare internals

var internals = {};


exports = module.exports = internals.Db = function (name, options) {

    options = options || {};
    Hoek.assert(!options.db, 'Cannot set db option');

    this._settings = Hoek.clone(options);
    this._name = name;

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


internals.Db.prototype.table = function (tables) {

    tables = [].concat(tables);

    for (var i = 0, il = tables.length; i < il; ++i) {
        var table = tables[i];
        Hoek.assert(!this.tables[table], 'Table already exists:', table);

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

    // Drop the database and recreate

    RethinkDB.dbDrop(this._name).run(this._connection, function (err, dropped) {

        // Ignore errors

        RethinkDB.dbCreate(self._name).run(self._connection, function (err, created) {

            if (err) {
                return callback(err);
            }

            var each = function (table, next) {

                RethinkDB.db(self._name).tableCreate(table).run(self._connection, function (err, result) {

                    self.table(table);
                    return next(err);
                });
            };

            Items.serial(tables, each, callback);
        });
    });
};
