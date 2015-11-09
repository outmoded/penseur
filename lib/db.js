'use strict';

// Load Modules

const Boom = require('boom');
const Hoek = require('hoek');
const Items = require('items');
const RethinkDB = require('rethinkdb');
const Criteria = require('./criteria');
const Table = require('./table');


// Declare internals

const internals = {};


exports = module.exports = class {

    constructor(name, options) {

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
    }

    connect(callback) {

        RethinkDB.connect(this._settings, (err, connection) => {

            if (err) {
                return callback(err);
            }

            this._connection = connection;
            return callback(null);
        });
    }

    close(next) {

        next = next || Hoek.ignore;

        if (!this._connection) {
            return next();
        }

        // Close change stream cursors

        const tables = Object.keys(this.tables);
        for (let i = 0; i < tables.length; ++i) {
            const table = this.tables[tables[i]];
            for (let j = 0; j < table._cursors.length; ++j) {
                table._cursors[j].close();
            }

            table._cursors = [];
        }

        // Close connection

        this._connection.close((err) => {     // Explicit callback to avoid generating a promise

            return next(err);
        });
    }

    table(tables) {

        const Proto = this._settings.extended || Table;

        tables = [].concat(tables);

        for (let i = 0; i < tables.length; ++i) {
            const table = tables[i];
            if (this.tables[table]) {
                return;
            }

            const record = new Proto(table, this);

            // Decorate object with tables

            this.tables[table] = record;
            if (!this[table] &&
                table[0] !== '_') {                 // Do not override prototype or private members

                this[table] = record;
            }
        }
    }

    establish(tables, callback) {

        // Connect if not connected already

        if (!this._connection) {
            this.connect((err) => {

                if (err) {
                    return callback(err);
                }

                return this.establish(tables, callback);
            });

            return;
        }

        RethinkDB.dbList().run(this._connection, (err, names) => {

            if (err) {
                return callback(err);
            }

            if (names.indexOf(this._name) === -1) {

                // Create new database

                RethinkDB.dbCreate(this._name).run(this._connection, (err, created) => {

                    if (err) {
                        return callback(err);
                    }

                    return this._createTable(tables, callback);
                });
            }
            else {

                // Reuse existing

                return this._createTable(tables, callback);
            }
        });
    }

    _createTable(tables, callback) {

        RethinkDB.db(this._name).tableList().run(this._connection, (err, names) => {

            if (err) {
                return callback(err);
            }

            const each = (table, next) => {

                if (names.indexOf(table) === -1) {

                    // Create new table

                    RethinkDB.db(this._name).tableCreate(table).run(this._connection, (err, result) => {

                        this.table(table);
                        return next(err);
                    });
                }
                else {

                    // Empty existing table

                    this.table(table);
                    return this.tables[table].empty(next);
                }
            };

            Items.serial(tables, each, callback);
        });
    }

    fields(criteria) {

        return new Criteria(criteria, 'fields');
    }
};


internals.disable = function (table, method) {

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    this.tables[table][method] = internals.disabled;
};


internals.disabled = function () {

    const callback = arguments[arguments.length - 1];
    return callback(Boom.internal('Simulated database error'));
};


internals.enable = function (table, method) {

    Hoek.assert(this.tables[table], 'Unknown table:', table);
    Hoek.assert(this.tables[table][method], 'Unknown method:', method);

    this.tables[table][method] = Table.prototype[method];
};
