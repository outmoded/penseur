'use strict';

// Load Modules


// Declare internals

const internals = {};


exports = module.exports = class {

    constructor(cursor, table, feedId) {

        this._cursor = cursor;
        this._table = table;
        this._feedId = feedId;

        this._table._cursors.push(cursor);
    }

    close(_cleanup) {

        if (_cleanup !== false) {                               // Defaults to true
            delete this._table._db._feeds[this._feedId];
        }

        this._table._cursors = this._table._cursors.filter((cursor) => cursor !== this._cursor);
        this._cursor.close();
    }
};
