'use strict';

// Load Modules


// Declare internals

const internals = {};


exports = module.exports = class {

    constructor(cursor) {

        this._cursor = cursor;
    }

    close() {

        this._cursor.close();
    }
};
