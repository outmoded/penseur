'use strict';

// Load modules

const Db = require('./db');
const Table = require('./table');
const Utils = require('./utils');
const RethinkDb = require('rethinkdb');


// Declare internals

const internals = {};


module.exports = {
    Db,
    Table,
    utils: Utils,
    r: RethinkDb
};
