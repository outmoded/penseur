'use strict';

const RethinkDb = require('rethinkdb');

const Db = require('./db');
const Table = require('./table');
const Utils = require('./utils');


const internals = {};


module.exports = {
    Db,
    Table,
    utils: Utils,
    r: RethinkDb
};
