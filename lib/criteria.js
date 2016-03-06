'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


exports = module.exports = internals.Criteria = class {

    constructor(criteria, type) {

        this.criteria = criteria;
        this.type = type || 'filter';

        if (this.type === 'filter') {
            this._compile();
        }
    }

    select(table) {

        return table[this.type === 'fields' ? 'hasFields' : 'filter'](this.criteria);
    }

    static wrap(criteria) {

        if (criteria instanceof internals.Criteria) {
            return criteria;
        }

        return new internals.Criteria(criteria);
    }

    static rule(type, values, options) {

        const rule = function () { };                       // Return function because 1. typeof fastest 2. illegal database type
        rule.type = type;
        rule.values = values;
        rule.flags = Hoek.clone(options || {});

        return rule;
    }

    _compile() {

        /*
            const criteria = {
                a: {
                    b: 1
                },
                c: 2
            };

            const filter = Rethinkdb.and(Rethinkdb.row('a')('b').eq(1), Rethinkdb.row('c').eq(2));
        */

        const lines = internals.flatten(this.criteria, []);
        if (!lines.length) {
            return;
        }

        const tests = [];
        for (let i = 0; i < lines.length; ++i) {
            let row = internals.row(lines[i].path);
            const value = lines[i].value;
            if (typeof value !== 'function') {
                tests.push(row.eq(value));
            }
            else {
                if (value.type === 'contains') {

                    // Contains

                    if (value.flags.keys) {
                        row = row.keys();
                    }

                    if (!Array.isArray(value.values)) {
                        tests.push(row.contains(value.values));
                    }
                    else {
                        const conditions = [];
                        for (let j = 0; j < value.values.length; ++j) {
                            conditions.push(row.contains(value.values[j]));
                        }

                        tests.push(RethinkDB[value.flags.condition || 'and'](RethinkDB.args(conditions)));
                    }
                }
                else {

                    // Or

                    const ors = [];
                    for (let j = 0; j < value.values.length; ++j) {
                        ors.push(row.eq(value.values[j]));
                    }

                    tests.push(RethinkDB.or(RethinkDB.args(ors)));
                }
            }
        }

        this.criteria = (tests.length === 1 ? tests[0] : RethinkDB.and(RethinkDB.args(tests)));
    }
};


internals.flatten = function (criteria, path) {

    const keys = Object.keys(criteria);
    let lines = [];
    for (let i = 0; i < keys.length; ++i) {
        const key = keys[i];
        const value = criteria[key];
        const location = path.concat(key);

        if (typeof value === 'object') {
            lines = lines.concat(internals.flatten(value, location));
        }
        else {
            lines.push({ path: location, value });
        }
    }

    return lines;
};


internals.row = function (path) {

    let row = RethinkDB.row(path[0]);
    for (let i = 1; i < path.length; ++i) {
        row = row(path[i]);
    }

    return row;
};
