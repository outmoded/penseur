'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


exports.select = function (criteria, table) {

    return table.filter(internals.compile(criteria));
};


internals.compile = function (criteria) {

    /*
        const criteria = {
            a: {
                b: 1
            },
            c: 2
        };

        const filter = Rethinkdb.and(Rethinkdb.row('a')('b').eq(1), Rethinkdb.row('c').eq(2));
    */

    const edges = [];
    const lines = internals.flatten(criteria, [], edges);
    if (!lines.length) {
        return criteria;
    }

    const tests = [];
    for (let i = 0; i < lines.length; ++i) {
        const path = lines[i].path;
        let row = internals.row(path);
        const value = lines[i].value;
        if (typeof value !== 'function') {
            tests.push(row.eq(value).default(null));
        }
        else {
            Hoek.assert(['contains', 'or', 'unset'].indexOf(value.type) !== -1, `Unknown criteria value type ${value.type}`);

            if (value.type === 'contains') {

                // Contains

                if (value.flags.keys ||
                    !path) {

                    row = row.keys();
                }

                if (!Array.isArray(value.value)) {
                    tests.push(row.contains(value.value));
                }
                else {
                    const conditions = [];
                    for (let j = 0; j < value.value.length; ++j) {
                        conditions.push(row.contains(value.value[j]));
                    }

                    tests.push(RethinkDB[value.flags.condition || 'and'](RethinkDB.args(conditions)));
                }
            }
            else if (value.type === 'or') {

                // Or

                const ors = [];
                for (let j = 0; j < value.value.length; ++j) {
                    const orValue = value.value[j];
                    if (typeof orValue === 'function') {
                        Hoek.assert(orValue.type === 'unset', `Unknown or criteria value type ${orValue.type}`);
                        ors.push(internals.row(path.slice(0, -1)).hasFields(path[path.length - 1]).not());
                    }
                    else {
                        ors.push(row.eq(orValue).default(null));
                    }
                }

                tests.push(RethinkDB.or(RethinkDB.args(ors)));
            }
            else {

                // Unset

                tests.push(internals.row(path.slice(0, -1)).hasFields(path[path.length - 1]).not());
            }
        }
    }

    criteria = (tests.length === 1 ? tests[0] : RethinkDB.and(RethinkDB.args(tests)));

    if (edges.length) {
        let typeCheck = internals.row(edges[0]).typeOf().eq('OBJECT');

        for (let i = 1; i < edges.length; ++i) {
            typeCheck = RethinkDB.and(typeCheck, internals.row(edges[i]).typeOf().eq('OBJECT'));
        }

        criteria = typeCheck.and(criteria);
    }

    return criteria;
};


internals.flatten = function (criteria, path, edges) {

    if (typeof criteria === 'function') {
        return [{ value: criteria }];
    }

    const keys = Object.keys(criteria);
    let lines = [];
    for (let i = 0; i < keys.length; ++i) {
        const key = keys[i];
        const value = criteria[key];
        const location = path.concat(key);

        if (typeof value === 'object') {
            edges.push(location);
            lines = lines.concat(internals.flatten(value, location, edges));
        }
        else {
            lines.push({ path: location, value });
        }
    }

    return lines;
};


internals.row = function (path) {

    if (!path ||
        !path.length) {

        return RethinkDB.row;
    }

    let row = RethinkDB.row(path[0]);
    for (let i = 1; i < path.length; ++i) {
        row = row(path[i]);
    }

    return row;
};
