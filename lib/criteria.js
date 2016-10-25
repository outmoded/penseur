'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


exports.select = function (criteria, table) {

    // Optimize secondary index query

    if (typeof criteria === 'object') {
        const keys = Object.keys(criteria);
        if (keys.length === 1) {
            const value = criteria[keys];
            if (table._secondary.indexOf(keys[0]) !== -1 &&
                typeof value !== 'object') {

                if (typeof value !== 'function') {
                    return table._table.getAll(value, { index: keys[0] });
                }

                if (value.type === 'or') {
                    const ids = [];
                    for (let i = 0; i < value.value.length; ++i) {
                        const id = value.value[i];
                        if (typeof id === 'function' ||
                            typeof id === 'object') {

                            break;
                        }

                        ids.push(id);
                    }

                    if (ids.length === value.value.length) {
                        return table._table.getAll(RethinkDB.args(ids), { index: keys[0] });
                    }
                }
            }
        }
    }

    // Construct query

    return table._table.filter(internals.compile(criteria));
};


internals.compile = function (criteria, relative) {

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
    const lines = internals.flatten(criteria, relative || [], edges);
    if (!lines.length) {
        return criteria;
    }

    const tests = [];
    for (let i = 0; i < lines.length; ++i) {
        const path = lines[i].path;
        let row = exports.row(path);
        const value = lines[i].value;

        if (typeof value === 'function') {

            // Special rule

            Hoek.assert(['contains', 'is', 'not', 'or', 'unset'].indexOf(value.type) !== -1, `Unknown criteria value type ${value.type}`);

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

                    tests.push(RethinkDB[value.flags.condition || 'and'].apply(RethinkDB, conditions));
                }
            }
            else if (value.type === 'or') {

                // Or

                const ors = [];
                for (let j = 0; j < value.value.length; ++j) {
                    const orValue = value.value[j];
                    if (typeof orValue === 'function') {
                        Hoek.assert(['unset', 'is'].indexOf(orValue.type) !== -1, `Unknown or criteria value type ${orValue.type}`);
                        if (orValue.type === 'unset') {
                            ors.push(exports.row(path.slice(0, -1)).hasFields(path[path.length - 1]).not());
                        }
                        else {
                            ors.push(internals.toComparator(row, orValue));
                        }
                    }
                    else if (typeof orValue === 'object') {
                        ors.push(internals.compile(orValue, path));
                    }
                    else {
                        ors.push(row.eq(orValue).default(null));
                    }
                }

                let test = RethinkDB.or.apply(RethinkDB, ors);
                if (value.flags.not) {
                    test = test.not();
                }

                tests.push(test);
            }
            else if (value.type === 'is') {

                // Is

                tests.push(internals.toComparator(row, value));
            }
            else {

                // Unset

                tests.push(exports.row(path.slice(0, -1)).hasFields(path[path.length - 1]).not());
            }
        }
        else {

            // Simple value

            tests.push(row.eq(value).default(null));
        }
    }

    criteria = (tests.length === 1 ? tests[0] : RethinkDB.and.apply(RethinkDB, tests));

    if (edges.length) {
        let typeCheck = exports.row(edges[0]).typeOf().eq('OBJECT');

        for (let i = 1; i < edges.length; ++i) {
            typeCheck = RethinkDB.and(typeCheck, exports.row(edges[i]).typeOf().eq('OBJECT'));
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


exports.row = function (path) {

    if (!path) {
        return RethinkDB.row;
    }

    path = [].concat(path);
    if (!path.length) {
        return RethinkDB.row;
    }

    let row = RethinkDB.row(path[0]);
    for (let i = 1; i < path.length; ++i) {
        row = row(path[i]);
    }

    return row;
};


internals.toComparator = function (row, value) {

    const primary = row[value.flags.comparator](value.value).default(null);

    if (!value.flags.and) {
        return primary;
    }

    const ands = [primary];
    value.flags.and.forEach((condition) => ands.push(row[condition.comparator](condition.value).default(null)));
    return RethinkDB.and.apply(RethinkDB, ands);
};
