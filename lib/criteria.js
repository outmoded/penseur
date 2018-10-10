'use strict';

// Load Modules

const Hoek = require('hoek');
const RethinkDB = require('rethinkdb');

const Geo = require('./geo');


// Declare internals

const internals = {};


exports.select = function (criteria, table, options = {}) {

    if (criteria === null) {
        return table.raw;
    }

    // Optimize secondary index query

    if (typeof criteria === 'function' &&
        criteria.type === 'by') {

        return table.raw.getAll(RethinkDB.args(criteria.value), { index: criteria.flags.index });
    }

    // Construct query

    const base = (table._geo ? Geo.select(criteria, table) : table.raw);
    const filter = internals.compile(criteria, null, options);
    return (filter ? base.filter(filter) : base);
};


internals.compile = function (criteria, relative, options) {

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

        // Simple value

        if (typeof value !== 'function') {
            const condition = (options.match ? row.match(internals.match(value, options.match)) : row.eq(value));
            tests.push(condition.default(null));
            continue;
        }

        // Special rule

        if (value.type === 'near') {
            continue;
        }

        Hoek.assert(['contains', 'is', 'not', 'or', 'unset', 'empty', 'match'].indexOf(value.type) !== -1, `Unknown criteria value type ${value.type}`);

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

                tests.push(RethinkDB[value.flags.condition || 'and'](...conditions));
            }
        }
        else if (value.type === 'match') {

            // Match

            if (!Array.isArray(value.value)) {
                tests.push(row.match(internals.match(value.value, value.flags)));
            }
            else {
                const conditions = [];
                for (let j = 0; j < value.value.length; ++j) {
                    conditions.push(row.match(internals.match(value.value[j], value.flags)));
                }

                tests.push(RethinkDB[value.flags.condition || 'and'](...conditions));
            }
        }
        else if (value.type === 'or') {

            // Or

            const ors = [];
            for (let j = 0; j < value.value.length; ++j) {
                const orValue = value.value[j];
                if (typeof orValue === 'function') {
                    Hoek.assert(['unset', 'is', 'empty'].indexOf(orValue.type) !== -1, `Unknown or criteria value type ${orValue.type}`);
                    if (orValue.type === 'unset') {
                        ors.push(exports.row(path.slice(0, -1)).hasFields(path[path.length - 1]).not());
                    }
                    else if (orValue.type === 'empty') {
                        ors.push(internals.empty(path));
                    }
                    else {
                        ors.push(internals.toComparator(row, orValue));
                    }
                }
                else if (typeof orValue === 'object') {
                    ors.push(internals.compile(orValue, path, options));
                }
                else {
                    ors.push(row.eq(orValue).default(null));
                }
            }

            let test = RethinkDB.or(...ors);
            if (value.flags.not) {
                test = test.not();
            }

            tests.push(test);
        }
        else if (value.type === 'is') {

            // Is

            tests.push(internals.toComparator(row, value));
        }
        else if (value.type === 'empty') {

            // empty

            tests.push(internals.empty(path));
        }
        else {

            // Unset

            tests.push(exports.row(path.slice(0, -1)).hasFields(path[path.length - 1]).not());
        }
    }

    if (!tests.length) {
        return null;
    }

    criteria = (tests.length === 1 ? tests[0] : RethinkDB.and(...tests));

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
    return RethinkDB.and(...ands);
};


internals.match = function (value, options) {

    let prefix = '';
    let suffix = '';

    if (options.insensitive) {
        prefix = '(?i)';
    }

    if (options.start ||
        options.exact) {

        prefix += '^';
    }

    if (options.end ||
        options.exact) {

        suffix = '$';
    }

    return prefix + value + suffix;
};


internals.empty = function (path) {

    const selector = path.reduce((memo, next) => memo(next), exports.row);
    return RethinkDB.branch(
        selector.typeOf().eq('ARRAY'), selector.isEmpty(),
        selector.typeOf().eq('OBJECT'), selector.keys().isEmpty(),
        false);
};
