'use strict';

// Load Modules

const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


exports.index = function (options) {

    if (!options.secondary ||
        !options.geo) {

        return null;
    }

    const indexes = [];
    for (let i = 0; i < options.secondary.length; ++i) {
        const index = options.secondary[i];
        if (index.options &&
            index.options.geo &&
            typeof index.source !== 'function') {

            indexes.push({ name: index.name, source: index.source || [index.name] });
        }
    }

    return indexes;
};


exports.read = function (items, table) {

    internals.scan(items, table, (field) => {

        if (field.$reql_type$ === 'GEOMETRY' &&
            field.type === 'Point') {

            return field.coordinates;
        }

        return field;
    });
};


exports.write = function (items, table) {

    internals.scan(items, table, (field) => {

        if (Array.isArray(field) &&
            field.length === 2) {

            return { $reql_type$: 'GEOMETRY', coordinates: field, type: 'Point' };
        }

        return field;
    });
};


exports.select = function (items, table) {

    let near = null;
    internals.scan(items, table, (field, index) => {

        if (typeof field !== 'function' ||
            field.type !== 'near') {

            return field;
        }

        if (near) {
            throw new Error('Cannot specify more than one near condition');
        }

        near = { field, index };
        return field;
    });

    if (!near) {
        return table.raw;
    }

    const area = RethinkDB.circle(RethinkDB.point(near.field.value[0], near.field.value[1]), near.field.flags.distance, { unit: near.field.flags.unit || 'm' });
    return table.raw.getIntersecting(area, { index: near.index });
};


internals.scan = function (items, table, each) {

    if (!items) {
        return;
    }

    const array = Array.isArray(items) ? items : [items];
    for (let i = 0; i < array.length; ++i) {
        const item = array[i];
        for (let j = 0; j < table._geo.length; ++j) {
            const index = table._geo[j];
            const path = index.source;

            let ref = item;
            let key = null;

            for (let k = 0; ; ++k) {
                key = path[k];

                if (k === path.length - 1 ||
                    !ref ||
                    typeof ref !== 'object') {

                    break;
                }

                ref = ref[key];
            }

            if (ref &&
                ref[key]) {

                ref[key] = each(ref[key], index.name);
            }
        }
    }
};
