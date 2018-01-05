'use strict';

// Load Modules

const Hoek = require('hoek');
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

    return internals.scan(items, table, false, (field) => {

        if (field.$reql_type$ === 'GEOMETRY' &&
            field.type === 'Point') {

            return field.coordinates;
        }
    });
};


exports.write = function (items, table) {

    return internals.scan(items, table, true, (field) => {

        if (Array.isArray(field) &&
            field.length === 2) {

            return { $reql_type$: 'GEOMETRY', coordinates: field, type: 'Point' };
        }
    });
};


exports.select = function (items, table) {

    let near = null;
    internals.scan(items, table, false, (field, index) => {

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


internals.scan = function (items, table, clone, each) {

    if (!items) {
        return items;
    }

    const result = [];
    const isArray = Array.isArray(items);
    const array = isArray ? items : [items];
    for (let i = 0; i < array.length; ++i) {
        let item = array[i];
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

                const override = each(ref[key], index.name);
                if (override === undefined) {
                    continue;
                }

                if (!clone) {
                    ref[key] = override;
                }
                else {
                    item = Hoek.clone(item);
                    internals.override(item, path, override);
                }
            }
        }

        result.push(item);
    }

    return (isArray ? result : result[0]);
};


internals.override = function (item, path, override) {

    let ref = item;
    for (let i = 0; i < path.length - 1; ++i) {
        ref = ref[path[i]];
    }

    ref[path[path.length - 1]] = override;
};
