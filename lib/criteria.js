'use strict';

// Load Modules


// Declare internals

const internals = {};


exports = module.exports = internals.Criteria = class {

    constructor(criteria, type) {

        this.criteria = criteria;
        this.type = type || 'filter';
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
};
