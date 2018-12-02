'use strict';

const Hoek = require('hoek');


const internals = {};


module.exports = internals.Special = class {

    constructor(type, value, options) {

        this.type = type;
        this.flags = (options ? Hoek.clone(options) : {});

        if (value !== undefined) {
            this.value = value;
        }
    }

    static isSpecial(value) {

        return value instanceof internals.Special;
    }
};
