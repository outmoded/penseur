'use strict';

// Load modules

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');


// Declare internals

const internals = {};


// Test shortcuts

const lab = exports.lab = Lab.script();
const describe = lab.describe;
const it = lab.it;
const expect = Code.expect;


describe('Penseur.utils', () => {

    describe('diff()', () => {

        it('compares object to base with missing properties', (done) => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff({ a: 2 }, item)).to.equal(item);
            done();
        });

        it('includes deleted items (whitelist)', (done) => {

            expect(Penseur.utils.diff({ a: 2 }, {}, { whitelist: ['a'], deleted: true })).to.equal({ a: null });
            done();
        });

        it('includes deleted items (whitelist missing)', (done) => {

            expect(Penseur.utils.diff({ a: 2 }, {}, { whitelist: ['b'], deleted: true })).to.be.null();
            done();
        });

        it('includes deleted items (overlap)', (done) => {

            expect(Penseur.utils.diff({ a: 2, b: 5 }, { b: 5 }, { deleted: true })).to.equal({ a: null });
            done();
        });

        it('includes deleted items (overlap sub)', (done) => {

            expect(Penseur.utils.diff({ a: 2, b: { x: 1, y: 3 } }, { b: { y: 3 } }, { deleted: true })).to.equal({ a: null, b: { x: null } });
            done();
        });

        it('compares to null with whitelist', (done) => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(null, item, { whitelist: ['a'] })).to.equal({ a: 1 });
            done();
        });

        it('compares to null with whitelist (no match)', (done) => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(null, item, { whitelist: ['d'] })).to.be.null();
            done();
        });

        it('compares identical objects', (done) => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(item, item)).to.be.null();
            done();
        });
    });
});
