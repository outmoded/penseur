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

        it('compares to null', (done) => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(null, item)).to.equal(item);
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

        it('compares arrays', (done) => {

            const item = { a: [1, 2], b: [4, 3], c: 6 };
            expect(Penseur.utils.diff(item, { a: [0, 2], b: 5, c: [1] })).to.equal({ a: { 0: 0 }, b: 5, c: [1] });
            expect(Penseur.utils.diff(item, { a: { x: 1 } })).to.equal({ a: { x: 1 } });
            expect(Penseur.utils.diff({ a: { x: 1 } }, item)).to.equal(item);
            expect(Penseur.utils.diff(item, { a: [0, 2], b: 5, c: [1] }, { arrays: false })).to.equal({ a: [0, 2], b: 5, c: [1] });
            expect(Penseur.utils.diff(item, { a: [0, 2], b: [4, 3], c: [1] }, { arrays: false })).to.equal({ a: [0, 2], c: [1] });
            done();
        });

        it('compares object to null', (done) => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(item, { b: null })).to.equal({ b: null });
            expect(Penseur.utils.diff({ b: null }, item)).to.equal(item);
            done();
        });
    });
});
