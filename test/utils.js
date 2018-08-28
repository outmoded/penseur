'use strict';

// Load modules

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');


// Declare internals

const internals = {};


// Test shortcuts

const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Penseur.utils', () => {

    describe('diff()', () => {

        it('compares object to base with missing properties', () => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff({ a: 2 }, item)).to.equal(item);
        });

        it('includes deleted items (whitelist)', () => {

            expect(Penseur.utils.diff({ a: 2 }, {}, { whitelist: ['a'], deleted: true })).to.equal({ a: null });
        });

        it('includes deleted items (whitelist missing)', () => {

            expect(Penseur.utils.diff({ a: 2 }, {}, { whitelist: ['b'], deleted: true })).to.be.null();
        });

        it('includes deleted items (overlap)', () => {

            expect(Penseur.utils.diff({ a: 2, b: 5 }, { b: 5 }, { deleted: true })).to.equal({ a: null });
        });

        it('includes deleted items (overlap sub)', () => {

            expect(Penseur.utils.diff({ a: 2, b: { x: 1, y: 3 } }, { b: { y: 3 } }, { deleted: true })).to.equal({ a: null, b: { x: null } });
        });

        it('compares to null', () => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(null, item)).to.equal(item);
        });

        it('compares to null with whitelist', () => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(null, item, { whitelist: ['a'] })).to.equal({ a: 1 });
        });

        it('compares to null with whitelist (no match)', () => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(null, item, { whitelist: ['d'] })).to.be.null();
        });

        it('compares identical objects', () => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(item, item)).to.be.null();
        });

        it('compares arrays', () => {

            const item = { a: [1, 2], b: [4, 3], c: 6 };
            expect(Penseur.utils.diff(item, { a: [0, 2], b: 5, c: [1] })).to.equal({ a: { 0: 0 }, b: 5, c: [1] });
            expect(Penseur.utils.diff(item, { a: { x: 1 } })).to.equal({ a: { x: 1 } });
            expect(Penseur.utils.diff({ a: { x: 1 } }, item)).to.equal(item);
            expect(Penseur.utils.diff(item, { a: [0, 2], b: 5, c: [1] }, { arrays: false })).to.equal({ a: [0, 2], b: 5, c: [1] });
            expect(Penseur.utils.diff(item, { a: [0, 2], b: [4, 3], c: [1] }, { arrays: false })).to.equal({ a: [0, 2], c: [1] });
        });

        it('compares nested arrays', () => {

            const item = { a: { b: [1] } };
            expect(Penseur.utils.diff({ a: {} }, { a: { b: [1, 2] } })).to.equal({ a: { b: [1,2] } });
            expect(Penseur.utils.diff(item, { a: { b: [1, 2] } })).to.equal({ a: { b: { 1: 2 } } });
            expect(Penseur.utils.diff(item, { a: { b: [1, 2] } }, { arrays: false })).to.equal({ a: { b: [1, 2] } });
        });

        it('compares object to null', () => {

            const item = { a: 1, b: { c: 'd' } };
            expect(Penseur.utils.diff(item, { b: null })).to.equal({ b: null });
            expect(Penseur.utils.diff({ b: null }, item)).to.equal(item);
        });
    });
});
