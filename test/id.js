'use strict';

// Load modules

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');
const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


// Test shortcuts

const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Id', () => {

    describe('normalize()', () => {

        it('errors on empty array of ids', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.get([])).to.reject('Empty array of ids not supported');
        });

        it('errors on null id', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.get([null])).to.reject('Invalid null or undefined id');
        });

        it('errors on undefined id', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.get([undefined])).to.reject('Invalid null or undefined id');
        });
    });

    describe('wrap()', () => {

        it('generates keys locally and server-side', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { id: { type: 'uuid' } } });
            const keys = await db.test.insert([{ id: 'abc', a: 1 }, { a: 2 }]);
            expect(keys[0]).to.equal('abc');
            expect(keys[1]).to.match(/^[\da-f]{8}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{12}$/);
        });

        it('generates keys server-side', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { id: { type: 'uuid' } } });
            const keys = await db.test.insert([{ id: 'abc', a: 1 }, { id: 'def', a: 2 }]);
            expect(keys).to.equal(['abc', 'def']);
        });
    });

    describe('uuid()', () => {

        it('generates keys locally', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { id: { type: 'uuid' } } });
            const keys = await db.test.insert([{ a: 1 }, { a: 2 }]);
            expect(keys.length).to.equal(2);
        });

        it('generates keys locally (implicit config)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { id: 'uuid' } });
            const keys = await db.test.insert([{ a: 1 }, { a: 2 }]);
            expect(keys.length).to.equal(2);
        });
    });

    describe('increment()', () => {

        it('generates key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.equal('1');
        });

        it('generates key (implicit config)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ penseur_id_allocate: true, test: { id: 'increment' } });
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.equal('1');
        });

        it('generates keys (same table)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            const keys = await db.test.insert([{ a: 1 }, { a: 2 }]);
            expect(keys).to.equal(['1', '2']);
        });

        it('generates key (different tables)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.connect();
            await RethinkDB.dbDrop(db.name).run(db._connection);
            await db.establish({ test1: { id: { type: 'increment', table: 'allocate' } }, test2: { id: { type: 'increment', table: 'allocate' } } });
            const keys1 = await db.test1.insert({ a: 1 });
            expect(keys1).to.equal('1');
            const keys2 = await db.test2.insert({ a: 1 });
            expect(keys2).to.equal('1');
            await db.close();
        });

        it('completes an existing incomplete allocation record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            await db.allocate.update('test', { value: db.unset() });
            db.test._id.verified = false;
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.equal('1');
        });

        it('reuses generate record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            await db.allocate.update('test', { value: 33 });
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.equal('34');
        });

        it('generates base62 id', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate', radix: 62 } } });
            await db.allocate.update('test', { value: 1324 });
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.equal('ln');
        });

        it('customizes key generation', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate', initial: 1325, radix: 62, record: 'test-id', key: 'v' } } });
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.equal('ln');
        });

        it('errors on invalid generate record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            await db.allocate.update('test', { value: 'string' });
            db.test._id.verified = false;
            const err = await expect(db.test.insert({ a: 1 })).to.reject();
            expect(err.data.error.message).to.equal('Increment id record contains non-integer value: test');
        });

        it('errors on create table error', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            db.test._db._createTable = () => Promise.reject(new Error('Failed'));
            db.test._id.verified = false;
            const err = await expect(db.test.insert({ a: 1 })).to.reject();
            expect(err.data.error.message).to.equal('Failed creating increment id table: test');
        });

        it('errors on table get error', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            db.test._id.table.get = () => Promise.reject(new Error('Failed'));
            db.test._id.verified = false;
            const err = await expect(db.test.insert({ a: 1 })).to.reject();
            expect(err.data.error.message).to.equal('Failed verifying increment id record: test');
        });

        it('errors on table update error', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            await db.allocate.update('test', { value: db.unset() });
            db.test._id.table.update = () => Promise.reject(new Error('Failed'));
            db.test._id.verified = false;
            const err = await expect(db.test.insert({ a: 1 })).to.reject();
            expect(err.data.error.message).to.equal('Failed initializing key-value pair to increment id record: test');
        });

        it('errors on table insert error', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            await db.allocate.remove('test');
            db.test._id.table.insert = () => Promise.reject(new Error('Failed'));
            db.test._id.verified = false;
            const err = await expect(db.test.insert({ a: 1 })).to.reject();
            expect(err.data.error.message).to.equal('Failed inserting increment id record: test');
        });

        it('errors on table next error', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } });
            db.test._id.table.next = () => Promise.reject(new Error('Failed'));
            db.test._id.verified = false;
            const err = await expect(db.test.insert([{ a: 1 }, { a: 1 }])).to.reject();
            expect(err.data.error.message).to.equal('Failed allocating increment id: test');
        });
    });
});
