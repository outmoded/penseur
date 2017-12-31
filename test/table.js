'use strict';

// Load modules

const Code = require('code');
const Hoek = require('hoek');
const Lab = require('lab');
const Penseur = require('..');
const RethinkDB = require('rethinkdb');
const Teamwork = require('teamwork');


// Declare internals

const internals = {};


// Test shortcuts

const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Table', () => {

    it('exposes table name', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        expect(db.test.name).to.equal('test');
    });

    describe('get()', () => {

        it('returns the requested objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.get([1, 3]);
            expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
        });

        it('returns the requested object (zero id)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 0, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.get([0]);
            expect(result).to.equal([{ id: 0, a: 1 }]);
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.get(1)).to.reject();
        });

        it('returns the requested objects (array of one)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.get([1]);
            expect(result).to.equal([{ id: 1, a: 1 }]);
        });

        it('returns the requested objects found (partial)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.get([1, 3, 4]);
            expect(result).to.have.length(2);
        });

        it('returns the requested objects found (duplicates)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.get([1, 3, 3]);
            expect(result).to.have.length(3);
        });

        it('returns the requested objects found (none)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.get([4, 5, 6]);
            expect(result).to.equal(null);
        });

        it('returns the requested objects (filter)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 2 }, { id: 3, a: 1, b: 3 }]);
            const result = await db.test.get([1, 3], { filter: ['id', 'b'] });
            expect(result).to.equal([{ id: 3, b: 3 }, { id: 1, b: 1 }]);
        });

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            await expect(db.test.get('1')).to.reject('Database disconnected');
        });

        it('errors on invalid id', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.get('0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789')).to.reject();
        });

        it('errors on invalid ids', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.get(['0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'])).to.reject();
        });
    });

    describe('all()', () => {

        it('returns the requested objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const items = await db.test.all();
            expect(items).to.equal([{ id: 3, a: 1 }, { id: 2, a: 2 }, { id: 1, a: 1 }]);
        });

        it('returns the requested objects (range)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const items = await db.test.all({ count: 2 });
            expect(items).to.equal([{ id: 1, a: 1 }, { id: 2, a: 2 }]);
        });

        it('returns the requested objects (from)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const items = await db.test.all({ from: 1 });
            expect(items).to.equal([{ id: 2, a: 2 }, { id: 3, a: 1 }]);
        });

        it('returns null when range is out of items', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const items1 = await db.test.all();
            expect(items1).to.be.null();

            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const items2 = await db.test.all({ from: 5 });
            expect(items2).to.be.null();
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.all()).to.reject();
        });
    });

    describe('exist()', () => {

        it('checks if record exists', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });
            const exists = await db.test.exist(1);
            expect(exists).to.be.true();
        });

        it('checks if record does not exists', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const exists = await db.test.exist(1);
            expect(exists).to.be.false();
        });

        it('errors on invalid id', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.exist('0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789')).to.reject();
        });
    });

    describe('distinct()', () => {

        it('return distinct combinations (single field)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const items = [
                { id: 1, a: 1, b: 2 },
                { id: 2, a: 2, b: 2 },
                { id: 3, a: 1, b: 2 },
                { id: 4, a: 2, b: 2 },
                { id: 5, a: 1, b: 3 },
                { id: 6, a: 3, b: 3 },
                { id: 7, a: 1, b: 2 }
            ];

            await db.test.insert(items);

            const result = await db.test.distinct(['a']);
            expect(result).to.equal([1, 2, 3]);
        });

        it('return distinct combinations (single field with criteria)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const items = [
                { id: 1, a: 1, b: 2 },
                { id: 2, a: 2, b: 2 },
                { id: 3, a: 1, b: 2 },
                { id: 4, a: 2, b: 2 },
                { id: 5, a: 1, b: 3 },
                { id: 6, a: 3, b: 3 },
                { id: 7, a: 1, b: 2 }
            ];

            await db.test.insert(items);
            const result = await db.test.distinct({ b: 2 }, 'a');
            expect(result).to.equal([1, 2]);
        });

        it('return distinct combinations (combination fields)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const items = [
                { id: 1, a: 1, b: 2 },
                { id: 2, a: 2, b: 2 },
                { id: 3, a: 1, b: 2 },
                { id: 4, a: 2, b: 2 },
                { id: 5, a: 1, b: 3 },
                { id: 6, a: 3, b: 3 },
                { id: 7, a: 1, b: 2 }
            ];

            await db.test.insert(items);
            const result = await db.test.distinct(['a', 'b']);
            expect(result).to.equal([{ a: 1, b: 2 }, { a: 1, b: 3 }, { a: 2, b: 2 }, { a: 3, b: 3 }]);
        });

        it('return null on no combinations', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const result = await db.test.distinct(['a']);
            expect(result).to.null();
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.distinct(['a'])).to.reject();
        });
    });

    describe('query()', () => {

        it('returns the requested objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.query({ a: 1 });
            expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
        });

        it('sorts the requested objects (key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
            const result1 = await db.test.query({ b: 1 }, {});
            expect(result1).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);

            const result2 = await db.test.query({ b: 1 }, { sort: 'a' });
            expect(result2).to.equal([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
        });

        it('sorts the requested objects (nested key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, x: { a: 1 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 3, x: { a: 3 }, b: 1 }]);
            const result1 = await db.test.query({ b: 1 }, {});
            expect(result1).to.equal([{ id: 3, x: { a: 3 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 1, x: { a: 1 }, b: 1 }]);

            const result2 = await db.test.query({ b: 1 }, { sort: ['x', 'a'] });
            expect(result2).to.equal([{ id: 1, x: { a: 1 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 3, x: { a: 3 }, b: 1 }]);
        });

        it('sorts the requested objects (object)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
            const result1 = await db.test.query({ b: 1 }, {});
            expect(result1).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);

            const result2 = await db.test.query({ b: 1 }, { sort: { key: 'a', order: 'descending' } });
            expect(result2).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);
        });

        it('includes results from a given position', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
            const result = await db.test.query({ b: 1 }, { sort: 'a', from: 1 });
            expect(result).to.equal([{ id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
        });

        it('includes n number of results', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
            const result1 = await db.test.query({ b: 1 }, { sort: 'a', count: 2 });
            expect(result1).to.equal([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }]);

            const result2 = await db.test.query({ b: 1 }, { sort: 'a', from: 1, count: 1 });
            expect(result2).to.equal([{ id: 2, a: 2, b: 1 }]);
        });

        it('filters fields', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
            const result = await db.test.query({ a: 1 }, { filter: ['b'] });
            expect(result).to.equal([{ b: 1 }]);
        });

        it('returns all objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.query(null);
            expect(result).to.equal([{ id: 3, a: 1 }, { id: 2, a: 2 }, { id: 1, a: 1 }]);
        });

        it('performs case insensitive match', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 'abcd' }, { id: 2, a: 'AbcD' }, { id: 3, a: 'xabCd' }, { id: 4, a: 'abbb' }]);
            expect(await db.test.query({ a: 'ABCD' })).to.be.null();
            expect(await db.test.query({ a: 'ABCD' }, { match: { insensitive: true } })).to.equal([{ id: 3, a: 'xabCd' }, { id: 2, a: 'AbcD' }, { id: 1, a: 'abcd' }]);
        });
    });

    describe('single()', () => {

        it('returns the requested object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.single({ a: 2 });
            expect(result).to.equal({ id: 2, a: 2 });
        });

        it('returns nothing', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.single({ a: 3 });
            expect(result).to.equal(null);
        });

        it('errors on multiple matches', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            await expect(db.test.single({ a: 1 })).to.reject('Found multiple items');
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.single({ a: 1 })).to.reject();
        });
    });

    describe('count()', () => {

        it('returns the number requested object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.count({ a: 1 });
            expect(result).to.equal(2);
        });

        it('returns the number of object with given field', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.count(db.contains('a'));
            expect(result).to.equal(3);
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.count({ a: 1 })).to.reject();
        });
    });

    describe('insert()', () => {

        it('inserts a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const keys = await db.test.insert({ id: 1, a: 1 });
            expect(keys).to.equal(1);

            const item = await db.test.get(1);
            expect(item.a).to.equal(1);
        });

        it('inserts multiple records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const batches = [];
            const orig = db.test._insert;
            db.test._insert = (items, ...args) => {

                batches.push(Array.isArray(items) ? items.length : 'single');
                return orig.call(db.test, items, ...args);
            };

            const records = [];
            const ids = [];
            for (let i = 1; i < 101; ++i) {
                records.push({ id: i, a: i });
                ids.push(i);
            }

            const keys = await db.test.insert(records, { chunks: 30 });
            expect(keys).to.equal(ids);
            expect(batches).to.equal([30, 30, 30, 10]);

            const items = await db.test.all();
            expect(items.length).to.equal(100);
        });

        it('inserts multiple records (each)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const batches = [];
            const orig = db.test._insert;
            db.test._insert = (items, ...args) => {

                batches.push(Array.isArray(items) ? items.length : 'single');
                return orig.call(db.test, items, ...args);
            };

            const records = [];
            const ids = [];
            for (let i = 1; i < 101; ++i) {
                records.push({ id: i, a: i });
                ids.push(i);
            }

            const updates = [];
            const keys = await db.test.insert(records, { chunks: 30, each: (s, is) => updates.push({ i: s, ids: is }) });
            expect(keys).to.equal(ids);
            expect(batches).to.equal([30, 30, 30, 10]);
            expect(updates).to.equal([
                { i: 0, ids: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30] },
                { i: 1, ids: [31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60] },
                { i: 2, ids: [61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90] },
                { i: 3, ids: [91, 92, 93, 94, 95, 96, 97, 98, 99, 100] }
            ]);

            const items = await db.test.all();
            expect(items.length).to.equal(100);
        });

        it('inserts a record (ignore batch on non array)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const batches = [];
            const orig = db.test._insert;
            db.test._insert = (items, ...args) => {

                batches.push(Array.isArray(items) ? items.length : 'single');
                return orig.call(db.test, items, ...args);
            };

            const keys = await db.test.insert({ id: 1, a: 1 }, { chunks: 10 });
            expect(keys).to.equal(1);
            expect(batches).to.equal(['single']);

            const item = await db.test.get(1);
            expect(item.a).to.equal(1);
        });

        it('inserts a record (ignore batch on solo array)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const batches = [];
            const orig = db.test._insert;
            db.test._insert = (items, ...args) => {

                batches.push(Array.isArray(items) ? items.length : 'single');
                return orig.call(db.test, items, ...args);
            };

            const keys = await db.test.insert([{ id: 1, a: 1 }], { chunks: 10 });
            expect(keys).to.equal([1]);
            expect(batches).to.equal([1]);

            const item = await db.test.get(1);
            expect(item.a).to.equal(1);
        });

        it('updates a record if exists', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1, b: 1 }, { merge: true });
            const item1 = await db.test.get(1);
            expect(item1).to.equal({ id: 1, a: 1, b: 1 });

            await db.test.insert({ id: 1, a: 2 }, { merge: true });
            const item2 = await db.test.get(1);
            expect(item2).to.equal({ id: 1, a: 2, b: 1 });
        });

        it('returns the generate key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.match(/\w+/);
        });

        it('returns the generate key (existing)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const keys = await db.test.insert({ id: 11, a: 1 });
            expect(keys).to.equal(11);
        });

        it('generates key locally (uuid)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { id: { type: 'uuid' } } });
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.match(/^[\da-f]{8}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{12}$/);
        });

        it('returns the generate keys', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const keys = await db.test.insert([{ a: 1 }, { a: 2 }]);
            expect(keys).to.have.length(2);
        });

        it('returns the generate keys when keys are present', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const keys = await db.test.insert([{ id: 1, a: 1 }, { a: 2 }]);
            expect(keys).to.have.length(2);
            expect(keys[0]).to.equal(1);
        });

        it('returns the generate keys when keys are present (last)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const keys = await db.test.insert([{ a: 1 }, { id: 1, a: 2 }]);
            expect(keys).to.have.length(2);
            expect(keys[1]).to.equal(1);
        });

        it('returns the generate keys when keys are present (mixed)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            const keys = await db.test.insert([{ a: 1 }, { id: 1, a: 2 }, { id: 2, a: 3 }, { a: 4 }, { a: 5 }, { id: 3, a: 6 }, { id: 4, a: 7 }]);
            expect(keys).to.have.length(7);
            expect(keys[1]).to.equal(1);
            expect(keys[2]).to.equal(2);
            expect(keys[5]).to.equal(3);
            expect(keys[6]).to.equal(4);
        });

        it('errors on key conflict', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });
            await expect(db.test.insert({ id: 1, a: 1 })).to.reject();
        });

        it('errors on upsert unique cleanup', async () => {

            const db = new Penseur.Db('penseurtest', { test: true });
            const settings = {
                penseur_unique_test_a: true,                 // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: 'a'
                    }
                }
            };

            await db.establish(settings);
            await db.test.insert({ id: '1', a: 1 });
            db.test._unique.rules[0].table.remove = (ids, next) => next(new Error());
            await expect(db.test.insert({ id: '1', a: 2 }, { merge: true })).to.reject();
        });

        it('errors on batch insert with multiple records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            db.test._insert = (items, options, next) => next(new Error());

            const records = [];
            for (let i = 1; i < 101; ++i) {
                records.push({ id: i, a: i });
            }

            await expect(db.test.insert(records, { chunks: 30 })).to.reject();
        });
    });

    describe('update()', () => {

        it('updates a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });
            await db.test.update(1, { a: 2 });

            const item = await db.test.get(1);
            expect(item.a).to.equal(2);
        });

        it('updates a record with empty object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });

            await db.test.update(1, { a: {} });

            const item = await db.test.get(1);
            expect(item.a).to.equal({});
        });

        it('updates a record with nested empty object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1 });

            await db.test.update(1, { a: { b: {} } });

            const item = await db.test.get(1);
            expect(item.a.b).to.equal({});
        });

        it('updates a record (increment modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1,
                b: {
                    c: 2
                }
            };

            await db.test.insert(item);

            const changes = {
                a: 2,
                b: {
                    c: db.increment(10)
                }
            };

            expect(changes.b.c).to.be.a.function();

            await db.test.update(1, changes);
            expect(changes.b.c).to.be.a.function();

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: 2,
                b: {
                    c: 12
                }
            });
        });

        it('updates a record (append modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1,
                b: {
                    c: [2]
                }
            };

            await db.test.insert(item);

            const changes = {
                a: 2,
                b: {
                    c: db.append(10)
                }
            };

            expect(changes.b.c).to.be.a.function();

            await db.test.update(1, changes);
            expect(changes.b.c).to.be.a.function();

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: 2,
                b: {
                    c: [2, 10]
                }
            });
        });

        it('updates a record (append array modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1,
                b: {
                    c: [2]
                }
            };

            await db.test.insert(item);

            const changes = {
                a: 2,
                b: {
                    c: db.append([10, 20])
                }
            };

            expect(changes.b.c).to.be.a.function();

            await db.test.update(1, changes);
            expect(changes.b.c).to.be.a.function();

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: 2,
                b: {
                    c: [2, 10, 20]
                }
            });
        });

        it('updates a record (append array single modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1,
                b: {
                    c: [2]
                }
            };

            await db.test.insert(item);

            const changes = {
                a: 2,
                b: {
                    c: db.append([10, 20], { single: true })
                }
            };

            expect(changes.b.c).to.be.a.function();

            await db.test.update(1, changes);
            expect(changes.b.c).to.be.a.function();

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: 2,
                b: {
                    c: [2, [10, 20]]
                }
            });
        });

        it('updates a record (unset modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1,
                b: {
                    c: [2]
                }
            };

            await db.test.insert(item);

            const changes = {
                a: 2,
                b: {
                    c: db.unset()
                }
            };

            expect(changes.b.c).to.be.a.function();

            await db.test.update(1, changes);
            expect(changes.b.c).to.be.a.function();

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: 2,
                b: {}
            });
        });

        it('updates a record (only unset modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1
            };

            await db.test.insert(item);

            const changes = {
                a: db.unset()
            };

            await db.test.update(1, changes);
            const updated = await db.test.get(1);
            expect(updated).to.equal({ id: 1 });
        });

        it('updates a record (no changes)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1
            };

            await db.test.insert(item);
            await db.test.update(1, {});
            const updated = await db.test.get(1);
            expect(updated).to.equal({ id: 1, a: 1 });
        });

        it('updates a record (unset and append modifiers)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: [1],
                b: {
                    c: [2]
                }
            };

            await db.test.insert(item);

            const changes = {
                a: db.append(2),
                b: {
                    c: db.unset()
                }
            };

            await db.test.update(1, changes);
            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: [1, 2],
                b: {}
            });
        });

        it('updates a record (override key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1, b: { c: 1, d: 1 }, e: 1 });
            await db.test.update(1, { a: 2, b: { d: 2 } });
            const item1 = await db.test.get(1);
            expect(item1).to.equal({ id: 1, a: 2, b: { c: 1, d: 2 }, e: 1 });

            await db.test.update(1, { a: 3, b: db.override({ c: 2 }) });

            const item2 = await db.test.get(1);
            expect(item2).to.equal({ id: 1, a: 3, b: { c: 2 }, e: 1 });
        });

        it('updates multiple records (same value)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

            await db.test.update([1, 2], { a: db.unset() });

            const items = await db.test.all();
            expect(items).to.equal([{ id: 2 }, { id: 1 }]);
        });

        it('updates multiple records (different value)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

            await db.test.update([{ id: 1, a: 3 }, { id: 2, a: db.unset() }]);

            const items = await db.test.all();
            expect(items).to.equal([{ id: 2 }, { id: 1, a: 3 }]);
        });

        it('updates a record (ignore batch)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }]);

            const batches = [];
            const orig = db.test._update;
            db.test._update = (ids, ...args) => {

                batches.push(ids.length);
                return orig.call(db.test, ids, ...args);
            };

            await db.test.update([{ id: 1, a: 2 }], { chunks: 10 });
            expect(batches).to.equal([1]);

            const item = await db.test.get(1);
            expect(item.a).to.equal(2);
        });

        it('updates a record (ignore empty options)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }]);

            const batches = [];
            const orig = db.test._update;
            db.test._update = (ids, ...args) => {

                batches.push(ids.length);
                return orig.call(db.test, ids, ...args);
            };

            await db.test.update([{ id: 1, a: 2 }], {});
            expect(batches).to.equal([1]);

            const item = await db.test.get(1);
            expect(item.a).to.equal(2);
        });

        it('updates multiple records (chunks)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const records = [];
            for (let i = 1; i < 101; ++i) {
                records.push({ id: i, a: i });
            }

            await db.test.insert(records);

            const batches = [];
            const orig = db.test._update;
            db.test._update = (ids, ...args) => {

                batches.push(ids.length);
                return orig.call(db.test, ids, ...args);
            };

            const updates = [];
            for (let i = 1; i < 101; ++i) {
                updates.push({ id: i, a: db.unset() });
            }

            await db.test.update(updates, { chunks: 30 });
            expect(batches).to.equal([30, 30, 30, 10]);

            const item = await db.test.get(1);
            expect(item.a).to.not.exist();
        });

        it('updates multiple records (chunks + each)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const records = [];
            for (let i = 1; i < 101; ++i) {
                records.push({ id: i, a: i });
            }

            await db.test.insert(records);

            const batches = [];
            const orig = db.test._update;
            db.test._update = (ids, ...args) => {

                batches.push(ids.length);
                return orig.call(db.test, ids, ...args);
            };

            const updates = [];
            for (let i = 1; i < 101; ++i) {
                updates.push({ id: i, a: db.unset() });
            }

            const reports = [];
            await db.test.update(updates, { chunks: 30, each: (s) => reports.push(s) });
            expect(batches).to.equal([30, 30, 30, 10]);
            expect(reports).to.equal([0, 1, 2, 3]);

            const item = await db.test.get(1);
            expect(item.a).to.not.exist();
        });

        it('updates a record (composite key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: [1, 1], a: 1 });

            const item1 = await db.test.get({ id: [1, 1] });
            expect(item1.a).to.equal(1);

            await db.test.update({ id: [1, 1] }, { a: 2 });

            const item2 = await db.test.get({ id: [1, 1] });
            expect(item2.a).to.equal(2);
        });

        it('ignroes empty array', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.update([]);
        });

        it('errors on unknown key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.update(2, { a: 2 })).to.reject('No document found');
        });

        it('errors on invalid object key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.update({}, { a: 2 })).to.reject('Invalid object id');
        });
    });

    describe('items()', () => {

        it('updates a record', () => {

            const changes = { a: 2 };
            expect(Penseur.Table.items(1, changes)).to.equal([changes]);
            expect(Penseur.Table.items([1, 2], changes)).to.equal([changes]);
            expect(Penseur.Table.items([changes, changes])).to.equal([changes, changes]);
            expect(Penseur.Table.items([changes, changes], {})).to.equal([changes, changes]);
            expect(Penseur.Table.items([])).to.equal([]);
        });
    });

    describe('next()', () => {

        it('updates a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });

            await db.test.next(1, 'a', 5);

            const item = await db.test.get(1);
            expect(item.a).to.equal(6);
        });

        it('errors on unknown key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.next(1, 'a', 5)).to.reject();
        });

        it('errors on invalid key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.next([1], 'a', 5)).to.reject('Array of ids not supported');
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.next(1, 'a', 5)).to.reject();
        });
    });

    describe('remove()', () => {

        it('removes a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });

            await db.test.remove(1);

            const item = await db.test.get(1);
            expect(item).to.not.exist();
        });

        it('removes a record (composite id)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: [1, 1], a: 1 });

            await db.test.remove({ id: [1, 1] });

            const item = await db.test.get({ id: [1, 1] });
            expect(item).to.not.exist();
        });

        it('removes multiple records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

            await db.test.remove([1, 2]);

            const count = await db.test.count({ a: 1 });
            expect(count).to.equal(0);
        });

        it('removes records using criteria', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

            await db.test.remove({ a: 1 });

            const count = await db.test.count({ a: 1 });
            expect(count).to.equal(0);
        });

        it('errors on unknown key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.remove(1)).to.reject();
        });

        it('errors on invalid key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await expect(db.test.remove([])).to.reject();
        });

        it('ignores error on unknown keys', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.remove([1]);
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.remove(1)).to.reject();
        });
    });

    describe('empty()', () => {

        it('removes all records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

            const count1 = await db.test.empty();
            expect(count1).to.equal(2);

            const count2 = await db.test.count({ a: 1 });
            expect(count2).to.equal(0);
        });

        it('errors on unknown table', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.connect();
            db.table('no_such_table_test');
            await expect(db.no_such_table_test.empty()).to.reject();
        });
    });

    describe('sync()', () => {

        it('returns when write is complete', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            await db.test.sync();
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.sync()).to.reject();
        });

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            await expect(db.test.sync()).to.reject('Database disconnected');
        });
    });

    describe('index()', () => {

        it('adds a secondary index', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.index('x');

            const result = await RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);
            expect(result[0]).to.contain({ index: 'x', multi: false, geo: false, ready: true });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.index('x')).to.reject();
        });

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            await expect(db.test.index('x')).to.reject('Database disconnected');
        });

        it('creates simple index from a string', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.index('simple');
            const result = await RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);
            expect(result[0]).to.contain({ index: 'simple', multi: false, geo: false, ready: true });
        });

        it('creates index from function', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const index = {
                name: 'name',
                source: (row) => row('name')
            };

            await db.test.index(index);
            const result = await RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);
            expect(result[0]).to.contain({ index: 'name', multi: false, geo: false, ready: true });
            expect(result[0].query).to.contain('("name")');
        });

        it('creates compound index from an array of fields', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.index({ name: 'compound', source: ['some', 'other'] });
            const result = await RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);
            expect(result[0]).to.contain({ index: 'compound', multi: false, geo: false, ready: true });
            expect(result[0].query).to.include('r.row("some")').and.to.include('r.row("other")');
        });

        it('creates index with options', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.index({ name: 'simple-multi', options: { multi: true } });
            const result = await RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);
            expect(result[0]).to.contain({ index: 'simple-multi', multi: true, geo: false, ready: true });
        });

        it('creates multiple indexes from array', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const indexes = [
                'simple',
                { name: 'location', options: { geo: true } }
            ];

            await db.test.index(indexes);
            const result = await RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);
            expect(result[0]).to.contain({ geo: true, index: 'location', multi: false, ready: true });
            expect(result[1]).to.contain({ geo: false, index: 'simple', multi: false, ready: true });
        });

        it('propagates errors from indexCreate', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const orig = db.test.raw.indexCreate;
            db.test.raw.indexCreate = () => {

                return {
                    run: () => Promise.reject(new Error('simulated error'))
                };
            };

            await expect(db.test.index('simple')).to.reject('simulated error');
            db.test.raw.indexCreate = orig;
        });
    });

    describe('changes()', () => {

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            await expect(db.test.changes('*', Hoek.ignore)).to.reject('Database disconnected');
        });

        it('reports on a record update (*)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.after.id);
            };

            await db.test.changes('*', each);

            await db.test.insert({ id: 1, a: 1 });
            await db.test.update(1, { a: 2 });
            expect(changes).to.equal([1, 1]);
            await db.close();
        });

        it('manually closes a cursor', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.after.id);
            };

            const cursor = await db.test.changes('*', each);
            await db.test.insert({ id: 1, a: 1 });
            await db.test.update(1, { a: 2 });
            expect(changes).to.equal([1, 1]);
            cursor.close();
            await db.close();
        });

        it('reports on a record update (id)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.after.id);
            };

            await db.test.insert([{ id: 1, a: 1 }]);

            await db.test.changes(1, each);
            await db.test.update(1, { a: 2 });
            await db.test.insert({ id: 2, a: 2 });
            expect(changes).to.equal([1]);
            await db.close();
        });

        it('reports on a record update (ids)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.after.id);
            };

            await db.test.insert([{ id: 1, a: 1 }]);
            await db.test.changes([1, 2], each);
            await db.test.update(1, { a: 2 });
            await db.test.insert({ id: 2, a: 2 });
            expect(changes).to.equal([1, 2]);
            await db.close();
        });

        it('reports on a record update (query)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.after.id);
            };

            await db.test.insert([{ id: 1, a: 1 }]);
            await db.test.changes({ a: 2 }, each);
            await db.test.update(1, { a: 2 });
            await db.test.insert({ id: 2, a: 2 });
            expect(changes).to.equal([1, 2]);
            await db.close();
        });

        it('reports on a record update (delete)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.before.id + ':' + (item.after === null));
            };

            await db.test.insert([{ id: 1, a: 1 }]);
            await db.test.changes(1, each);
            await db.test.remove(1);
            expect(changes).to.equal(['1:true']);
            await db.close();
        });

        it('reports on a record update (id missing)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.after.id);
            };

            await db.test.changes(1, each);
            await db.test.insert({ id: 1, a: 1 });
            expect(changes).to.equal([1]);
            await db.close();
        });

        it('includes initial state', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.id);
            };

            await db.test.insert([{ id: 1, a: 1 }]);
            await db.test.changes(1, { handler: each, initial: true });
            await db.test.update(1, { a: 2 });
            await db.test.insert({ id: 2, a: 2 });
            expect(changes).to.equal([1, 1]);
            await db.close();
        });

        it('handles initial state on missing initial item', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                expect(err).to.not.exist();
                changes.push(item.id);
            };

            await db.test.changes(1, { handler: each, initial: true });
            await db.test.insert([{ id: 1, a: 1 }]);
            await db.test.update(1, { a: 2 });
            await db.test.insert({ id: 2, a: 2 });
            expect(changes).to.equal([1, 1]);
            await db.close();
        });

        it('handles closed cursor while still processing rows', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const each = (err, item) => {

                expect(err).to.not.exist();
            };

            await db.test.changes(1, { handler: each, initial: true });
            await db.close();
        });

        it('reconnects', async () => {

            const team = new Teamwork();

            let step2 = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (step2) {
                    step2();
                }
            };

            const db = new Penseur.Db('penseurtest', { onConnect });
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                if (!err) {
                    changes.push(item.type);

                    if (changes.length === 3) {
                        expect(changes).to.equal(['insert', { willReconnect: true, disconnected: true }, 'initial']);
                        expect(count).to.equal(2);
                        team.attend();
                    }
                }
                else {
                    changes.push(err.flags);
                }
            };

            await db.test.changes(1, { handler: each, initial: true });
            await db.test.insert([{ id: 1, a: 1 }]);
            step2 = async () => {

                step2 = null;
                await db.test.update(1, { a: 2 });
            };

            db._connection.close();
            await team.work;
            await db.close();
        });

        it('does not reconnect on manual cursor close', async () => {

            const team = new Teamwork();

            let step2 = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (step2) {
                    step2();
                }
            };

            const db = new Penseur.Db('penseurtest', { onConnect });
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                if (!err) {
                    changes.push(item.type);
                }
                else {
                    changes.push(err.flags);
                }
            };

            const cursor = await db.test.changes(1, { handler: each, initial: true });
            await db.test.insert([{ id: 1, a: 1 }]);
            cursor.close();

            step2 = async () => {

                step2 = null;
                await db.test.update(1, { a: 2 });
                await db.test.update(1, { a: 2 });
                expect(changes).to.equal(['insert']);
                expect(count).to.equal(2);
                team.attend();
            };

            db._connection.close();
            await team.work;
            await db.close();
        });

        it('does not reconnect (feed reconnect disabled)', async () => {

            const team = new Teamwork();

            let step2 = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (step2) {
                    step2();
                }
            };

            const db = new Penseur.Db('penseurtest', { onConnect });
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                if (!err) {
                    changes.push(item.type);
                }
                else {
                    changes.push(err.flags);
                }
            };

            await db.test.changes(1, { handler: each, initial: true, reconnect: false });
            await db.test.insert([{ id: 1, a: 1 }]);

            step2 = async () => {

                step2 = null;
                await db.test.update(1, { a: 2 });
                await db.test.update(1, { a: 2 });
                expect(changes).to.equal(['insert', { willReconnect: false, disconnected: true }]);
                expect(count).to.equal(2);
                team.attend();
            };

            db._connection.close();
            await team.work;
            await db.close();
        });

        it('does not reconnect (db reconnect disabled)', async () => {

            new Teamwork();

            let count = 0;
            const onConnect = () => {

                ++count;
            };

            const db = new Penseur.Db('penseurtest', { onConnect, reconnect: false });
            await db.establish(['test']);

            const changes = [];
            const each = (err, item) => {

                if (!err) {
                    changes.push(item.type);
                }
                else {
                    changes.push(err.flags);
                }
            };

            await db.test.changes(1, { handler: each, initial: true });
            await db.test.insert([{ id: 1, a: 1 }]);

            await db._connection.close();
            await Hoek.wait(100);

            expect(changes).to.equal(['insert', { willReconnect: false, disconnected: true }]);
            expect(count).to.equal(1);
            await db.close();
        });

        it('errors on bad cursor', async () => {

            const team = new Teamwork();

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const each = (err, item) => {

                if (err) {
                    expect(err.message).to.equal('kaboom');
                    team.attend();
                }
            };

            const cursor = await db.test.changes('*', each);

            const orig = cursor._cursor._next;
            cursor._cursor._next = () => {

                cursor._cursor._next = orig;
                throw new Error('kaboom');
            };

            await db.test.insert({ id: 1, a: 1 });
            await team.work;
        });

        it('errors on invalid table', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();
            await expect(db.invalid.changes('*', Hoek.ignore)).to.reject();
        });
    });
});
