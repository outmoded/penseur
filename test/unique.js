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


describe('Unique', () => {

    describe('reserve()', () => {

        it('allows setting a unique value', async () => {

            const db = new Penseur.Db('penseurtest');
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
            const keys = await db.test.insert([{ a: 1 }, { a: 2 }]);
            expect(keys.length).to.equal(2);
        });

        it('allows setting a unique value (nested)', async () => {

            const db = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a_b: true,                  // Test cleanup
                penseur_unique_test_a_c_d: true,                // Test cleanup
                test: {
                    id: 'uuid',
                    unique: [
                        { path: ['a', 'b'] },
                        { path: ['a', 'c', 'd'] }
                    ]
                }
            };

            await db.establish(settings);
            const keys = await db.test.insert([{ id: 1, a: { b: 1 } }, { id: 2, a: { c: { d: [2] } } }]);
            expect(keys.length).to.equal(2);

            await expect(db.test.update(2, { a: { b: 1 } })).to.reject();
            await db.test.update(2, { a: { c: { d: db.append([1, 2]) } } });
        });

        it('allows updating a unique value', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.update('1', { a: 2 });
            await db.test.update('1', { a: 1 });
        });

        it('allows userting a unique value', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.insert({ id: '1', a: 1 }, { merge: true });
            await db.test.insert({ id: '1', a: 2 }, { merge: true });
            await db.test.insert({ id: '2', a: 1 });
        });

        it('allows appending a unique value', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.insert({ id: '1', a: [1] });
            await db.test.update('1', { a: db.append(2) });
            await db.test.update('1', { a: db.append(1) });
        });

        it('releases value on unset', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.update('1', { a: db.unset() });
            await db.test.insert({ id: 2, a: 1 });
        });

        it.skip('releases value on unset (parent)', async () => {

            const db = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a_b: true,              // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: ['a', 'b']
                    }
                }
            };

            await db.establish(settings);
            await db.test.insert({ id: '1', a: { b: 1 } });
            await db.test.update('1', { a: db.unset() });
            await db.test.insert({ id: 2, a: { b: 1 } });
        });

        it('ignores empty object', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.insert({ id: '1', a: {} });
        });

        it('ignores missing path parent (2 segments)', async () => {

            const db = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a: true,                 // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: ['a', 'b']
                    }
                }
            };

            await db.establish(settings);
            await db.test.insert({ id: '1' });
        });

        it('allows adding a unique value via update', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.insert({ id: '1' });
            await db.test.update('1', { a: 2 });
        });

        it('forbids violating a unique value', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await expect(db.test.insert([{ a: 1 }, { a: 1 }])).to.reject();
        });

        it('forbids violating a unique value (keys)', async () => {

            const db = new Penseur.Db('penseurtest');
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
            const key = await db.test.insert({ a: { b: [1] } });
            await db.test.update(key, { a: { b: db.append(2) } });
            await db.test.insert({ a: { c: 2, d: 4 } });
            await expect(db.test.insert({ a: { b: 3 } })).to.reject();
            await expect(db.test.insert({ a: { d: 5 } })).to.reject();
        });

        it('allows same owner changes', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.insert({ id: 1, a: { c: 1 } });
            await db.test.update(1, { a: { c: 5 } });
        });

        it('releases reservations on update (keys)', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.insert({ id: 1, a: { b: 1 } });
            await db.test.insert({ id: 2, a: { c: 2, d: 4 } });
            await db.test.update(2, { a: { c: db.unset() } });
            await db.test.update(1, { a: { c: 5 } });
        });

        it('forbids violating a unique value (array)', async () => {

            const db = new Penseur.Db('penseurtest');
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
            await db.test.insert({ a: ['b'] });
            const key = await db.test.insert({ a: ['c', 'a'] });
            await expect(db.test.insert({ a: ['b'] })).to.reject();
            await expect(db.test.insert({ a: ['a'] })).to.reject();
            await db.test.update(key, { a: [] });
            await db.test.insert({ a: ['a'] });
        });

        it('customizes unique table name', async () => {

            const db = new Penseur.Db('penseurtest');
            const settings = {
                unique_a: true,                     // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: 'a',
                        table: 'unique_a'
                    }
                }
            };

            await db.establish(settings);
            const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }]);
            expect(keys.length).to.equal(2);

            const items = await db.unique_a.get([1, 2]);
            expect(items.length).to.equal(2);
        });

        it('ignores non unique keys', async () => {

            const db = new Penseur.Db('penseurtest');
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
            const keys = await db.test.insert([{ b: 1 }, { b: 2 }]);
            expect(keys.length).to.equal(2);
        });

        it('ignore further nested values', async () => {

            const db = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a_b_c: true,            // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: ['a', 'b', 'c']
                    }
                }
            };

            await db.establish(settings);
            await db.test.insert({ a: { b: 1 } });
        });

        it('ignore further nested values (non existing)', async () => {

            const db = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a_b_c: true,            // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: ['a', 'b', 'c']
                    }
                }
            };

            await db.establish(settings);
            await db.test.insert({ d: { b: 1 } });
        });

        it('errors on incrementing unique index', async () => {

            const db = new Penseur.Db('penseurtest');
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
            const key = await db.test.insert({ a: 1 });
            await expect(db.test.update(key, { a: db.increment(1) })).to.reject();
        });

        it('errors on appending a single array to a unique index', async () => {

            const db = new Penseur.Db('penseurtest');
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
            const key = await db.test.insert({ a: [1] });
            await expect(db.test.update(key, { a: db.append([2], { single: true }) })).to.reject();
        });

        it('errors on database unique table get error', async () => {

            const db = new Penseur.Db('penseurtest');
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
            db.test._unique.rules[0].table.get = () => Promise.reject(new Error('boom'));
            await expect(db.test.insert([{ a: 1 }, { a: 2 }])).to.reject();
        });
    });

    describe('verify()', () => {

        it('errors on create table error (insert)', async () => {

            const prep = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a: true,                 // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: 'a'
                    }
                }
            };

            await prep.establish(settings);
            await prep.close();

            const db = new Penseur.Db('penseurtest');
            await db.connect();
            db.table(settings);
            db.test._db._createTable = (options, callback) => callback(new Error('Failed'));
            const err = await expect(db.test.insert({ a: 1 })).to.reject();
            expect(err.data.error.message).to.equal('Failed creating unique table: penseur_unique_test_a');
        });

        it('errors on create table error (update)', async () => {

            const prep = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a: true,                 // Test cleanup
                test: {
                    id: 'uuid',
                    unique: {
                        path: 'a'
                    }
                }
            };

            await prep.establish(settings);
            const key = await prep.test.insert({ a: 1 });
            await prep.close();

            const db = new Penseur.Db('penseurtest');
            await db.connect();
            db.table(settings);
            db.test._db._createTable = () => Promise.reject(new Error('Failed'));
            const err = await expect(db.test.update(key, { a: 2 })).to.reject();
            expect(err.data.error.message).to.equal('Failed creating unique table: penseur_unique_test_a');
        });
    });
});
