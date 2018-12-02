'use strict';

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');

const Special = require('../lib/special');


const internals = {};


const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Modifier', () => {

    describe('update()', () => {

        it('reuses nested fields objects', async () => {

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
                },
                c: {
                    d: {
                        e: 'a'
                    }
                }
            };

            expect(changes.b.c).to.be.an.instanceof(Special);

            await db.test.update(1, changes);
            expect(changes.b.c).to.be.an.instanceof(Special);

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: 2,
                b: {
                    c: 12
                },
                c: {
                    d: {
                        e: 'a'
                    }
                }
            });
        });

        it('shallow clone once', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

            const item = {
                id: 1,
                a: 1,
                b: {
                    c: 2,
                    d: 1
                }
            };

            await db.test.insert(item);

            const changes = {
                a: 2,
                b: {
                    c: db.increment(10),
                    d: db.increment(10)
                }
            };

            expect(changes.b.c).to.be.an.instanceof(Special);

            await db.test.update(1, changes);
            expect(changes.b.c).to.be.an.instanceof(Special);

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                a: 2,
                b: {
                    c: 12,
                    d: 11
                }
            });
        });

        it('unsets multiple keys', async () => {

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
                a: db.unset(),
                b: {
                    c: db.unset()
                }
            };

            await db.test.update(1, changes);

            const updated = await db.test.get(1);
            expect(updated).to.equal({
                id: 1,
                b: {}
            });
        });

        it('errors on invalid changes (number)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });
            await expect(db.test.update(1, 1)).to.reject('Invalid changes object');
        });

        it('errors on invalid changes (null)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert({ id: 1, a: 1 });
            await expect(db.test.update(1, null)).to.reject('Invalid changes object');
        });
    });
});
