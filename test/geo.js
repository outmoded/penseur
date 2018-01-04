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


describe('Geo', () => {

    it('returns nearby items', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location', options: { geo: true } }] } });
        await db.test.insert([
            { id: 'shop', location: [-121.981434, 37.221310], a: 1 },
            { id: 'school', location: [-121.9643744, 37.2158098], a: 2 },
            { id: 'hospital', location: [-121.9570936, 37.2520443], a: 3 }
        ]);

        const result1 = await db.test.query({ location: db.near([-121.956064, 37.255768], 500) });
        expect(result1).to.equal([{ id: 'hospital', location: [-121.9570936, 37.2520443], a: 3 }]);

        const result2 = await db.test.query({ location: db.near([-121.956064, 37.255768], 10, 'mi') }, { filter: ['id'] });
        expect(result2).to.equal([{ id: 'hospital' }, { id: 'school' }, { id: 'shop' }]);

        const result3 = await db.test.query({ a: 1, location: db.near([-121.956064, 37.255768], 10, 'mi') }, { filter: ['id'] });
        expect(result3).to.equal([{ id: 'shop' }]);
    });

    it('converts geo point to plain value', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location', options: { geo: true } }] } });
        await db.test.insert({ id: 1, location: [-121.981434, 37.221310] });
        const result = await db.test.get(1);
        expect(result).to.equal({ id: 1, location: [-121.981434, 37.221310] });

        const raw = await RethinkDB.db(db.name).table('test').get(1).run(db._connection);
        expect(raw).to.equal({
            id: 1,
            location: {
                '$reql_type$': 'GEOMETRY',
                coordinates: [-121.981434, 37.22131],
                type: 'Point'
            }
        });
    });

    it('converts geo point to plain value (nested)', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location', source: ['x', 'pos'], options: { geo: true } }] } });
        await db.test.insert({ id: 1, x: { pos: [-121.981434, 37.221310] } });
        const result = await db.test.get(1);
        expect(result).to.equal({ id: 1, x: { pos: [-121.981434, 37.221310] } });

        const raw = await RethinkDB.db(db.name).table('test').get(1).run(db._connection);
        expect(raw).to.equal({
            id: 1,
            x: {
                pos: {
                    '$reql_type$': 'GEOMETRY',
                    coordinates: [-121.981434, 37.22131],
                    type: 'Point'
                }
            }
        });
    });

    it('reports on a record update (*)', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location', options: { geo: true } }] } });

        const changes = [];
        const each = (err, item) => {

            expect(err).to.not.exist();
            changes.push(item.after);
        };

        await db.test.changes('*', each);

        await db.test.insert({ id: 1, location: [-121.981434, 37.221310] });
        await db.test.update(1, { location: [-121.98, 37.22] });
        expect(changes).to.equal([
            { id: 1, location: [-121.981434, 37.22131] },
            { id: 1, location: [-121.98, 37.22] }
        ]);

        await db.close();
    });

    it('ignores geo conversion when table geo is false', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: false, secondary: [{ name: 'location', options: { geo: true } }] } });
        await db.test.insert({ id: 1, location: [-121.981434, 37.221310] });
        const result = await db.test.get(1);
        expect(result).to.equal({ id: 1, location: [-121.981434, 37.221310] });

        const raw = await RethinkDB.db(db.name).table('test').get(1).run(db._connection);
        expect(raw).to.equal({
            id: 1,
            location: [-121.981434, 37.221310]
        });
    });

    it('ignores geo conversion when no geo index is configured (options)', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location', options: {} }] } });
        await db.test.insert({ id: 1, location: [-121.981434, 37.221310] });
        const result = await db.test.get(1);
        expect(result).to.equal({ id: 1, location: [-121.981434, 37.221310] });

        const raw = await RethinkDB.db(db.name).table('test').get(1).run(db._connection);
        expect(raw).to.equal({
            id: 1,
            location: [-121.981434, 37.221310]
        });
    });

    it('ignores geo conversion when no geo index is configured (no options)', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: ['location'] } });
        await db.test.insert({ id: 1, location: [-121.981434, 37.221310] });
        const result = await db.test.get(1);
        expect(result).to.equal({ id: 1, location: [-121.981434, 37.221310] });

        const raw = await RethinkDB.db(db.name).table('test').get(1).run(db._connection);
        expect(raw).to.equal({
            id: 1,
            location: [-121.981434, 37.221310]
        });
    });

    it('ignores nested key on missing parent', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location', source: ['x', 'y', 'z', 'pos'], options: { geo: true } }] } });
        await db.test.insert({ id: 1, x: {} });
        const result = await db.test.get(1);
        expect(result).to.equal({ id: 1, x: {} });
    });

    it('ignores invalid location value', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location', options: { geo: true } }] } });
        await db.test.insert({ id: 1, location: 'x' });
        expect(await db.test.get(1)).to.equal({ id: 1, location: 'x' });
        expect(await db.test.query({ location: 'x' })).to.equal([{ id: 1, location: 'x' }]);
    });

    it('errors on multiple near criteria', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish({ test: { geo: true, secondary: [{ name: 'location1', options: { geo: true } }, { name: 'location2', options: { geo: true } }] } });
        await db.test.insert([{ id: 'shop', location1: [-121.981434, 37.221310], location2: [-121.981434, 37.221310] }]);

        await expect(db.test.query({ location1: db.near([-121.956064, 37.255768], 500), location2: db.near([-121.956064, 37.255768], 500) })).to.reject('Cannot specify more than one near condition');
    });
});
