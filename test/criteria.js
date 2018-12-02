'use strict';

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');


const internals = {};


const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Criteria', () => {

    it('parses empty criteria', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
        const result = await db.test.query({});
        expect(result.length).to.equal(3);
    });

    it('parses multiple keys', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: 2 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 1, b: 1 }]);
        const result = await db.test.query({ a: 1, b: 1 });
        expect(result).to.equal([{ id: 3, a: 1, b: 1 }]);
    });

    it('parses nested keys', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }]);
        const result = await db.test.query({ a: 1, b: { c: 1 } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: 1 } }]);
    });

    it('parses or', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }]);
        const result = await db.test.query({ a: 1, b: { c: db.or([1, 2]) } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: 1 } }, { id: 1, a: 1, b: { c: 2 } }]);
    });

    it('parses or with comparator', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 3 }]);
        const result = await db.test.query({ a: db.or([db.is('>=', 3), db.is('eq', 1)]) });
        expect(result).to.equal([{ id: 3, a: 3 }, { id: 1, a: 1 }]);
    });

    it('parses or unset', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, b: 1 }]);
        const result = await db.test.query({ a: db.or([2, db.unset()]) });
        expect(result).to.equal([{ id: 3, b: 1 }, { id: 2, a: 2 }]);
    });

    it('parses or unset nested', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 1, b: { d: 3 } }, { id: 3, a: 1, b: { c: 66 } }]);
        const result = await db.test.query({ a: 1, b: { c: db.or([66, db.unset()]) } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: 66 } }, { id: 2, a: 1, b: { d: 3 } }]);
    });

    it('parses not unset nested', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 1, b: { d: 3 } }, { id: 3, a: 1, b: { c: 66 } }]);
        const result = await db.test.query({ a: 1, b: { c: db.not([66, db.unset()]) } });
        expect(result).to.equal([{ id: 1, a: 1, b: { c: 2 } }]);
    });

    it('parses or empty', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: [] }, { id: 2, a: 1 }, { id: 3, a: [2] }]);
        const result = await db.test.query({ a: db.or([1, db.empty()]) });
        expect(result).to.equal([{ id: 2, a: 1 }, { id: 1, a: [] }]);
    });

    it('parses or empty nested', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1] } }, { id: 2, a: 1, b: { c: [] } }, { id: 3, a: 1, b: { c: 66 } }]);
        const result = await db.test.query({ a: 1, b: { c: db.or([66, db.empty()]) } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: 66 } }, { id: 2, a: 1, b: { c: [] } }]);
    });

    it('parses not empty nested', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1] } }, { id: 2, a: 1, b: { c: [] } }, { id: 3, a: 1, b: { c: 66 } }]);
        const result = await db.test.query({ a: 1, b: { c: db.not([66, db.empty()]) } });
        expect(result).to.equal([{ id: 1, a: 1, b: { c: [1] } }]);
    });

    it('parses or root', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);

        await db.test.insert([
            { id: 1, a: 1, b: { c: 2 } },
            { id: 2, a: 2, b: { c: 1 } },
            { id: 3, a: 3, b: { c: 3 } },
            { id: 4, a: 1 }
        ]);

        const result = await db.test.query(db.or([{ a: 1 }, { b: { c: 3 } }]));
        expect(result).to.equal([{ id: 4, a: 1 }, { id: 3, a: 3, b: { c: 3 } }, { id: 1, a: 1, b: { c: 2 } }]);
    });

    it('parses or objects', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);

        await db.test.insert([
            { id: 1, x: { a: 1, b: { c: 2 } } },
            { id: 2, x: { a: 2, b: { c: 1 } } },
            { id: 3, x: { a: 3, b: { c: 3 } } },
            { id: 4, x: { a: 1 } }
        ]);

        const result = await db.test.query({ x: db.or([{ a: 1 }, { b: { c: 3 } }]) });
        expect(result).to.equal([{ id: 4, x: { a: 1 } }, { id: 3, x: { a: 3, b: { c: 3 } } }, { id: 1, x: { a: 1, b: { c: 2 } } }]);
    });

    it('parses is', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);

        await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 3 }]);
        const result = await db.test.query({ a: db.is('<', 2) });
        expect(result).to.equal([{ id: 1, a: 1 }]);
    });

    it('parses is (multiple conditions)', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 3 }]);
        const result = await db.test.query({ a: db.is('>', 1, '<', 3) });
        expect(result).to.equal([{ id: 2, a: 2 }]);
    });

    it('parses contains', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }]);
        const result = await db.test.query({ a: 1, b: { c: db.contains(1) } });
        expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
    });

    it('parses contains or', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }]);
        const result = await db.test.query({ a: 1, b: { c: db.contains([1, 2], { condition: 'or' }) } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: [2, 3] } }, { id: 1, a: 1, b: { c: [1, 2] } }]);
    });

    it('parses contains and', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }]);
        const result = await db.test.query({ a: 1, b: { c: db.contains([1, 2], { condition: 'and' }) } });
        expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
    });

    it('parses contains and default', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }]);
        const result = await db.test.query({ a: 1, b: { c: db.contains([1, 2]) } });
        expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
    });

    it('parses contains key', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3] } }]);
        const result = await db.test.query({ a: 1, b: db.contains('c', { keys: true }) });
        expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
    });

    it('parses contains keys or', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3] } }]);
        const result = await db.test.query({ a: 1, b: db.contains(['c', 'e'], { keys: true, condition: 'or' }) });
        expect(result).to.equal([{ id: 3, a: 1, b: { e: [2, 3] } }, { id: 1, a: 1, b: { c: [1, 2] } }]);
    });

    it('parses contains keys and', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3], f: 'x' } }]);
        const result = await db.test.query({ a: 1, b: db.contains(['f', 'e'], { keys: true, condition: 'and' }) });
        expect(result).to.equal([{ id: 3, a: 1, b: { e: [2, 3], f: 'x' } }]);
    });

    it('parses unset key', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 1, b: { d: 3 } }, { id: 3, a: 1, b: { c: null } }]);
        const result = await db.test.query({ a: 1, b: { c: db.unset() } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: null } }, { id: 2, a: 1, b: { d: 3 } }]);
    });

    it('parses empty key', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: null }, { id: 2, a: 1, b: [2] }, { id: 3, a: 2, b: [] }, { id: 4, a: 1, b: 3 }, { id: 5, a: 1, b: { } }]);
        const result = await db.test.query({ a: 1, b: db.empty() });
        expect(result).to.equal([{ id: 5, a: 1, b: { } }]);
    });

    it('parses nested empty key for arrays', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: null } }, { id: 2, a: 1, b: { c: [2] } }, { id: 3, a: 1, b: { c: [] } }, { id: 4, a: 1, b: { c: 3 } }]);
        const result = await db.test.query({ a: 1, b: { c: db.empty() } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: [] } }]);
    });

    it('parses nested empty key for objects', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: { c: null } }, { id: 2, a: 1, b: { c: { d: 4 } } }, { id: 3, a: 1, b: { c: {} } }, { id: 4, a: 1, b: { c: 3 } }]);
        const result = await db.test.query({ a: 1, b: { c: db.empty() } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: {} } }]);
    });

    it('handles query into nested object where item is not an object', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 1, b: false }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }]);
        const result = await db.test.query({ a: 1, b: { c: 1 } });
        expect(result).to.equal([{ id: 3, a: 1, b: { c: 1 } }]);
    });

    it('handles query into double nested object where item is not an object', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);

        await db.test.insert([
            { id: 1, a: 1, b: false },
            { id: 2, a: 2, b: { c: { d: 4 } } },
            { id: 3, a: 1, b: { c: 1 } }
        ]);

        const result = await db.test.query({ b: { c: { d: 4 } } });
        expect(result).to.equal([{ id: 2, a: 2, b: { c: { d: 4 } } }]);
    });

    it('parses matches', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);
        await db.test.insert([{ id: 1, a: 'abcdef' }, { id: 2, a: 'acdef' }, { id: 3, a: 'bcdef' }, { id: 4, a: 'cdefg' }, { id: 5, a: 'ABC' }]);
        expect(await db.test.query({ a: db.match('b') })).to.equal([{ id: 3, a: 'bcdef' }, { id: 1, a: 'abcdef' }]);
        expect(await db.test.query({ a: db.match(['a', 'b']) })).to.equal([{ id: 1, a: 'abcdef' }]);
        expect(await db.test.query({ a: db.match(['a', 'b'], { condition: 'or' }) })).to.equal([{ id: 3, a: 'bcdef' }, { id: 2, a: 'acdef' }, { id: 1, a: 'abcdef' }]);
        expect(await db.test.query({ a: db.match('b', { start: true }) })).to.equal([{ id: 3, a: 'bcdef' }]);
        expect(await db.test.query({ a: db.match('G', { end: true, insensitive: true }) })).to.equal([{ id: 4, a: 'cdefg' }]);
        expect(await db.test.query({ a: db.match('abc', { exact: true, insensitive: true }) })).to.equal([{ id: 5, a: 'ABC' }]);
    });

    describe('select()', () => {

        it('selects by secondary index', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { secondary: 'a' } });
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const result = await db.test.query(db.by('a', 1));
            expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
        });
    });
});
