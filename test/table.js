'use strict';

// Load modules

const Apiece = require('apiece');
const Code = require('code');
const Hoek = require('hoek');
const Lab = require('lab');
const Penseur = require('..');
const RethinkDB = require('rethinkdb');


// Declare internals

const internals = {};


// Test shortcuts

const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Table', () => {

    it('exposes table name', async () => {

        const db = new Penseur.Db('penseurtest');
        await db.establish(['test']);

            expect(err).to.not.exist();
            expect(db.test.name).to.equal('test');
            done();
        });
    });

    describe('get()', () => {

        it('returns the requested objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const item = await db.test.get([1, 3]);

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested object (zero id)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 0, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const item = await db.test.get([0]);

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 0, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();
                db.invalid.get(1);

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('returns the requested objects (array of one)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const item = await db.test.get([1]);

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (partial)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const item = await db.test.get([1, 3, 4]);

                        expect(err).to.not.exist();
                        expect(result).to.have.length(2);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (duplicates)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const item = await db.test.get([1, 3, 3]);

                        expect(err).to.not.exist();
                        expect(result).to.have.length(3);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (none)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const item = await db.test.get([4, 5, 6]);

                        expect(err).to.not.exist();
                        expect(result).to.equal(null);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (filter)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 2 }, { id: 3, a: 1, b: 3 }]);

                    expect(err).to.not.exist();

                    const item = await db.test.get([1, 3], { filter: ['id', 'b'] });

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 3, b: 3 }, { id: 1, b: 1 }]);
                        done();
                    });
                });
            });
        });

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            const item = await db.test.get('1', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
            });
        });

        it('errors on invalid id', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const item = await db.test.get('0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789');

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('errors on invalid ids', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const item = await db.test.get(['0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789']);

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('all()', () => {

        it('returns the requested objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.all((err, items) => {

                        expect(err).to.not.exist();
                        expect(items).to.equal([{ id: 3, a: 1 }, { id: 2, a: 2 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (range)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.all({ count: 2 }, (err, items) => {

                        expect(err).to.not.exist();
                        expect(items).to.equal([{ id: 1, a: 1 }, { id: 2, a: 2 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (from)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.all({ from: 1 }, (err, items) => {

                        expect(err).to.not.exist();
                        expect(items).to.equal([{ id: 2, a: 2 }, { id: 3, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns null when range is out of items', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                db.test.all((err, items1) => {

                    expect(err).to.not.exist();
                    expect(items1).to.be.null();

                    const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                        expect(err).to.not.exist();

                        db.test.all({ from: 5 }, (err, items2) => {

                            expect(err).to.not.exist();
                            expect(items2).to.be.null();
                            done();
                        });
                    });
                });
            });
        });

        it('breaks query into chunks', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const results = [];
                    const cb = Apiece.wrap({
                        end: (err) => {

                            expect(err).to.not.exist();
                            expect(results).to.equal([{ id: 1, a: 1 }, { id: 2, a: 1 }, { id: 3, a: 1 }]);
                            done();
                        },
                        each: (item) => results.push(item)
                    });

                    db.test.all({ chunks: 1 }, cb);
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();
                db.invalid.all((err, items) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('exist()', () => {

        it('checks if record exists', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

                    expect(err).to.not.exist();

                    db.test.exist(1, (err, exists) => {

                        expect(err).to.not.exist();
                        expect(exists).to.be.true();
                        done();
                    });
                });
            });
        });

        it('checks if record does not exists', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.exist(1, (err, exists) => {

                    expect(err).to.not.exist();
                    expect(exists).to.be.false();
                    done();
                });
            });
        });

        it('errors on invalid id', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                db.test.exist('0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789');

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('distinct()', () => {

        it('return distinct combinations (single field)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const items = [
                    { id: 1, a: 1, b: 2 },
                    { id: 2, a: 2, b: 2 },
                    { id: 3, a: 1, b: 2 },
                    { id: 4, a: 2, b: 2 },
                    { id: 5, a: 1, b: 3 },
                    { id: 6, a: 3, b: 3 },
                    { id: 7, a: 1, b: 2 }
                ];

                const keys = await db.test.insert(items);

                    expect(err).to.not.exist();

                    db.test.distinct(['a']);

                        expect(err).to.not.exist();
                        expect(result).to.equal([1, 2, 3]);
                        done();
                    });
                });
            });
        });

        it('return distinct combinations (single field with criteria)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const items = [
                    { id: 1, a: 1, b: 2 },
                    { id: 2, a: 2, b: 2 },
                    { id: 3, a: 1, b: 2 },
                    { id: 4, a: 2, b: 2 },
                    { id: 5, a: 1, b: 3 },
                    { id: 6, a: 3, b: 3 },
                    { id: 7, a: 1, b: 2 }
                ];

                const keys = await db.test.insert(items);

                    expect(err).to.not.exist();

                    db.test.distinct({ b: 2 }, 'a');

                        expect(err).to.not.exist();
                        expect(result).to.equal([1, 2]);
                        done();
                    });
                });
            });
        });

        it('return distinct combinations (combination fields)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const items = [
                    { id: 1, a: 1, b: 2 },
                    { id: 2, a: 2, b: 2 },
                    { id: 3, a: 1, b: 2 },
                    { id: 4, a: 2, b: 2 },
                    { id: 5, a: 1, b: 3 },
                    { id: 6, a: 3, b: 3 },
                    { id: 7, a: 1, b: 2 }
                ];

                const keys = await db.test.insert(items);

                    expect(err).to.not.exist();

                    db.test.distinct(['a', 'b']);

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ a: 1, b: 2 }, { a: 1, b: 3 }, { a: 2, b: 2 }, { a: 3, b: 3 }]);
                        done();
                    });
                });
            });
        });

        it('return null on no combinations', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.distinct(['a']);

                    expect(err).to.not.exist();
                    expect(result).to.null();
                    done();
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();
                db.invalid.distinct(['a']);

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('query()', () => {

        it('returns the requested objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const result = await db.test.query({ a: 1 });

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('sorts the requested objects (key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);

                    expect(err).to.not.exist();
                    const result = await db.test.query({ b: 1 }, {}, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);

                        const result = await db.test.query({ b: 1 }, { sort: 'a' }, (err, result2) => {

                            expect(err).to.not.exist();
                            expect(result2).to.equal([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);

                            const results = [];
                            const cb = Apiece.wrap({
                                end: (err) => {

                                    expect(err).to.not.exist();
                                    expect(results).to.equal([{ id: 1, a: 1, b: 1 }]);
                                    done();
                                },
                                each: (item) => results.push(item)
                            });

                            const result = await db.test.query({ b: 1 }, { count: 1, sort: { key: 'id', order: 'ascending' }, from: 0 }, cb);
                        });
                    });
                });
            });
        });

        it('sorts the requested objects (nested key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, x: { a: 1 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 3, x: { a: 3 }, b: 1 }]);

                    expect(err).to.not.exist();
                    const result = await db.test.query({ b: 1 }, {}, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 3, x: { a: 3 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 1, x: { a: 1 }, b: 1 }]);

                        const result = await db.test.query({ b: 1 }, { sort: ['x', 'a'] }, (err, result2) => {

                            expect(err).to.not.exist();
                            expect(result2).to.equal([{ id: 1, x: { a: 1 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 3, x: { a: 3 }, b: 1 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('sorts the requested objects (object)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);

                    expect(err).to.not.exist();
                    const result = await db.test.query({ b: 1 }, {}, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);

                        const result = await db.test.query({ b: 1 }, { sort: { key: 'a', order: 'descending' } }, (err, result2) => {

                            expect(err).to.not.exist();
                            expect(result2).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('includes results from a given position', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);

                    expect(err).to.not.exist();
                    const result = await db.test.query({ b: 1 }, { sort: 'a', from: 1 });

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
                        done();
                    });
                });
            });
        });

        it('includes n number of results', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);

                    expect(err).to.not.exist();
                    const result = await db.test.query({ b: 1 }, { sort: 'a', count: 2 }, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }]);
                        const result = await db.test.query({ b: 1 }, { sort: 'a', from: 1, count: 1 }, (err, result2) => {

                            expect(err).to.not.exist();
                            expect(result2).to.equal([{ id: 2, a: 2, b: 1 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('filters fields', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);

                    expect(err).to.not.exist();
                    const result = await db.test.query({ a: 1 }, { filter: ['b'] });

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ b: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns all objects', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const result = await db.test.query(null, (err, items) => {

                        expect(err).to.not.exist();
                        expect(items).to.equal([{ id: 3, a: 1 }, { id: 2, a: 2 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });
    });

    describe('_chunks()', () => {

        it('breaks query into chunks', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const results = [];
                    const cb = Apiece.wrap({
                        end: (err) => {

                            expect(err).to.not.exist();
                            expect(results).to.equal([{ id: 1, a: 1 }, { id: 2, a: 1 }, { id: 3, a: 1 }]);
                            done();
                        },
                        each: (item) => results.push(item)
                    });

                    const result = await db.test.query({ a: 1 }, { chunks: 1 }, cb);
                });
            });
        });

        it('breaks query into chunks (custom sort)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    const results = [];
                    const cb = Apiece.wrap({
                        end: (err) => {

                            expect(err).to.not.exist();
                            expect(results).to.equal([{ id: 3, a: 1 }, { id: 2, a: 2 }, { id: 1, a: 1 }]);
                            done();
                        },
                        each: (item) => results.push(item)
                    });

                    const result = await db.test.query(null, { chunks: 1, sort: { key: 'id', order: 'descending' } }, cb);
                });
            });
        });

        it('errors on chunks with count', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                expect(() => {

                    const result = await db.test.query(null, { chunks: 1, count: 1 }, Hoek.ignore);
                }).to.throw('Cannot use chunks option with from or count');

                done();
            });
        });

        it('errors on chunks with from', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                expect(() => {

                    const result = await db.test.query(null, { chunks: 1, from: 1 }, Hoek.ignore);
                }).to.throw('Cannot use chunks option with from or count');

                done();
            });
        });

        it('errors on databse error', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                db.test._run = (ignore1, ignore2, ignore3, next) => next(new Error());
                const result = await db.test.query(null, { chunks: 1 }, Apiece.wrap((err) => {

                    expect(err).to.exist();
                    done();
                }));
            });
        });
    });

    describe('single()', () => {

        it('returns the requested object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.single({ a: 2 });

                        expect(err).to.not.exist();
                        expect(result).to.equal({ id: 2, a: 2 });
                        done();
                    });
                });
            });
        });

        it('returns nothing', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.single({ a: 3 });

                        expect(err).to.not.exist();
                        expect(result).to.equal(null);
                        done();
                    });
                });
            });
        });

        it('errors on multiple matches', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.single({ a: 1 });

                        expect(err).to.exist();
                        expect(err.message).to.equal('Found multiple items');
                        done();
                    });
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();
                db.invalid.single({ a: 1 });

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('count()', () => {

        it('returns the number requested object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.count({ a: 1 });

                        expect(err).to.not.exist();
                        expect(result).to.equal(2);
                        done();
                    });
                });
            });
        });

        it('returns the number of object with given field', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.count(db.contains('a'));

                        expect(err).to.not.exist();
                        expect(result).to.equal(3);
                        done();
                    });
                });
            });
        });
    });

    describe('insert()', () => {

        it('inserts a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

                    expect(err).to.not.exist();
                    expect(keys).to.equal(1);

                    const item = await db.test.get(1);

                        expect(err).to.not.exist();
                        expect(item.a).to.equal(1);
                        done();
                    });
                });
            });
        });

        it('inserts multiple records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

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

                    expect(err).to.not.exist();
                    expect(keys).to.equal(ids);
                    expect(batches).to.equal([30, 30, 30, 10]);

                    db.test.all((err, items) => {

                        expect(err).to.not.exist();
                        expect(items.length).to.equal(100);
                        done();
                    });
                });
            });
        });

        it('inserts a record (ignore batch on non array)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const batches = [];
                const orig = db.test._insert;
                db.test._insert = (items, ...args) => {

                    batches.push(Array.isArray(items) ? items.length : 'single');
                    return orig.call(db.test, items, ...args);
                };

                const keys = await db.test.insert({ id: 1, a: 1 }, { chunks: 10 });

                    expect(err).to.not.exist();
                    expect(keys).to.equal(1);
                    expect(batches).to.equal(['single']);

                    const item = await db.test.get(1);

                        expect(err).to.not.exist();
                        expect(item.a).to.equal(1);
                        done();
                    });
                });
            });
        });

        it('inserts a record (ignore batch on solo array)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const batches = [];
                const orig = db.test._insert;
                db.test._insert = (items, ...args) => {

                    batches.push(Array.isArray(items) ? items.length : 'single');
                    return orig.call(db.test, items, ...args);
                };

                const keys = await db.test.insert([{ id: 1, a: 1 }], { chunks: 10 });

                    expect(err).to.not.exist();
                    expect(keys).to.equal([1]);
                    expect(batches).to.equal([1]);

                    const item = await db.test.get(1);

                        expect(err).to.not.exist();
                        expect(item.a).to.equal(1);
                        done();
                    });
                });
            });
        });

        it('updates a record if exists', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1, b: 1 }, { merge: true }, (err, keys1) => {

                    expect(err).to.not.exist();
                    const item = await db.test.get(1, (err, item1) => {

                        expect(err).to.not.exist();
                        expect(item1).to.equal({ id: 1, a: 1, b: 1 });

                        const keys = await db.test.insert({ id: 1, a: 2 }, { merge: true }, (err, keys2) => {

                            expect(err).to.not.exist();
                            const item = await db.test.get(1, (err, item2) => {

                                expect(err).to.not.exist();
                                expect(item2).to.equal({ id: 1, a: 2, b: 1 });
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('returns the generate key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ a: 1 });

                    expect(err).to.not.exist();
                    expect(keys).to.match(/\w+/);
                    done();
                });
            });
        });

        it('returns the generate key (existing)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 11, a: 1 });

                    expect(err).to.not.exist();
                    expect(keys).to.equal(11);
                    done();
                });
            });
        });

        it('generates key locally (uuid)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { id: { type: 'uuid' } } }, (err) => {

                expect(err).to.not.exist();
                const keys = await db.test.insert({ a: 1 });

                    expect(err).to.not.exist();
                    expect(keys).to.match(/^[\da-f]{8}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{12}$/);
                    done();
                });
            });
        });

        it('returns the generate keys', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ a: 1 }, { a: 2 }]);

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(2);
                    done();
                });
            });
        });

        it('returns the generate keys when keys are present', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { a: 2 }]);

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(2);
                    expect(keys[0]).to.equal(1);
                    done();
                });
            });
        });

        it('returns the generate keys when keys are present (last)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ a: 1 }, { id: 1, a: 2 }]);

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(2);
                    expect(keys[1]).to.equal(1);
                    done();
                });
            });
        });

        it('returns the generate keys when keys are present (mixed)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ a: 1 }, { id: 1, a: 2 }, { id: 2, a: 3 }, { a: 4 }, { a: 5 }, { id: 3, a: 6 }, { id: 4, a: 7 }]);

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(7);
                    expect(keys[1]).to.equal(1);
                    expect(keys[2]).to.equal(2);
                    expect(keys[5]).to.equal(3);
                    expect(keys[6]).to.equal(4);
                    done();
                });
            });
        });

        it('errors on key conflict', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 }, (err, key1) => {

                    expect(err).to.not.exist();
                    const keys = await db.test.insert({ id: 1, a: 1 }, (err, key2) => {

                        expect(err).to.exist();
                        done();
                    });
                });
            });
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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: '1', a: 1 }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test._unique.rules[0].table.remove = (ids, next) => next(new Error());
                    const keys = await db.test.insert({ id: '1', a: 2 }, { merge: true }, (err) => {

                        expect(err).to.exist();
                        done();
                    });
                });
            });
        });

        it('errors on batch insert with multiple records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test._insert = (items, options, next) => next(new Error());

                const records = [];
                for (let i = 1; i < 101; ++i) {
                    records.push({ id: i, a: i });
                }

                const keys = await db.test.insert(records, { chunks: 30 });

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('update()', () => {

        it('updates a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

                    expect(err).to.not.exist();

                    db.test.update(1, { a: 2 }, (err) => {

                        expect(err).to.not.exist();

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(2);
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record with empty object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

                    expect(err).to.not.exist();

                    db.test.update(1, { a: {} }, (err) => {

                        expect(err).to.not.exist();

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item.a).to.equal({});
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record with nested empty object', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1 });

                    expect(err).to.not.exist();

                    db.test.update(1, { a: { b: {} } }, (err) => {

                        expect(err).to.not.exist();

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item.a.b).to.equal({});
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (increment modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: 2
                    }
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();

                    const changes = {
                        a: 2,
                        b: {
                            c: db.increment(10)
                        }
                    };

                    expect(changes.b.c).to.be.a.function();

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        expect(changes.b.c).to.be.a.function();

                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({
                                id: 1,
                                a: 2,
                                b: {
                                    c: 12
                                }
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (append modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();

                    const changes = {
                        a: 2,
                        b: {
                            c: db.append(10)
                        }
                    };

                    expect(changes.b.c).to.be.a.function();

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        expect(changes.b.c).to.be.a.function();

                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({
                                id: 1,
                                a: 2,
                                b: {
                                    c: [2, 10]
                                }
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (append array modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();

                    const changes = {
                        a: 2,
                        b: {
                            c: db.append([10, 20])
                        }
                    };

                    expect(changes.b.c).to.be.a.function();

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        expect(changes.b.c).to.be.a.function();

                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({
                                id: 1,
                                a: 2,
                                b: {
                                    c: [2, 10, 20]
                                }
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (append array single modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();

                    const changes = {
                        a: 2,
                        b: {
                            c: db.append([10, 20], { single: true })
                        }
                    };

                    expect(changes.b.c).to.be.a.function();

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        expect(changes.b.c).to.be.a.function();

                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({
                                id: 1,
                                a: 2,
                                b: {
                                    c: [2, [10, 20]]
                                }
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (unset modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();

                    const changes = {
                        a: 2,
                        b: {
                            c: db.unset()
                        }
                    };

                    expect(changes.b.c).to.be.a.function();

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        expect(changes.b.c).to.be.a.function();

                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({
                                id: 1,
                                a: 2,
                                b: {}
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (only unset modifier)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();

                    const changes = {
                        a: db.unset()
                    };

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({ id: 1 });
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (no changes)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();
                    db.test.update(1, {}, (err) => {

                        expect(err).to.not.exist();
                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({ id: 1, a: 1 });
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (unset and append modifiers)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: [1],
                    b: {
                        c: [2]
                    }
                };

                const keys = await db.test.insert(item);

                    expect(err).to.not.exist();

                    const changes = {
                        a: db.append(2),
                        b: {
                            c: db.unset()
                        }
                    };

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        const item = await db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({
                                id: 1,
                                a: [1, 2],
                                b: {}
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (override key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1, b: { c: 1, d: 1 }, e: 1 });

                    expect(err).to.not.exist();
                    db.test.update(1, { a: 2, b: { d: 2 } }, (err) => {

                        expect(err).to.not.exist();
                        const item = await db.test.get(1, (err, item1) => {

                            expect(err).to.not.exist();
                            expect(item1).to.equal({ id: 1, a: 2, b: { c: 1, d: 2 }, e: 1 });

                            db.test.update(1, { a: 3, b: db.override({ c: 2 }) }, (err) => {

                                expect(err).to.not.exist();
                                const item = await db.test.get(1, (err, item2) => {

                                    expect(err).to.not.exist();
                                    expect(item2).to.equal({ id: 1, a: 3, b: { c: 2 }, e: 1 });
                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        it('updates multiple records (same value)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.update([1, 2], { a: db.unset() }, (err) => {

                        expect(err).to.not.exist();

                        db.test.all((err, items) => {

                            expect(err).to.not.exist();
                            expect(items).to.equal([{ id: 2 }, { id: 1 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('updates multiple records (different value)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.update([{ id: 1, a: 3 }, { id: 2, a: db.unset() }], (err) => {

                        expect(err).to.not.exist();

                        db.test.all((err, items) => {

                            expect(err).to.not.exist();
                            expect(items).to.equal([{ id: 2 }, { id: 1, a: 3 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (ignore batch)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }]);

                    expect(err).to.not.exist();

                    const batches = [];
                    const orig = db.test._update;
                    db.test._update = (ids, ...args) => {

                        batches.push(ids.length);
                        return orig.call(db.test, ids, ...args);
                    };

                    db.test.update([{ id: 1, a: 2 }], { chunks: 10 }, (err) => {

                        expect(err).to.not.exist();
                        expect(batches).to.equal([1]);

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(2);
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (ignore empty options)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }]);

                    expect(err).to.not.exist();

                    const batches = [];
                    const orig = db.test._update;
                    db.test._update = (ids, ...args) => {

                        batches.push(ids.length);
                        return orig.call(db.test, ids, ...args);
                    };

                    db.test.update([{ id: 1, a: 2 }], {}, (err) => {

                        expect(err).to.not.exist();
                        expect(batches).to.equal([1]);

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(2);
                            done();
                        });
                    });
                });
            });
        });

        it('updates multiple records (chunks)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const records = [];
                for (let i = 1; i < 101; ++i) {
                    records.push({ id: i, a: i });
                }

                const keys = await db.test.insert(records);

                    expect(err).to.not.exist();

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

                    db.test.update(updates, { chunks: 30 }, (err) => {

                        expect(err).to.not.exist();
                        expect(batches).to.equal([30, 30, 30, 10]);

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item.a).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (composite key)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: [1, 1], a: 1 });

                    expect(err).to.not.exist();

                    const item = await db.test.get({ id: [1, 1] }, (err, item1) => {

                        expect(err).to.not.exist();
                        expect(item1.a).to.equal(1);

                        db.test.update({ id: [1, 1] }, { a: 2 }, (err) => {

                            expect(err).to.not.exist();

                            const item = await db.test.get({ id: [1, 1] }, (err, item2) => {

                                expect(err).to.not.exist();
                                expect(item2.a).to.equal(2);
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('errors on unknown key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.update(2, { a: 2 }, (err) => {

                    expect(err).to.exist();
                    expect(err.data.error).to.equal('No document found');
                    done();
                });
            });
        });

        it('errors on invalid object key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.update({}, { a: 2 }, (err) => {

                    expect(err).to.exist();
                    expect(err.data.error).to.equal('Invalid object id');
                    done();
                });
            });
        });
    });

    describe('next()', () => {

        it('updates a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

                    expect(err).to.not.exist();

                    db.test.next(1, 'a', 5, (err) => {

                        expect(err).to.not.exist();

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(6);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.next(1, 'a', 5, (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('errors on invalid key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.next([1], 'a', 5, (err) => {

                    expect(err).to.be.an.error('Array of ids not supported');
                    done();
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();
                db.invalid.next(1, 'a', 5, (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('remove()', () => {

        it('removes a record', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

                    expect(err).to.not.exist();

                    db.test.remove(1, (err) => {

                        expect(err).to.not.exist();

                        const item = await db.test.get(1);

                            expect(err).to.not.exist();
                            expect(item).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });

        it('removes a record (composite id)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: [1, 1], a: 1 });

                    expect(err).to.not.exist();

                    db.test.remove({ id: [1, 1] }, (err) => {

                        expect(err).to.not.exist();

                        const item = await db.test.get({ id: [1, 1] });

                            expect(err).to.not.exist();
                            expect(item).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });

        it('removes multiple records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.remove([1, 2], (err) => {

                        expect(err).to.not.exist();

                        db.test.count({ a: 1 }, (err, count) => {

                            expect(err).to.not.exist();
                            expect(count).to.equal(0);
                            done();
                        });
                    });
                });
            });
        });

        it('removes records using criteria', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.remove({ a: 1 }, (err) => {

                        expect(err).to.not.exist();

                        db.test.count({ a: 1 }, (err, count) => {

                            expect(err).to.not.exist();
                            expect(count).to.equal(0);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.remove(1, (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('errors on invalid key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.remove([], (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('ignored error on unknown keys', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                db.test.remove([1], (err) => {

                    expect(err).to.not.exist();
                    done();
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();
                db.invalid.remove(1);

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('empty()', () => {

        it('removes all records', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.empty((err, count1) => {

                        expect(err).to.not.exist();
                        expect(count1).to.equal(2);

                        db.test.count({ a: 1 }, (err, count2) => {

                            expect(err).to.not.exist();
                            expect(count2).to.equal(0);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown table', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.connect();

                expect(err).to.not.exist();

                db.table('no_such_table_test');
                db.no_such_table_test.empty((err, count) => {

                    expect(err).to.exist();
                    expect(count).to.equal(0);
                    done();
                });
            });
        });
    });

    describe('sync()', () => {

        it('returns when write is complete', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);

                    expect(err).to.not.exist();

                    db.test.sync((err) => {

                        expect(err).to.not.exist();
                        done();
                    });
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();

                db.invalid.sync((err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            db.test.sync((err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
            });
        });
    });

    describe('index()', () => {

        it('adds a secondary index', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                db.test.index('x', (err) => {

                    expect(err).to.not.exist();

                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'x', multi: false, geo: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('fails on database error', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();

                db.invalid.index('x', (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            db.test.index('x', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
            });
        });

        it('creates simple index from a string', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                db.test.index('simple', (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'simple', multi: false, geo: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('creates index from function', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const index = {
                    name: 'name',
                    source: (row) => row('name')
                };

                db.test.index(index, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'name', multi: false, geo: false, ready: true });
                        expect(result[0].query).to.contain('("name")');
                        done();
                    });
                });
            });
        });

        it('creates compound index from an array of fields', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                db.test.index({ name: 'compound', source: ['some', 'other'] }, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'compound', multi: false, geo: false, ready: true });
                        expect(result[0].query).to.include('r.row("some")').and.to.include('r.row("other")');
                        done();
                    });
                });
            });
        });

        it('creates index with options', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                db.test.index({ name: 'simple-multi', options: { multi: true } }, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'simple-multi', multi: true, geo: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('creates multiple indexes from array', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const indexes = [
                    'simple',
                    { name: 'location', options: { geo: true } }
                ];

                db.test.index(indexes, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection);

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ geo: true, index: 'location', multi: false, ready: true });
                        expect(result[1]).to.contain({ geo: false, index: 'simple', multi: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('propogates errors from indexCreate', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const orig = db.test.raw.indexCreate;
                db.test.raw.indexCreate = () => {

                    return {
                        run(connection, options, callback) {

                            setImmediate(() => callback(new Error('simulated error')));
                        }
                    };
                };

                db.test.index('simple', (err) => {

                    db.test.raw.indexCreate = orig;
                    expect(err).to.be.an.error('simulated error');
                    expect(err.data.error.stack).to.exist();
                    expect(err.data.error.message).to.equal('simulated error');

                    done();
                });
            });
        });
    });

    describe('changes()', () => {

        it('fails on disconnected database', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            db.test.changes('*', Hoek.ignore, (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
            });
        });

        it('reports on a record update (*)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.changes('*', each, (err, cursor) => {

                    expect(err).to.not.exist();

                    const keys = await db.test.insert({ id: 1, a: 1 });

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err) => {

                            expect(err).to.not.exist();

                            expect(changes).to.equal([1, 1]);
                            await db.close();
                        });
                    });
                });
            });
        });

        it('manually closes a cursor', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.changes('*', each, (err, cursor) => {

                    expect(err).to.not.exist();

                    const keys = await db.test.insert({ id: 1, a: 1 });

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err) => {

                            expect(err).to.not.exist();

                            expect(changes).to.equal([1, 1]);
                            cursor.close();
                            await db.close();
                        });
                    });
                });
            });
        });

        it('reports on a record update (id)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes(1, each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            const keys = await db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1]);
                                await db.close();
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (ids)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes([1, 2], each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            const keys = await db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1, 2]);
                                await db.close();
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (query)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes({ a: 2 }, each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            const keys = await db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1, 2]);
                                await db.close();
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (delete)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.before.id + ':' + (item.after === null));
                };

                const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes(1, each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.remove(1, (err) => {

                            expect(err).to.not.exist();
                            expect(changes).to.equal(['1:true']);
                            await db.close();
                        });
                    });
                });
            });
        });

        it('reports on a record update (id missing)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.changes(1, each, (err, cursor) => {

                    expect(err).to.not.exist();

                    const keys = await db.test.insert({ id: 1, a: 1 });

                        expect(err).to.not.exist();
                        expect(changes).to.equal([1]);
                        await db.close();
                    });
                });
            });
        });

        it('includes initial state', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.id);
                };

                const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            const keys = await db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1, 1]);
                                await db.close();
                            });
                        });
                    });
                });
            });
        });

        it('handles initial state on missing initial item', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.id);
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        expect(err).to.not.exist();
                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();
                            const keys = await db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();
                                expect(changes).to.equal([1, 1]);
                                await db.close();
                            });
                        });
                    });
                });
            });
        });

        it('handles closed cursor while still processing rows', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const each = (err, item) => {

                    expect(err).to.not.exist();
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    await db.close();
                });
            });
        });

        it('reconnects', async () => {

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

                expect(err).to.not.exist();
                const changes = [];
                const each = (err, item) => {

                    if (!err) {
                        changes.push(item.type);

                        if (changes.length === 3) {

                            expect(changes).to.equal(['insert', { willReconnect: true, disconnected: true }, 'initial']);
                            expect(count).to.equal(2);
                            await db.close();
                        }
                    }
                    else {
                        changes.push(err.flags);
                    }
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        expect(err).to.not.exist();
                        step2 = () => {

                            step2 = null;
                            db.test.update(1, { a: 2 }, Hoek.ignore);
                        };

                        db._connection.close(Hoek.ignore);
                    });
                });
            });
        });

        it('does not reconnect on manual cursor close', async () => {

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

                expect(err).to.not.exist();
                const changes = [];
                const each = (err, item) => {

                    if (!err) {
                        changes.push(item.type);
                    }
                    else {
                        changes.push(err.flags);
                    }
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        expect(err).to.not.exist();
                        cursor.close();

                        step2 = () => {

                            step2 = null;
                            db.test.update(1, { a: 2 }, (err) => {

                                expect(err).to.not.exist();
                                db.test.update(1, { a: 2 }, (err) => {

                                    expect(err).to.not.exist();
                                    expect(changes).to.equal(['insert']);
                                    expect(count).to.equal(2);
                                    await db.close();
                                });
                            });
                        };

                        db._connection.close();
                    });
                });
            });
        });

        it('does not reconnect (feed reconnect disabled)', async () => {

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

                expect(err).to.not.exist();
                const changes = [];
                const each = (err, item) => {

                    if (!err) {
                        changes.push(item.type);
                    }
                    else {
                        changes.push(err.flags);
                    }
                };

                db.test.changes(1, { handler: each, initial: true, reconnect: false }, (err, cursor) => {

                    expect(err).to.not.exist();
                    const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        expect(err).to.not.exist();

                        step2 = () => {

                            step2 = null;
                            db.test.update(1, { a: 2 }, (err) => {

                                expect(err).to.not.exist();
                                db.test.update(1, { a: 2 }, (err) => {

                                    expect(err).to.not.exist();
                                    expect(changes).to.equal(['insert', { willReconnect: false, disconnected: true }]);
                                    expect(count).to.equal(2);
                                    await db.close();
                                });
                            });
                        };

                        db._connection.close();
                    });
                });
            });
        });

        it('does not reconnect (db reconnect disabled)', async () => {

            let count = 0;
            const onConnect = () => {

                ++count;
            };

            const db = new Penseur.Db('penseurtest', { onConnect, reconnect: false });
            await db.establish(['test']);

                expect(err).to.not.exist();
                const changes = [];
                const each = (err, item) => {

                    if (!err) {
                        changes.push(item.type);
                    }
                    else {
                        changes.push(err.flags);
                    }
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    const keys = await db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        db._connection.close(() => {

                            expect(err).to.not.exist();
                            await Hoek.wait(100);

                                expect(changes).to.equal(['insert', { willReconnect: false, disconnected: true }]);
                                expect(count).to.equal(1);
                                await db.close();
                        });
                    });
                });
            });
        });

        it('errors on bad cursor', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const each = (err, item) => {

                    if (err) {
                        expect(err.message).to.equal('kaboom');
                        done();
                    }
                };

                db.test.changes('*', each, (err, cursor) => {

                    expect(err).to.not.exist();

                    const orig = cursor._cursor._next;
                    cursor._cursor._next = () => {

                        cursor._cursor._next = orig;
                        throw new Error('kaboom');
                    };

                    const keys = await db.test.insert({ id: 1, a: 1 });

                        expect(err).to.not.exist();
                    });
                });
            });
        });

        it('errors on invalid table', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            await db.connect();

                expect(err).to.not.exist();
                db.invalid.changes('*', Hoek.ignore);

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });
});
