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

const lab = exports.lab = Lab.script();
const describe = lab.describe;
const it = lab.it;
const expect = Code.expect;


describe('Table', { parallel: false }, () => {

    it('exposes table name', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            expect(db.test.name).to.equal('test');
            done();
        });
    });

    describe('get()', () => {

        it('returns the requested objects', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.get([1, 3], (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('fails on database error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();
                db.invalid.get(1, (err, item) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('returns the requested objects (array of one)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.get([1], (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (partial)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.get([1, 3, 4], (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.have.length(2);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (duplicates)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.get([1, 3, 3], (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.have.length(3);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (none)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.get([4, 5, 6], (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal(null);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (filter)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 2 }, { id: 3, a: 1, b: 3 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.get([1, 3], { filter: ['id', 'b'] }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 3, b: 3 }, { id: 1, b: 1 }]);
                        done();
                    });
                });
            });
        });

        it('fails on disconnected database', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            db.test.get('1', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
            });
        });

        it('errors on invalid id', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.get('0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', (err, result) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('errors on invalid ids', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.get(['0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789'], (err, result) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('all()', () => {

        it('returns the requested objects', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.all((err, items) => {

                        expect(err).to.not.exist();
                        expect(items).to.equal([{ id: 3, a: 1 }, { id: 2, a: 2 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('fails on database error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();
                db.invalid.all((err, items) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('exist()', () => {

        it('checks if record exists', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.exist(1, (err, exists) => {

                        expect(err).to.not.exist();
                        expect(exists).to.be.true();
                        done();
                    });
                });
            });
        });

        it('checks if record does not exists', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                db.test.exist(1, (err, exists) => {

                    expect(err).to.not.exist();
                    expect(exists).to.be.false();
                    done();
                });
            });
        });

        it('errors on invalid id', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.exist('0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', (err, result) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('query()', () => {

        it('returns the requested objects', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1 }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('sorts the requested objects (key)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }], (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.query({ b: 1 }, {}, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);

                        db.test.query({ b: 1 }, { sort: 'a' }, (err, result2) => {

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

                            db.test.query({ b: 1 }, { count: 1, sort: { key: 'id', order: 'ascending' }, from: 0 }, cb);
                        });
                    });
                });
            });
        });

        it('sorts the requested objects (nested key)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, x: { a: 1 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 3, x: { a: 3 }, b: 1 }], (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.query({ b: 1 }, {}, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 3, x: { a: 3 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 1, x: { a: 1 }, b: 1 }]);

                        db.test.query({ b: 1 }, { sort: ['x', 'a'] }, (err, result2) => {

                            expect(err).to.not.exist();
                            expect(result2).to.equal([{ id: 1, x: { a: 1 }, b: 1 }, { id: 2, x: { a: 2 }, b: 1 }, { id: 3, x: { a: 3 }, b: 1 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('sorts the requested objects (object)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }], (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.query({ b: 1 }, {}, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);

                        db.test.query({ b: 1 }, { sort: { key: 'a', order: 'descending' } }, (err, result2) => {

                            expect(err).to.not.exist();
                            expect(result2).to.equal([{ id: 3, a: 3, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 1, a: 1, b: 1 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('includes results from a given position', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }], (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.query({ b: 1 }, { sort: 'a', from: 1 }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }]);
                        done();
                    });
                });
            });
        });

        it('includes n number of results', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }], (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.query({ b: 1 }, { sort: 'a', count: 2 }, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }]);
                        db.test.query({ b: 1 }, { sort: 'a', from: 1, count: 1 }, (err, result2) => {

                            expect(err).to.not.exist();
                            expect(result2).to.equal([{ id: 2, a: 2, b: 1 }]);
                            done();
                        });
                    });
                });
            });
        });

        it('filters fields', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: 1 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 3, b: 1 }], (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.query({ a: 1 }, { filter: ['b'] }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ b: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns all objects', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query(null, (err, items) => {

                        expect(err).to.not.exist();
                        expect(items).to.equal([{ id: 3, a: 1 }, { id: 2, a: 2 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });
    });

    describe('_chunks()', () => {

        it('breaks query into chunks', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }, { id: 3, a: 1 }], (err, keys) => {

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

                    db.test.query({ a: 1 }, { chunks: 1 }, cb);
                });
            });
        });

        it('breaks query into chunks (custom sort)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

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

                    db.test.query(null, { chunks: 1, sort: { key: 'id', order: 'descending' } }, cb);
                });
            });
        });

        it('errors on chunks with count', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                expect(() => {

                    db.test.query(null, { chunks: 1, count: 1 }, Hoek.ignore);
                }).to.throw('Cannot use chunks option with from or count');

                done();
            });
        });

        it('errors on chunks with from', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                expect(() => {

                    db.test.query(null, { chunks: 1, from: 1 }, Hoek.ignore);
                }).to.throw('Cannot use chunks option with from or count');

                done();
            });
        });

        it('errors on databse error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test._run = (ignore1, ignore2, ignore3, next) => next(new Error());
                db.test.query(null, { chunks: 1 }, Apiece.wrap((err) => {

                    expect(err).to.exist();
                    done();
                }));
            });
        });
    });

    describe('single()', () => {

        it('returns the requested object', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.single({ a: 2 }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal({ id: 2, a: 2 });
                        done();
                    });
                });
            });
        });

        it('returns nothing', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.single({ a: 3 }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal(null);
                        done();
                    });
                });
            });
        });

        it('errors on multiple matches', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.single({ a: 1 }, (err, result) => {

                        expect(err).to.exist();
                        expect(err.message).to.equal('Found multiple items');
                        done();
                    });
                });
            });
        });

        it('fails on database error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();
                db.invalid.single({ a: 1 }, (err, item) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('count()', () => {

        it('returns the number requested object', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.count({ a: 1 }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal(2);
                        done();
                    });
                });
            });
        });

        it('returns the number of object with given field', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.count(db.contains('a'), (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal(3);
                        done();
                    });
                });
            });
        });
    });

    describe('insert()', () => {

        it('updates a record if exists', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1, b: 1 }, { merge: true }, (err, keys1) => {

                    expect(err).to.not.exist();
                    db.test.get(1, (err, item1) => {

                        expect(err).to.not.exist();
                        expect(item1).to.equal({ id: 1, a: 1, b: 1 });

                        db.test.insert({ id: 1, a: 2 }, { merge: true }, (err, keys2) => {

                            expect(err).to.not.exist();
                            db.test.get(1, (err, item2) => {

                                expect(err).to.not.exist();
                                expect(item2).to.equal({ id: 1, a: 2, b: 1 });
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('returns the generate key', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.match(/\w+/);
                    done();
                });
            });
        });

        it('returns the generate key (existing)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 11, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal(11);
                    done();
                });
            });
        });

        it('generates key locally (uuid)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ test: { id: { type: 'uuid' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.match(/^[\da-f]{8}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{12}$/);
                    done();
                });
            });
        });

        it('returns the generate keys', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(2);
                    done();
                });
            });
        });

        it('returns the generate keys when keys are present', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(2);
                    expect(keys[0]).to.equal(1);
                    done();
                });
            });
        });

        it('returns the generate keys when keys are present (last)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { id: 1, a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(2);
                    expect(keys[1]).to.equal(1);
                    done();
                });
            });
        });

        it('returns the generate keys when keys are present (mixed)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { id: 1, a: 2 }, { id: 2, a: 3 }, { a: 4 }, { a: 5 }, { id: 3, a: 6 }, { id: 4, a: 7 }], (err, keys) => {

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

        it('errors on key conflict', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, key1) => {

                    expect(err).to.not.exist();
                    db.test.insert({ id: 1, a: 1 }, (err, key2) => {

                        expect(err).to.exist();
                        done();
                    });
                });
            });
        });
    });

    describe('update()', () => {

        it('updates a record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.update(1, { a: 2 }, (err) => {

                        expect(err).to.not.exist();

                        db.test.get(1, (err, item) => {

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(2);
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record with empty object', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.update(1, { a: {} }, (err) => {

                        expect(err).to.not.exist();

                        db.test.get(1, (err, item) => {

                            expect(err).to.not.exist();
                            expect(item.a).to.equal({});
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record with nested empty object', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.update(1, { a: { b: {} } }, (err) => {

                        expect(err).to.not.exist();

                        db.test.get(1, (err, item) => {

                            expect(err).to.not.exist();
                            expect(item.a.b).to.equal({});
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                db.test.update(2, { a: 2 }, (err) => {

                    expect(err).to.exist();
                    expect(err.data.error).to.equal('No document found');
                    done();
                });
            });
        });

        it('updates a record (increment modifier)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: 2
                    }
                };

                db.test.insert(item, (err, keys) => {

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

                        db.test.get(1, (err, updated) => {

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

        it('updates a record (append modifier)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                db.test.insert(item, (err, keys) => {

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

                        db.test.get(1, (err, updated) => {

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

        it('updates a record (append array modifier)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                db.test.insert(item, (err, keys) => {

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

                        db.test.get(1, (err, updated) => {

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

        it('updates a record (append array single modifier)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                db.test.insert(item, (err, keys) => {

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

                        db.test.get(1, (err, updated) => {

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

        it('updates a record (unset modifier)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: [2]
                    }
                };

                db.test.insert(item, (err, keys) => {

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

                        db.test.get(1, (err, updated) => {

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

        it('updates a record (only unset modifier)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1
                };

                db.test.insert(item, (err, keys) => {

                    expect(err).to.not.exist();

                    const changes = {
                        a: db.unset()
                    };

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({ id: 1 });
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (no changes)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1
                };

                db.test.insert(item, (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.update(1, {}, (err) => {

                        expect(err).to.not.exist();
                        db.test.get(1, (err, updated) => {

                            expect(err).to.not.exist();
                            expect(updated).to.equal({ id: 1, a: 1 });
                            done();
                        });
                    });
                });
            });
        });

        it('updates a record (unset and append modifiers)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: [1],
                    b: {
                        c: [2]
                    }
                };

                db.test.insert(item, (err, keys) => {

                    expect(err).to.not.exist();

                    const changes = {
                        a: db.append(2),
                        b: {
                            c: db.unset()
                        }
                    };

                    db.test.update(1, changes, (err) => {

                        expect(err).to.not.exist();
                        db.test.get(1, (err, updated) => {

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

        it('updates a record (override key)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1, b: { c: 1, d: 1 }, e: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.update(1, { a: 2, b: { d: 2 } }, (err) => {

                        expect(err).to.not.exist();
                        db.test.get(1, (err, item1) => {

                            expect(err).to.not.exist();
                            expect(item1).to.equal({ id: 1, a: 2, b: { c: 1, d: 2 }, e: 1 });

                            db.test.update(1, { a: 3, b: db.override({ c: 2 }) }, (err) => {

                                expect(err).to.not.exist();
                                db.test.get(1, (err, item2) => {

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
    });

    describe('next()', () => {

        it('updates a record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.next(1, 'a', 5, (err) => {

                        expect(err).to.not.exist();

                        db.test.get(1, (err, item) => {

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(6);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                db.test.next(1, 'a', 5, (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('fails on database error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();
                db.invalid.next(1, 'a', 5, (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('remove()', () => {

        it('removes a record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.remove(1, (err) => {

                        expect(err).to.not.exist();

                        db.test.get(1, (err, item) => {

                            expect(err).to.not.exist();
                            expect(item).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });

        it('removes multiple records', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }], (err, keys) => {

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

        it('removes records using criteria', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }], (err, keys) => {

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

        it('errors on unknown key', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                db.test.remove(1, (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('ignored error on unknown keys', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                db.test.remove([1], (err) => {

                    expect(err).to.not.exist();
                    done();
                });
            });
        });

        it('fails on database error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();
                db.invalid.remove(1, (err, item) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('empty()', () => {

        it('removes all records', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }], (err, keys) => {

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

        it('errors on unknown table', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.connect((err) => {

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

        it('returns when write is complete', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.sync((err) => {

                        expect(err).to.not.exist();
                        done();
                    });
                });
            });
        });

        it('fails on database error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();

                db.invalid.sync((err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('fails on disconnected database', (done) => {

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

        it('adds a secondary index', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.index('x', (err) => {

                    expect(err).to.not.exist();

                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'x', multi: false, geo: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('fails on database error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();

                db.invalid.index('x', (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('fails on disconnected database', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            db.test.index('x', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
            });
        });

        it('creates simple index from a string', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.index('simple', (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'simple', multi: false, geo: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('creates index from function', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const index = {
                    name: 'name',
                    source: (row) => row('name')
                };

                db.test.index(index, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'name', multi: false, geo: false, ready: true });
                        expect(result[0].query).to.contain('("name")');
                        done();
                    });
                });
            });
        });

        it('creates compound index from an array of fields', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.index({ name: 'compound', source: ['some', 'other'] }, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'compound', multi: false, geo: false, ready: true });
                        expect(result[0].query).to.include('r.row("some")').and.to.include('r.row("other")');
                        done();
                    });
                });
            });
        });

        it('creates index with options', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.index({ name: 'simple-multi', options: { multi: true } }, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ index: 'simple-multi', multi: true, geo: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('creates multiple indexes from array', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const indexes = [
                    'simple',
                    { name: 'location', options: { geo: true } }
                ];

                db.test.index(indexes, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db.name).table('test').indexStatus().run(db._connection, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result[0]).to.contain({ geo: true, index: 'location', multi: false, ready: true });
                        expect(result[1]).to.contain({ geo: false, index: 'simple', multi: false, ready: true });
                        done();
                    });
                });
            });
        });

        it('propogates errors from indexCreate', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

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

        it('fails on disconnected database', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            db.test.changes('*', Hoek.ignore, (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
            });
        });

        it('reports on a record update (*)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.changes('*', each, (err, cursor) => {

                    expect(err).to.not.exist();

                    db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err) => {

                            expect(err).to.not.exist();

                            expect(changes).to.equal([1, 1]);
                            db.close(done);
                        });
                    });
                });
            });
        });

        it('manually closes a cursor', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.changes('*', each, (err, cursor) => {

                    expect(err).to.not.exist();

                    db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err) => {

                            expect(err).to.not.exist();

                            expect(changes).to.equal([1, 1]);
                            cursor.close();
                            db.close(done);
                        });
                    });
                });
            });
        });

        it('reports on a record update (id)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes(1, each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1]);
                                db.close(done);
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (ids)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes([1, 2], each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1, 2]);
                                db.close(done);
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (query)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes({ a: 2 }, each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1, 2]);
                                db.close(done);
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (delete)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.before.id + ':' + (item.after === null));
                };

                db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes(1, each, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.remove(1, (err) => {

                            expect(err).to.not.exist();
                            expect(changes).to.equal(['1:true']);
                            db.close(done);
                        });
                    });
                });
            });
        });

        it('reports on a record update (id missing)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.after.id);
                };

                db.test.changes(1, each, (err, cursor) => {

                    expect(err).to.not.exist();

                    db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(changes).to.equal([1]);
                        db.close(done);
                    });
                });
            });
        });

        it('includes initial state', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.id);
                };

                db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                    expect(err).to.not.exist();

                    db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();

                            db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();

                                expect(changes).to.equal([1, 1]);
                                db.close(done);
                            });
                        });
                    });
                });
            });
        });

        it('handles initial state on missing initial item', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const changes = [];
                const each = (err, item) => {

                    expect(err).to.not.exist();
                    changes.push(item.id);
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        expect(err).to.not.exist();
                        db.test.update(1, { a: 2 }, (err, keys2) => {

                            expect(err).to.not.exist();
                            db.test.insert({ id: 2, a: 2 }, (err) => {

                                expect(err).to.not.exist();
                                expect(changes).to.equal([1, 1]);
                                db.close(done);
                            });
                        });
                    });
                });
            });
        });

        it('handles closed cursor while still processing rows', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const each = (err, item) => {

                    expect(err).to.not.exist();
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    db.close(done);
                });
            });
        });

        it('reconnects', (done) => {

            let step2 = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (step2) {
                    step2();
                }
            };

            const db = new Penseur.Db('penseurtest', { onConnect });
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                const changes = [];
                const each = (err, item) => {

                    if (!err) {
                        changes.push(item.type);

                        if (changes.length === 3) {

                            expect(changes).to.equal(['insert', { willReconnect: true, disconnected: true }, 'initial']);
                            expect(count).to.equal(2);
                            db.close(done);
                        }
                    }
                    else {
                        changes.push(err.flags);
                    }
                };

                db.test.changes(1, { handler: each, initial: true }, (err, cursor) => {

                    expect(err).to.not.exist();
                    db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

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

        it('does not reconnect on manual cursor close', (done) => {

            let step2 = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (step2) {
                    step2();
                }
            };

            const db = new Penseur.Db('penseurtest', { onConnect });
            db.establish(['test'], (err) => {

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
                    db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

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
                                    db.close(done);
                                });
                            });
                        };

                        db._connection.close();
                    });
                });
            });
        });

        it('does not reconnect (feed reconnect disabled)', (done) => {

            let step2 = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (step2) {
                    step2();
                }
            };

            const db = new Penseur.Db('penseurtest', { onConnect });
            db.establish(['test'], (err) => {

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
                    db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        expect(err).to.not.exist();

                        step2 = () => {

                            step2 = null;
                            db.test.update(1, { a: 2 }, (err) => {

                                expect(err).to.not.exist();
                                db.test.update(1, { a: 2 }, (err) => {

                                    expect(err).to.not.exist();
                                    expect(changes).to.equal(['insert', { willReconnect: false, disconnected: true }]);
                                    expect(count).to.equal(2);
                                    db.close(done);
                                });
                            });
                        };

                        db._connection.close();
                    });
                });
            });
        });

        it('does not reconnect (db reconnect disabled)', (done) => {

            let count = 0;
            const onConnect = () => {

                ++count;
            };

            const db = new Penseur.Db('penseurtest', { onConnect, reconnect: false });
            db.establish(['test'], (err) => {

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
                    db.test.insert([{ id: 1, a: 1 }], (err, keys1) => {

                        db._connection.close(() => {

                            expect(err).to.not.exist();
                            setTimeout(() => {

                                expect(changes).to.equal(['insert', { willReconnect: false, disconnected: true }]);
                                expect(count).to.equal(1);
                                db.close(done);
                            }, 100);
                        });
                    });
                });
            });
        });

        it('errors on bad cursor', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

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

                    db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                    });
                });
            });
        });

        it('errors on invalid table', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect((err) => {

                expect(err).to.not.exist();
                db.invalid.changes('*', Hoek.ignore, (err, item) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });
});
