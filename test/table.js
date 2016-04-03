'use strict';

// Load modules

const Code = require('code');
const Hoek = require('hoek');
const Lab = require('lab');
const Penseur = require('..');


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

        it('returns the requested objects', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.get([1, 3], (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
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
                        expect(result).to.deep.equal([{ id: 1, a: 1 }]);
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

        it('fails on disconnected database', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table('test');
            db.test.get('1', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Database disconnected');
                done();
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
                        expect(result).to.deep.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (empty criteria)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({}, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result.length).to.equal(3);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (multiple keys)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: 2 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 1, b: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: 1 }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1, b: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (nested keys)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: { c: 1 } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1, b: { c: 1 } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (or)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: { c: db.or([1, 2]) } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1, b: { c: 1 } }, { id: 1, a: 1, b: { c: 2 } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (contains)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: { c: db.contains(1) } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (contains or)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: { c: db.contains([1, 2], { condition: 'or' }) } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1, b: { c: [2, 3] } }, { id: 1, a: 1, b: { c: [1, 2] } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (contains and)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: { c: db.contains([1, 2], { condition: 'and' }) } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (contains and default)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: { c: db.contains([1, 2]) } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (contains key)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3] } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: db.contains('c', { keys: true }) }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (contains keys or)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3] } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: db.contains(['c', 'e'], { keys: true, condition: 'or' }) }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1, b: { e: [2, 3] } }, { id: 1, a: 1, b: { c: [1, 2] } }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects (contains keys and)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3], f: 'x' } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: db.contains(['f', 'e'], { keys: true, condition: 'and' }) }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1, b: { e: [2, 3], f: 'x' } }]);
                        done();
                    });
                });
            });
        });

        it('handles query into nested object where item is not an object', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1, b: false }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1, b: { c: 1 } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 3, a: 1, b: { c: 1 } }]);
                        done();
                    });
                });
            });
        });

        it('handles query into double nested object where item is not an object', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([
                    { id: 1, a: 1, b: false },
                    { id: 2, a: 2, b: { c: { d: 4 } } },
                    { id: 3, a: 1, b: { c: 1 } }
                ], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test.query({ b: { c: { d: 4 } } }, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([{ id: 2, a: 2, b: { c: { d: 4 } } }]);
                        done();
                    });
                });
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
                        expect(result).to.deep.equal({ id: 2, a: 2 });
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
                        expect(err.message).to.equal('Database error');
                        done();
                    });
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

                    db.test.count(db.fields(['a']), (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal(3);
                        done();
                    });
                });
            });
        });
    });

    describe('insert()', () => {

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
                            expect(item.a).to.deep.equal({});
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
                            expect(item.a.b).to.deep.equal({});
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
                            expect(updated).to.deep.equal({
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
                            expect(updated).to.deep.equal({
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
                            expect(updated).to.deep.equal({
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
                            expect(updated).to.deep.equal({
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
                            expect(updated).to.deep.equal({
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

    describe('_run()', () => {

        it('errors on invalid cursor', { parallel: false }, (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();

                    db.test._table.filter({ a: 1 }).run(db._connection, (err, cursor) => {

                        expect(err).to.not.exist();

                        const proto = Object.getPrototypeOf(cursor);
                        const orig = proto.toArray;
                        proto.toArray = function (callback) {

                            proto.toArray = orig;
                            return callback(new Error('boom'));
                        };

                        cursor.close();

                        db.test.query({ a: 1 }, (err, result) => {

                            expect(err).to.exist();
                            done();
                        });
                    });
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

                            expect(changes).to.deep.equal([1, 1]);
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

                            expect(changes).to.deep.equal([1, 1]);
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

                                expect(changes).to.deep.equal([1]);
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

                                expect(changes).to.deep.equal([1, 2]);
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

                                expect(changes).to.deep.equal([1, 2]);
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
                            expect(changes).to.deep.equal(['1:true']);
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
                        expect(changes).to.deep.equal([1]);
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

                                expect(changes).to.deep.equal([1, 1]);
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
                db.test.changes('1', { handler: Hoek.ignore, initial: true }, (err, cursor) => {

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

            const db = new Penseur.Db('penseurtest', { onConnect: onConnect });
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                const changes = [];
                const each = (err, item) => {

                    if (!err) {
                        changes.push(item.type);

                        if (changes.length === 3) {

                            expect(changes).to.deep.equal(['insert', { willReconnect: true, disconnected: true }, 'initial']);
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

            const db = new Penseur.Db('penseurtest', { onConnect: onConnect });
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
                                    expect(changes).to.deep.equal(['insert']);
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

            const db = new Penseur.Db('penseurtest', { onConnect: onConnect });
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
                                    expect(changes).to.deep.equal(['insert', { willReconnect: false, disconnected: true }]);
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

            const db = new Penseur.Db('penseurtest', { onConnect: onConnect, reconnect: false });
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
                            expect(changes).to.deep.equal(['insert', { willReconnect: false, disconnected: true }]);
                            expect(count).to.equal(1);
                            db.close(done);
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
                        expect(err.message).to.equal('Database error');
                        done();
                    }
                };

                db.test.changes('*', each, (err, cursor) => {

                    expect(err).to.not.exist();

                    const orig = cursor._cursor._next;
                    cursor._cursor._next = (next) => {

                        cursor._cursor._next = orig;
                        return next(new Error('kaboom'));
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
});
