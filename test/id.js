'use strict';

// Load modules

const Code = require('code');
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


describe('Id', () => {

    describe('normalize()', () => {

        it('errors on empty array of ids', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.get([], (err, result) => {

                    expect(err).to.be.an.error('Empty array of ids not supported');
                    done();
                });
            });
        });

        it('errors on null id', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.get([null], (err, result) => {

                    expect(err).to.be.an.error('Invalid null or undefined id');
                    done();
                });
            });
        });

        it('errors on undefined id', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.get([undefined], (err, result) => {

                    expect(err).to.be.an.error('Invalid null or undefined id');
                    done();
                });
            });
        });
    });

    describe('wrap()', () => {

        it('generates keys locally and server-side', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ test: { id: { type: 'uuid' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 'abc', a: 1 }, { a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys[0]).to.equal('abc');
                    expect(keys[1]).to.match(/^[\da-f]{8}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{4}\-[\da-f]{12}$/);
                    done();
                });
            });
        });

        it('generates keys server-side', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ test: { id: { type: 'uuid' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 'abc', a: 1 }, { id: 'def', a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal(['abc', 'def']);
                    done();
                });
            });
        });
    });

    describe('uuid()', () => {

        it('generates keys locally', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ test: { id: { type: 'uuid' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys.length).to.equal(2);
                    done();
                });
            });
        });

        it('generates keys locally (implicit config)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ test: { id: 'uuid' } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys.length).to.equal(2);
                    done();
                });
            });
        });
    });

    describe('increment()', () => {

        it('generates key', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal('1');
                    done();
                });
            });
        });

        it('generates key (implicit config)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ penseur_id_allocate: true, test: { id: 'increment' } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal('1');
                    done();
                });
            });
        });

        it('generates keys (same table)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal(['1', '2']);
                    done();
                });
            });
        });

        it('generates key (different tables)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.connect((err) => {

                expect(err).to.not.exist();

                RethinkDB.dbDrop(db.name).run(db._connection, (err, dropped) => {

                    expect(err).to.not.exist();
                    db.establish({ test1: { id: { type: 'increment', table: 'allocate' } }, test2: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                        expect(err).to.not.exist();
                        db.test1.insert({ a: 1 }, (err, keys1) => {

                            expect(err).to.not.exist();
                            expect(keys1).to.equal('1');
                            db.test2.insert({ a: 1 }, (err, keys2) => {

                                expect(err).to.not.exist();
                                expect(keys2).to.equal('1');
                                db.close(done);
                            });
                        });
                    });
                });
            });
        });

        it('completes an existing incomplete allocation record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.update('test', { value: db.unset() }, (err) => {

                    expect(err).to.not.exist();
                    db.test._id.verified = false;
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(keys).to.equal('1');
                        done();
                    });
                });
            });
        });

        it('reuses generate record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.update('test', { value: 33 }, (err) => {

                    expect(err).to.not.exist();
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(keys).to.equal('34');
                        done();
                    });
                });
            });
        });

        it('generates base62 id', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate', radix: 62 } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.update('test', { value: 1324 }, (err) => {

                    expect(err).to.not.exist();
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(keys).to.equal('ln');
                        done();
                    });
                });
            });
        });

        it('customizes key generation', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate', initial: 1325, radix: 62, record: 'test-id', key: 'v' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal('ln');
                    done();
                });
            });
        });

        it('errors on invalid generate record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.update('test', { value: 'string' }, (err) => {

                    expect(err).to.not.exist();
                    db.test._id.verified = false;
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.exist();
                        expect(err.data.error.message).to.equal('Increment id record contains non-integer value: test');
                        done();
                    });
                });
            });
        });

        it('errors on create table error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test._db._createTable = (options, callback) => callback(new Error('Failed'));
                db.test._id.verified = false;
                db.test.insert({ a: 1 }, (err, keys) => {

                    expect(err).to.exist();
                    expect(err.data.error.message).to.equal('Failed creating increment id table: test');
                    done();
                });
            });
        });

        it('errors on table get error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test._id.table.get = (id, callback) => callback(new Error('Failed'));
                db.test._id.verified = false;
                db.test.insert({ a: 1 }, (err, keys) => {

                    expect(err).to.exist();
                    expect(err.data.error.message).to.equal('Failed verifying increment id record: test');
                    done();
                });
            });
        });

        it('errors on table update error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.update('test', { value: db.unset() }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test._id.table.update = (id, changes, callback) => callback(new Error('Failed'));
                    db.test._id.verified = false;
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.exist();
                        expect(err.data.error.message).to.equal('Failed initializing key-value pair to increment id record: test');
                        done();
                    });
                });
            });
        });

        it('errors on table insert error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.remove('test', (err) => {

                    expect(err).to.not.exist();
                    db.test._id.table.insert = (item, callback) => callback(new Error('Failed'));
                    db.test._id.verified = false;
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.exist();
                        expect(err.data.error.message).to.equal('Failed inserting increment id record: test');
                        done();
                    });
                });
            });
        });

        it('errors on table next error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { type: 'increment', table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test._id.table.next = (id, key, inc, callback) => callback(new Error('Failed'));
                db.test._id.verified = false;
                db.test.insert([{ a: 1 }, { a: 1 }], (err, keys) => {

                    expect(err).to.exist();
                    expect(err.data.error.message).to.equal('Failed allocating increment id: test');
                    done();
                });
            });
        });
    });
});
