'use strict';

// Load modules

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');


// Declare internals

const internals = {};


// Test shortcuts

const lab = exports.lab = Lab.script();
const describe = lab.describe;
const it = lab.it;
const expect = Code.expect;


describe('Id', { parallel: false }, () => {

    describe('increment()', () => {

        it('generates key', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal('1');
                    done();
                });
            });
        });

        it('generates keys', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: db.id('increment'), a: 1 }, { id: db.id('increment'), a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.deep.equal(['1', '2']);
                    done();
                });
            });
        });

        it('updates generate record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.insert({ id: 'test' }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(keys).to.equal('1');
                        done();
                    });
                });
            });
        });

        it('reuses generate record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.insert({ id: 'test', value: 33 }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(keys).to.equal('34');
                        done();
                    });
                });
            });
        });

        it('generates base62 id', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate', radix: 62 } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.insert({ id: 'test', value: 1324 }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(keys).to.equal('ln');
                        done();
                    });
                });
            });
        });

        it('customizes key generation', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate', initial: 1325, radix: 62, record: 'test-id', key: 'v' } } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys).to.equal('ln');
                    done();
                });
            });
        });

        it('errors on invalid generate record', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.insert({ id: 'test', value: 'string' }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                        expect(err).to.exist();
                        expect(err.message).to.equal('Increment id record contains non-integer value: test');
                        done();
                    });
                });
            });
        });

        it('errors on increment id without table config', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Cannot allocated an incremented id on a table without id settings: test');
                    done();
                });
            });
        });

        it('errors on create table error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test._db._createTable = (options, callback) => callback(new Error('Failed'));
                db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Failed creating increment id table: test');
                    done();
                });
            });
        });

        it('errors on table get error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test._id.table.get = (id, callback) => callback(new Error('Failed'));
                db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Failed verifying increment id record: test');
                    done();
                });
            });
        });

        it('errors on table update error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.allocate.insert({ id: 'test' }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test._id.table.update = (id, changes, callback) => callback(new Error('Failed'));
                    db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                        expect(err).to.exist();
                        expect(err.message).to.equal('Failed initializing key-value pair to increment id record: test');
                        done();
                    });
                });
            });
        });

        it('errors on table insert error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test._id.table.insert = (item, callback) => callback(new Error('Failed'));
                db.test.insert({ id: db.id('increment'), a: 1 }, (err, keys) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Failed inserting increment id record: test');
                    done();
                });
            });
        });

        it('errors on table next error', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ allocate: true, test: { id: { table: 'allocate' } } }, (err) => {

                expect(err).to.not.exist();
                db.test._id.table.next = (id, key, inc, callback) => callback(new Error('Failed'));
                db.test.insert([{ id: db.id('increment'), a: 1 }, { id: db.id('increment'), a: 1 }], (err, keys) => {

                    expect(err).to.exist();
                    expect(err.message).to.equal('Failed allocating increment id: test');
                    done();
                });
            });
        });
    });
});
