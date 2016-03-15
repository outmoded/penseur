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


describe('Unique', () => {

    describe('reserve()', () => {

        it('allows setting a unique value', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys.length).to.equal(2);
                    done();
                });
            });
        });

        it('allows setting a unique value (nested)', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: { b: 1 } }, { id: 2, a: { c: { d: 2 } } }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys.length).to.equal(2);

                    db.test.update(2, { a: { b: 1 } }, (err) => {

                        expect(err).to.exist();
                        done();
                    });
                });
            });
        });

        it('allows updating a unique value', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: '1', a: 1 }, (err, key) => {

                    expect(err).to.not.exist();
                    db.test.update('1', { a: 2 }, (err) => {

                        expect(err).to.not.exist();
                        db.test.update('1', { a: 1 }, (err) => {

                            expect(err).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });

        it('forbids violating a unique value', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { a: 1 }], (err, keys) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('cusomizes unique table name', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys.length).to.equal(2);

                    db.unique_a.get([1, 2], (err, items) => {

                        expect(err).to.not.exist();
                        expect(items.length).to.equal(2);
                        done();
                    });
                });
            });
        });

        it('ignores non unique keys', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ b: 1 }, { b: 2 }], (err, keys) => {

                    expect(err).to.not.exist();
                    expect(keys.length).to.equal(2);
                    done();
                });
            });
        });

        it('ignore non-plain values', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: { b: 1 } }, (err, key) => {

                    expect(err).to.not.exist();
                    done();
                });
            });
        });

        it('ignore further nested values', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: { b: 1 } }, (err, key) => {

                    expect(err).to.not.exist();
                    done();
                });
            });
        });

        it('ignore further nested values (non existing)', (done) => {

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

            db.establish(settings, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ d: { b: 1 } }, (err, key) => {

                    expect(err).to.not.exist();
                    done();
                });
            });
        });
    });

    describe('verify()', () => {

        it('errors on create table error (insert)', (done) => {

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

            prep.establish(settings, (err) => {

                expect(err).to.not.exist();
                prep.close();

                const db = new Penseur.Db('penseurtest');
                db.connect((err) => {

                    expect(err).to.not.exist();
                    db.table(settings);
                    db.test._db._createTable = (options, callback) => callback(new Error('Failed'));
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.exist();
                        expect(err.data.error.message).to.equal('Failed creating unique table: penseur_unique_test_a');
                        done();
                    });
                });
            });
        });

        it('errors on create table error (update)', (done) => {

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

            prep.establish(settings, (err) => {

                expect(err).to.not.exist();
                prep.test.insert({ a: 1 }, (err, key) => {

                    expect(err).to.not.exist();
                    prep.close();

                    const db = new Penseur.Db('penseurtest');
                    db.connect((err) => {

                        expect(err).to.not.exist();
                        db.table(settings);
                        db.test._db._createTable = (options, callback) => callback(new Error('Failed'));
                        db.test.update(key, { a: 2 }, (err) => {

                            expect(err).to.exist();
                            expect(err.data.error.message).to.equal('Failed creating unique table: penseur_unique_test_a');
                            done();
                        });
                    });
                });
            });
        });
    });
});
