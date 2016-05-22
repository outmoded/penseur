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


describe('Modifier', { parallel: false }, () => {

    describe('update()', () => {

        it('reuses nested fields objects', (done) => {

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
                        },
                        c: {
                            d: {
                                e: 'a'
                            }
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
                                },
                                c: {
                                    d: {
                                        e: 'a'
                                    }
                                }
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('shallow clone once', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: 2,
                        d: 1
                    }
                };

                db.test.insert(item, (err, keys) => {

                    expect(err).to.not.exist();

                    const changes = {
                        a: 2,
                        b: {
                            c: db.increment(10),
                            d: db.increment(10)
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
                                    c: 12,
                                    d: 11
                                }
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('unsets multiple keys', (done) => {

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
                        a: db.unset(),
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
                                b: {}
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('errors on invalid changes (number)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    expect(() => {

                        db.test.update(1, 1, () => { });
                    }).to.throw('Invalid changes object');
                    done();
                });
            });
        });

        it('errors on invalid changes (null)', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, (err, keys) => {

                    expect(err).to.not.exist();

                    expect(() => {

                        db.test.update(1, null, () => { });
                    }).to.throw('Invalid changes object');
                    done();
                });
            });
        });
    });
});
