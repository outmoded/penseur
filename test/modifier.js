'use strict';

// Load modules

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');


// Declare internals

const internals = {};


// Test shortcuts

const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Modifier', () => {

    describe('update()', () => {

        it('reuses nested fields objects', async () => {

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

                        const item = await db.test.get(1, (err, updated) => {

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

        it('shallow clone once', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();

                const item = {
                    id: 1,
                    a: 1,
                    b: {
                        c: 2,
                        d: 1
                    }
                };

                const keys = await db.test.insert(item);

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

                        const item = await db.test.get(1, (err, updated) => {

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

        it('unsets multiple keys', async () => {

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
                        a: db.unset(),
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
                                b: {}
                            });

                            done();
                        });
                    });
                });
            });
        });

        it('errors on invalid changes (number)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

                    expect(err).to.not.exist();

                    expect(() => {

                        db.test.update(1, 1, () => { });
                    }).to.throw('Invalid changes object');
                    done();
                });
            });
        });

        it('errors on invalid changes (null)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);

                expect(err).to.not.exist();
                const keys = await db.test.insert({ id: 1, a: 1 });

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
