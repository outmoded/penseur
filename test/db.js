'use strict';

// Load modules

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


describe('Db', () => {

    it('establishes and interacts with a database', (done) => {

        const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert({ id: 1, value: 'x' }, (err, result) => {

                expect(err).to.not.exist();

                db.test.get(1, (err, item) => {

                    expect(err).to.not.exist();
                    expect(item.value).to.equal('x');
                    db.close(done);
                });
            });
        });
    });

    it('overrides table methods', (done) => {

        const Override = class extends Penseur.Table {
            insert(items, callback) {

                items = [].concat(items);
                for (let i = 0; i < items.length; ++i) {
                    items[i].flag = true;
                }

                return super.insert(items, callback);
            }
        };

        const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, extended: Override });

        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert({ id: 1, value: 'x' }, (err, result) => {

                expect(err).to.not.exist();

                db.test.get(1, (err, item) => {

                    expect(err).to.not.exist();
                    expect(item.value).to.equal('x');
                    expect(item.flag).to.equal(true);
                    db.close(done);
                });
            });
        });
    });

    describe('connect()', () => {

        it('uses default server location', (done) => {

            const db = new Penseur.Db('penseurtest');

            db.connect((err) => {

                expect(err).to.not.exist();
                db.close(done);
            });
        });

        it('fails connecting to missing server', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 0.001 });

            db.connect((err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });
    });

    describe('close()', () => {

        it('ignores unconnected state', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.close(done);
        });

        it('allows no callback', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.close();
            done();
        });
    });

    describe('table()', () => {

        it('skips decorating object when table name conflicts', (done) => {

            const db = new Penseur.Db('penseurtest');

            db.establish(['connect'], (err) => {

                expect(err).to.not.exist();
                expect(typeof db.connect).to.equal('function');
                expect(db.tables.connect).to.exist();
                db.close(done);
            });
        });

        it('skips decorating object when table already set up', (done) => {

            const db = new Penseur.Db('penseurtest');

            db.table('abc');
            db.establish(['abc'], (err) => {

                expect(err).to.not.exist();
                expect(db.tables.abc).to.exist();
                db.close(done);
            });
        });
    });

    describe('establish()', () => {

        it('creates new database', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.connect((err) => {

                expect(err).to.not.exist();

                RethinkDB.dbDrop(db._name).run(db._connection, (err, dropped) => {

                    expect(err).to.not.exist();

                    db.establish({ test: { index: 'other' } }, (err) => {

                        expect(err).to.not.exist();
                        RethinkDB.db(db._name).table('test').indexList().run(db._connection, (err, result) => {

                            expect(err).to.not.exist();
                            expect(result).to.deep.equal(['other']);
                            db.close(done);
                        });
                    });
                });
            });
        });

        it('customize table options', (done) => {

            const Override = class extends Penseur.Table {
                insert(items, callback) {

                    items = [].concat(items);
                    for (let i = 0; i < items.length; ++i) {
                        items[i].flag = true;
                    }

                    return super.insert(items, callback);
                }
            };

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            db.establish({ test: { extended: Override }, user: false, other: true }, (err) => {

                expect(err).to.not.exist();
                expect(db.user).to.not.exist();
                expect(db.other).to.exist();

                db.test.insert({ id: 1, value: 'x' }, (err, result) => {

                    expect(err).to.not.exist();

                    db.test.get(1, (err, item) => {

                        expect(err).to.not.exist();
                        expect(item.value).to.equal('x');
                        expect(item.flag).to.equal(true);
                        db.close(done);
                    });
                });
            });
        });

        it('creates database with different table indexes', (done) => {

            const db1 = new Penseur.Db('penseurtest');
            db1.connect((err) => {

                expect(err).to.not.exist();

                db1.establish({ test: { index: 'other' } }, (err) => {

                    expect(err).to.not.exist();

                    RethinkDB.db(db1._name).table('test').indexList().run(db1._connection, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.deep.equal(['other']);
                        db1.close(() => {

                            const db2 = new Penseur.Db('penseurtest');
                            db2.connect((err) => {

                                expect(err).to.not.exist();

                                db2.establish(['test'], (err) => {

                                    expect(err).to.not.exist();
                                    RethinkDB.db(db2._name).table('test').indexList().run(db2._connection, (err, result2) => {

                                        expect(err).to.not.exist();
                                        expect(result2).to.deep.equal([]);
                                        db2.close(done);
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        it('creates database with different table indexes (partial overlap)', (done) => {

            const db1 = new Penseur.Db('penseurtest');
            db1.connect((err) => {

                expect(err).to.not.exist();

                db1.establish({ test: { index: ['a', 'b'] } }, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db1._name).table('test').indexList().run(db1._connection, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.deep.equal(['a', 'b']);
                        db1.close(() => {

                            const db2 = new Penseur.Db('penseurtest');
                            db2.connect((err) => {

                                expect(err).to.not.exist();

                                db2.establish({ test: { index: ['b', 'c'] } }, (err) => {

                                    expect(err).to.not.exist();
                                    RethinkDB.db(db2._name).table('test').indexList().run(db2._connection, (err, result2) => {

                                        expect(err).to.not.exist();
                                        expect(result2).to.deep.equal(['b', 'c']);
                                        db2.close(done);
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        it('retains records in existing table', (done) => {

            const db1 = new Penseur.Db('penseurtest');
            db1.connect((err) => {

                expect(err).to.not.exist();

                db1.establish(['test'], (err) => {

                    expect(err).to.not.exist();
                    db1.test.insert({ id: 1 }, (err, id) => {

                        expect(err).to.not.exist();
                        db1.close(() => {

                            const db2 = new Penseur.Db('penseurtest');
                            db2.connect((err) => {

                                expect(err).to.not.exist();

                                db2.establish({ test: { purge: false } }, (err) => {

                                    expect(err).to.not.exist();
                                    db2.test.get(1, (err, item) => {

                                        expect(err).to.not.exist();
                                        expect(item.id).to.equal(1);
                                        db2.close(done);
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        it('fails creating a database', (done) => {

            const db = new Penseur.Db('penseur-test');

            db.establish(['test'], (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('fails connecting to missing server', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 0.001 });

            db.establish(['test'], (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('errors on database indexList() error', { parallel: false }, (done) => {

            const db = new Penseur.Db('penseurtest');
            const orig = RethinkDB.dbList;
            RethinkDB.dbList = function () {

                RethinkDB.dbList = orig;

                return {
                    run: function (connection, next) {

                        return next(new Error('Bad database'));
                    }
                };
            };

            db.establish(['test'], (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('errors on database dbList() error', { parallel: false }, (done) => {

            const db = new Penseur.Db('penseurtest');
            const orig = RethinkDB.dbList;
            RethinkDB.dbList = function () {

                RethinkDB.dbList = orig;

                return {
                    run: function (connection, next) {

                        return next(new Error('Bad database'));
                    }
                };
            };

            db.establish(['test'], (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('errors on database dbCreate() error', { parallel: false }, (done) => {

            const db = new Penseur.Db('penseur-test');
            db.connect((err) => {

                expect(err).to.not.exist();

                RethinkDB.dbDrop(db._name).run(db._connection, (err, dropped) => {

                    expect(err).to.exist();

                    const orig = RethinkDB.dbCreate;
                    RethinkDB.dbCreate = function () {

                        RethinkDB.dbCreate = orig;

                        return {
                            run: function (connection, next) {

                                return next(new Error('Bad database'));
                            }
                        };
                    };

                    db.establish(['test'], (err) => {

                        expect(err).to.exist();
                        db.close(done);
                    });
                });
            });
        });

        it('errors on database indexList() error', { parallel: false }, (done) => {

            const db = new Penseur.Db('penseurtest');
            const orig = RethinkDB.db;
            let count = 0;
            RethinkDB.db = function () {

                return {
                    tableList: function () {

                        return {
                            run: function (connection, next) {

                                return next(null, ['test']);
                            }
                        };
                    },
                    table: function () {

                        if (++count === 1) {
                            return orig('penseurtest').table('test');
                        }

                        RethinkDB.db = orig;
                        return {
                            indexList: function () {

                                return {
                                    run: function (connection, next) {

                                        return next(new Error('Bad database'));
                                    }
                                };
                            }
                        };
                    }
                };
            };

            db.establish(['test'], (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('errors creating new table', (done) => {

            const db = new Penseur.Db('penseurtest');

            db.establish(['bad name'], (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('errors emptying existing table', (done) => {

            const Override = class extends Penseur.Table {
                empty(callback) {

                    return callback(new Error('failed'));
                }
            };

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            db.establish({ test: { extended: Override } }, (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });
    });

    describe('_createTable', () => {

        it('errors on database dbList() error', { parallel: false }, (done) => {

            const db = new Penseur.Db('penseurtest');
            const orig = RethinkDB.db;
            RethinkDB.db = function () {

                RethinkDB.db = orig;

                return {
                    tableList: function () {

                        return {
                            run: function (connection, next) {

                                return next(new Error('Bad database'));
                            }
                        };
                    }
                };
            };

            db.establish(['test'], (err) => {

                expect(err).to.exist();
                db.close(done);
            });
        });
    });

    describe('disable()', () => {

        it('simulates an error', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, value: 'x' }, (err, result) => {

                    expect(err).to.not.exist();

                    db.test.get(1, (err, item1) => {

                        expect(err).to.not.exist();
                        expect(item1.value).to.equal('x');

                        db.disable('test', 'get');
                        db.test.get(1, (err, item2) => {

                            expect(err).to.exist();

                            db.enable('test', 'get');
                            db.test.get(1, (err, item3) => {

                                expect(err).to.not.exist();
                                expect(item3.value).to.equal('x');
                                db.close(done);
                            });
                        });
                    });
                });
            });
        });

        it('simulates a changes error', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.disable('test', 'changes');

                db.test.changes({ a: 1 }, Hoek.ignore, (err) => {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('simulates a changes update error', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.disable('test', 'changes', { updates: true });

                const each = (err, update) => {

                    expect(err).to.exist();
                    done();
                };

                db.test.changes({ a: 1 }, each, (err) => {

                    expect(err).to.not.exist();
                });
            });
        });
    });
});
