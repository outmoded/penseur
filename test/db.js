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

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            db.establish({ test: { extended: Override } }, (err) => {

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

                    db.establish(['test'], (err) => {

                        expect(err).to.not.exist();
                        db.close(done);
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
    });
});
