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

    it('decorates static methods', (done) => {

        expect(Penseur.Db.or).to.exist();
        expect(Penseur.Db.contains).to.exist();
        expect(Penseur.Db.not).to.exist();
        expect(Penseur.Db.unset).to.exist();
        expect(Penseur.Db.append).to.exist();
        expect(Penseur.Db.increment).to.exist();
        done();
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

        it('reconnects automatically', (done) => {

            let count = 0;
            const onConnect = () => {

                ++count;
                if (count === 2) {
                    db.close(done);
                }
            };

            const onDisconnect = (willReconnect) => {

                expect(willReconnect).to.equal(count !== 2);
            };

            const db = new Penseur.Db('penseurtest', { onDisconnect, onConnect });

            db.connect((err) => {

                expect(err).to.not.exist();
                db._connection.close(Hoek.ignore);
            });
        });

        it('reports reconnect errors and tries again', (done) => {

            let orig = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (count === 2) {
                    db.close(done);
                }
            };

            const onDisconnect = (willReconnect) => {

                expect(willReconnect).to.equal(count !== 2);
            };

            const onError = (err) => {

                expect(err).to.exist();
                db.connect = orig;
            };

            const db = new Penseur.Db('penseurtest', { onDisconnect, onConnect, onError });
            orig = db.connect;

            db.connect((err) => {

                expect(err).to.not.exist();
                db.connect = (callback) => callback(new Error('failed to connect'));
                db._connection.close(Hoek.ignore);
            });
        });

        it('waits between reconnections', (done) => {

            const timer = new Hoek.Bench();

            let orig = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (count === 2) {
                    expect(timer.elapsed()).to.be.above(200);
                    db.close(done);
                }
            };

            const onDisconnect = (willReconnect) => {

                expect(willReconnect).to.equal(count !== 2);
            };

            let errors = 0;
            const onError = (err) => {

                ++errors;
                expect(err).to.exist();
                if (errors === 2) {
                    db.connect = orig;
                }
            };

            const db = new Penseur.Db('penseurtest', { onDisconnect, onConnect, onError, reconnectTimeout: 100 });
            orig = db.connect;

            db.connect((err) => {

                expect(err).to.not.exist();
                db.connect = (callback) => callback(new Error('failed to connect'));
                timer.reset();
                db._connection.close(Hoek.ignore);
            });
        });

        it('reconnects immediately', (done) => {

            const timer = new Hoek.Bench();

            let orig = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                if (count === 2) {
                    expect(timer.elapsed()).to.be.below(100);
                    db.close(done);
                }
            };

            const onDisconnect = (willReconnect) => {

                expect(willReconnect).to.equal(count !== 2);
            };

            let errors = 0;
            const onError = (err) => {

                ++errors;
                expect(err).to.exist();
                if (errors === 2) {
                    db.connect = orig;
                }
            };

            const db = new Penseur.Db('penseurtest', { onDisconnect, onConnect, onError, reconnectTimeout: false });
            orig = db.connect;

            db.connect((err) => {

                expect(err).to.not.exist();
                db.connect = (callback) => callback(new Error('failed to connect'));
                timer.reset();
                db._connection.close(Hoek.ignore);
            });
        });

        it('does not reconnect automatically', (done) => {

            const onDisconnect = (willReconnect) => {

                expect(willReconnect).to.be.false();
                done();
            };

            const db = new Penseur.Db('penseurtest', { onDisconnect, reconnect: false });

            db.connect((err) => {

                expect(err).to.not.exist();
                db._connection.close(Hoek.ignore);
            });
        });

        it('notifies of errors', (done) => {

            const onError = (err) => {

                expect(err.message).to.equal('boom');
                db.close(done);
            };

            const db = new Penseur.Db('penseurtest', { onError });

            db.connect((err) => {

                expect(err).to.not.exist();
                db._connection.emit('error', new Error('boom'));
            });
        });

        it('notifies of timeout', (done) => {

            const onError = (err) => {

                expect(err.message).to.equal('Database connection timeout');
                db.close(done);
            };

            const db = new Penseur.Db('penseurtest', { onError });

            db.connect((err) => {

                expect(err).to.not.exist();
                db._connection.emit('timeout');
            });
        });

        it('prepares generate id table', (done) => {

            const prep = new Penseur.Db('penseurtest');
            prep.establish(['allocate', 'test'], (err) => {         // Cleanup

                expect(err).to.not.exist();
                prep.close();

                const db = new Penseur.Db('penseurtest');
                db.table('test', { id: { type: 'increment', table: 'allocate' } });
                db.connect((err) => {

                    expect(err).to.not.exist();
                    db.test.insert({ a: 1 }, (err, keys) => {

                        expect(err).to.not.exist();
                        expect(keys).to.equal('1');
                        done();
                    });
                });
            });
        });

        it('errors on missing database', (done) => {

            const db = new Penseur.Db('penseurtest_no_such_db');

            db.connect((err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Missing database: penseurtest_no_such_db');
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

        it('decorates an array of tables', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.table(['test1', 'test2']);
            expect(db.tables.test1).to.exist();
            expect(db.tables.test2).to.exist();
            db.close(done);
        });
    });

    describe('establish()', () => {

        it('creates new database', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.connect((err) => {

                expect(err).to.not.exist();

                RethinkDB.dbDrop(db.name).run(db._connection, (err, dropped) => {

                    expect(err).to.not.exist();

                    db.establish({ test: { secondary: 'other' } }, (err) => {

                        expect(err).to.not.exist();
                        RethinkDB.db(db.name).table('test').indexList().run(db._connection, (err, result) => {

                            expect(err).to.not.exist();
                            expect(result).to.equal(['other']);
                            db.close(done);
                        });
                    });
                });
            });
        });

        it('creates new database (complex tables pre-loaded)', (done) => {

            const prep = new Penseur.Db('penseurtest');
            prep.connect((err) => {

                expect(err).to.not.exist();
                RethinkDB.dbDrop('penseurtest').run(prep._connection, (err, dropped) => {

                    expect(err).to.not.exist();
                    prep.close();

                    const db = new Penseur.Db('penseurtest');
                    db.table({ test: { id: 'increment' } });

                    db.establish(['test'], (err) => {

                        expect(err).to.not.exist();
                        db.close(done);
                    });
                });
            });
        });

        it('customizes table options', (done) => {

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

        it('customizes table options before establish', (done) => {

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
            db.table('test', { extended: Override });
            db.establish('test', (err) => {

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

        it('creates database with different table indexes', (done) => {

            const db1 = new Penseur.Db('penseurtest');
            db1.connect((err) => {

                expect(err).to.not.exist();

                db1.establish({ test: { secondary: 'other' } }, (err) => {

                    expect(err).to.not.exist();

                    RethinkDB.db(db1.name).table('test').indexList().run(db1._connection, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal(['other']);
                        db1.close(() => {

                            const db2 = new Penseur.Db('penseurtest');
                            db2.connect((err) => {

                                expect(err).to.not.exist();

                                db2.establish(['test'], (err) => {

                                    expect(err).to.not.exist();
                                    RethinkDB.db(db2.name).table('test').indexList().run(db2._connection, (err, result2) => {

                                        expect(err).to.not.exist();
                                        expect(result2).to.equal([]);
                                        db2.close(done);
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        it('creates database and retains table indexes', (done) => {

            const db1 = new Penseur.Db('penseurtest');
            db1.connect((err) => {

                expect(err).to.not.exist();

                db1.establish({ test: { secondary: 'other' } }, (err) => {

                    expect(err).to.not.exist();

                    RethinkDB.db(db1.name).table('test').indexList().run(db1._connection, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal(['other']);
                        db1.close(() => {

                            const db2 = new Penseur.Db('penseurtest');
                            db2.connect((err) => {

                                expect(err).to.not.exist();

                                db2.establish({ test: { secondary: false } }, (err) => {

                                    expect(err).to.not.exist();
                                    RethinkDB.db(db2.name).table('test').indexList().run(db2._connection, (err, result2) => {

                                        expect(err).to.not.exist();
                                        expect(result2).to.equal(['other']);
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

                db1.establish({ test: { secondary: ['a', 'b'] } }, (err) => {

                    expect(err).to.not.exist();
                    RethinkDB.db(db1.name).table('test').indexList().run(db1._connection, (err, result1) => {

                        expect(err).to.not.exist();
                        expect(result1).to.equal(['a', 'b']);
                        db1.close(() => {

                            const db2 = new Penseur.Db('penseurtest');
                            db2.connect((err) => {

                                expect(err).to.not.exist();

                                db2.establish({ test: { secondary: ['b', 'c'] } }, (err) => {

                                    expect(err).to.not.exist();
                                    RethinkDB.db(db2.name).table('test').indexList().run(db2._connection, (err, result2) => {

                                        expect(err).to.not.exist();
                                        expect(result2).to.equal(['b', 'c']);
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

    describe('_createTable()', () => {

        it('creates table with custom primary key', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ test: { primary: 'other', id: 'uuid' } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert({ a: 1 }, (err, id) => {

                    expect(err).to.not.exist();
                    db.test.get(id, (err, item) => {

                        expect(err).to.not.exist();
                        expect(item).to.equal({ other: id, a: 1 });
                        db.close(done);
                    });
                });
            });
        });

        it('errors on database tableList() error', { parallel: false }, (done) => {

            const db = new Penseur.Db('penseurtest');
            const orig = RethinkDB.db;
            let count = 0;

            RethinkDB.db = function (name) {

                ++count;
                if (count === 1) {
                    return orig(name);
                }

                RethinkDB.db = orig;

                return {
                    tableList: function () {

                        return {
                            map: function () {

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
    });

    describe('_verify()', () => {

        it('errors on create table error (id)', (done) => {

            const prep = new Penseur.Db('penseurtest');
            const settings = {
                penseur_unique_test_a: true,                 // Test cleanup
                test: {
                    id: { type: 'increment', table: 'allocate' },
                    unique: {
                        path: 'a'
                    }
                }
            };

            prep.establish(settings, (err) => {

                expect(err).to.not.exist();
                prep.close();

                const db = new Penseur.Db('penseurtest');
                db.table(settings);

                db.test._db._createTable = (options, callback) => callback(new Error('Failed'));
                db.connect((err) => {

                    expect(err).to.exist();
                    db.close(done);
                });
            });
        });

        it('errors on create table error (unique)', (done) => {

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
                db.table(settings);

                db.test._db._createTable = (options, callback) => callback(new Error('Failed'));
                db.connect((err) => {

                    expect(err).to.exist();
                    db.close(done);
                });
            });
        });
    });

    describe('run()', () => {

        it('makes custom request', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.run(db.test.raw.insert({ id: 1, value: 'x' }), (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal({
                        deleted: 0,
                        errors: 0,
                        inserted: 1,
                        replaced: 0,
                        skipped: 0,
                        unchanged: 0
                    });

                    db.run(db.test.raw.get(1).pluck('id'), (err, item) => {

                        expect(err).to.not.exist();
                        expect(item).to.equal({ id: 1 });

                        db.run(db.test.raw, (err, all) => {

                            expect(err).to.not.exist();
                            expect(all).to.equal([{ id: 1, value: 'x' }]);
                            db.close(done);
                        });
                    });
                });
            });
        });

        it('includes table name on error', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });
            db.table('test123');
            db.run(db.test123.raw.insert({ id: 1, value: 'x' }), (err, result) => {

                expect(err).to.be.an.error('Database disconnected');
                expect(err.data.table).to.equal('test123');

                db.run(db.test123.raw.get(1).pluck('id'), (err, item) => {

                    expect(err).to.be.an.error('Database disconnected');
                    expect(err.data.table).to.equal('test123');

                    db.run(db.test123.raw, (err, all) => {

                        expect(err).to.be.an.error('Database disconnected');
                        expect(err.data.table).to.equal('test123');
                        done();
                    });
                });
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

        it('simulates a response', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, value: 'x' }, (err, result) => {

                    expect(err).to.not.exist();

                    db.test.get(1, (err, item1) => {

                        expect(err).to.not.exist();
                        expect(item1.value).to.equal('x');

                        db.disable('test', 'get', { value: 'hello' });
                        db.test.get(1, (err, item2) => {

                            expect(err).to.not.exist();
                            expect(item2).to.equal('hello');

                            db.disable('test', 'get', { value: null });
                            db.test.get(1, (err, item3) => {

                                expect(err).to.not.exist();
                                expect(item3).to.be.null();

                                db.disable('test', 'get', { value: new Error('stuff') });
                                db.test.get(1, (err, item4) => {

                                    expect(err).to.be.an.error('stuff');
                                    expect(item4).to.not.exist();

                                    db.enable('test', 'get');
                                    db.test.get(1, (err, item5) => {

                                        expect(err).to.not.exist();
                                        expect(item5.value).to.equal('x');
                                        db.close(done);
                                    });
                                });
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
                    expect(err.flags.willReconnect).to.be.true();
                    done();
                };

                db.test.changes({ a: 1 }, each, (err) => {

                    expect(err).to.not.exist();
                });
            });
        });

        it('simulates a changes update error (flags)', (done) => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

            db.establish(['test'], (err) => {

                expect(err).to.not.exist();
                db.disable('test', 'changes', { updates: true, flags: { willReconnect: false } });

                const each = (err, update) => {

                    expect(err).to.exist();
                    expect(err.flags.willReconnect).to.be.false();
                    done();
                };

                db.test.changes({ a: 1 }, each, (err) => {

                    expect(err).to.not.exist();
                });
            });
        });
    });

    describe('is()', () => {

        it('errors on invalid number of arguments (3)', (done) => {

            expect(() => {

                Penseur.Db.is('=', 5, '<');
            }).to.throw('Cannot have odd number of arguments');

            done();
        });

        it('errors on invalid number of arguments (1)', (done) => {

            expect(() => {

                Penseur.Db.is('=');
            }).to.throw('Missing value argument');

            done();
        });
    });
});
