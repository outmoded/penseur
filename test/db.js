'use strict';

const Code = require('code');
const Hoek = require('hoek');
const Lab = require('lab');
const Penseur = require('..');
const RethinkDB = require('rethinkdb');
const Teamwork = require('teamwork');


const internals = {};


const { describe, it } = exports.lab = Lab.script();
const expect = Code.expect;


describe('Db', () => {

    it('exposes driver', () => {

        const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });
        expect(db.r).to.shallow.equal(RethinkDB);
        expect(Penseur.r).to.shallow.equal(RethinkDB);
    });

    it('establishes and interacts with a database', async () => {

        const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

        await db.establish(['test']);
        await db.test.insert({ id: 1, value: 'x' });
        const item = await db.test.get(1);
        expect(item.value).to.equal('x');
        await db.close();
    });

    it('overrides table methods', async () => {

        const Override = class extends Penseur.Table {

            insert(items) {

                items = [].concat(items);
                for (let i = 0; i < items.length; ++i) {
                    items[i].flag = true;
                }

                return super.insert(items);
            }
        };

        const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, extended: Override });

        await db.establish(['test']);
        await db.test.insert({ id: 1, value: 'x' });
        const item = await db.test.get(1);
        expect(item.value).to.equal('x');
        expect(item.flag).to.equal(true);
        await db.close();
    });

    it('decorates static methods', () => {

        expect(Penseur.Db.or).to.exist();
        expect(Penseur.Db.contains).to.exist();
        expect(Penseur.Db.not).to.exist();
        expect(Penseur.Db.unset).to.exist();
        expect(Penseur.Db.append).to.exist();
        expect(Penseur.Db.increment).to.exist();
    });

    describe('connect()', () => {

        it('uses default server location', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.connect();
            await db.close();
        });

        it('fails connecting to missing server', async () => {

            const db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 0.001 });
            await expect(db.connect()).to.reject();
            await db.close();
        });

        it('reconnects automatically', async () => {

            const team = new Teamwork({ meetings: 2 });
            let count = 0;
            const onConnect = () => {

                ++count;
                team.attend();
            };

            const onDisconnect = (willReconnect) => {

                expect(willReconnect).to.equal(count !== 2);
            };

            const db = new Penseur.Db('penseurtest', { onDisconnect, onConnect });

            await db.connect();
            db._connection.close();
            await team.work;
            await db.close();
        });

        it('reports reconnect errors and tries again', async () => {

            const team = new Teamwork({ meetings: 2 });

            let orig = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                team.attend();
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

            await db.connect();
            db.connect = () => Promise.reject(new Error('failed to connect'));
            db._connection.close(Hoek.ignore);
            await team.work;
            await db.close();
        });

        it('waits between reconnections', async () => {

            const team = new Teamwork({ meetings: 2 });
            const timer = new Hoek.Bench();

            let orig = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                team.attend();
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

            await db.connect();
            db.connect = () => Promise.reject(new Error('failed to connect'));
            timer.reset();
            db._connection.close(Hoek.ignore);
            await team.work;
            expect(timer.elapsed()).to.be.above(200);
            await db.close();
        });

        it('reconnects immediately', async () => {

            const team = new Teamwork({ meetings: 2 });
            const timer = new Hoek.Bench();

            let orig = null;
            let count = 0;
            const onConnect = () => {

                ++count;
                team.attend();
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

            await db.connect();
            db.connect = () => Promise.reject(new Error('failed to connect'));
            timer.reset();
            db._connection.close(Hoek.ignore);
            await team.work;
            expect(timer.elapsed()).to.be.below(100);
            await db.close();
        });

        it('does not reconnect automatically', async () => {

            const team = new Teamwork({ meetings: 1 });
            const onDisconnect = (willReconnect) => {

                expect(willReconnect).to.be.false();
                team.attend();
            };

            const db = new Penseur.Db('penseurtest', { onDisconnect, reconnect: false });

            await db.connect();
            db._connection.close(Hoek.ignore);
            await team.work;
            await db.close();
        });

        it('notifies of errors', async () => {

            const team = new Teamwork({ meetings: 1 });
            const onError = (err) => {

                expect(err.message).to.equal('boom');
                team.attend();
            };

            const db = new Penseur.Db('penseurtest', { onError });

            await db.connect();
            db._connection.emit('error', new Error('boom'));
            await team.work;
            await db.close();
        });

        it('notifies of timeout', async () => {

            const team = new Teamwork({ meetings: 1 });
            const onError = (err) => {

                expect(err.message).to.equal('Database connection timeout');
                team.attend();
            };

            const db = new Penseur.Db('penseurtest', { onError });
            await db.connect();
            db._connection.emit('timeout');
            await team.work;
            await db.close();
        });

        it('prepares generate id table', async () => {

            const prep = new Penseur.Db('penseurtest');
            await prep.establish(['allocate', 'test']);         // Cleanup
            await prep.close();

            const db = new Penseur.Db('penseurtest');
            db.table('test', { id: { type: 'increment', table: 'allocate' } });
            await db.connect();
            const keys = await db.test.insert({ a: 1 });
            expect(keys).to.equal('1');
        });

        it('errors on missing database', async () => {

            const db = new Penseur.Db('penseurtest_no_such_db');
            await expect(db.connect()).to.reject('Missing database: penseurtest_no_such_db');
            await db.close();
        });

        it('errors on database dbList() error', async () => {

            const db = new Penseur.Db('penseurtest');
            const orig = RethinkDB.dbList;
            RethinkDB.dbList = function () {

                RethinkDB.dbList = orig;

                return {
                    run: function (connection) {

                        return Promise.reject(new Error('Bad database'));
                    }
                };
            };

            await expect(db.connect()).to.reject();
            await db.close();
        });
    });

    describe('close()', () => {

        it('ignores unconnected state', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.close();
        });
    });

    describe('table()', () => {

        it('skips decorating object when table name conflicts', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['connect']);
            expect(typeof db.connect).to.equal('function');
            expect(db.tables.connect).to.exist();
            await db.close();
        });

        it('skips decorating object when table name begins with _', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['_testx']);
            expect(db._testx).to.not.exist();
            expect(db.tables._testx).to.exist();
            await db.close();
        });

        it('skips decorating object when table already set up', async () => {

            const db = new Penseur.Db('penseurtest');

            db.table('abc');
            await db.establish(['abc']);
            expect(db.tables.abc).to.exist();
            await db.close();
        });

        it('decorates an array of tables', async () => {

            const db = new Penseur.Db('penseurtest');
            db.table(['test1', 'test2']);
            expect(db.tables.test1).to.exist();
            expect(db.tables.test2).to.exist();
            await db.close();
        });
    });

    describe('establish()', () => {

        it('creates new database', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.connect();
            await RethinkDB.dbDrop(db.name).run(db._connection);

            await db.establish({ test: { secondary: 'other' } });
            const result = await RethinkDB.db(db.name).table('test').indexList().run(db._connection);
            expect(result).to.equal(['other']);
            await db.close();
        });

        it('creates new database (implicit tables)', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.connect();
            db.table({ test: { secondary: 'other' } });

            await db.establish();
            const result = await RethinkDB.db(db.name).table('test').indexList().run(db._connection);
            expect(result).to.equal(['other']);
            await db.close();
        });

        it('creates new database (complex tables pre-loaded)', async () => {

            const prep = new Penseur.Db('penseurtest');
            await prep.connect();
            await RethinkDB.dbDrop('penseurtest').run(prep._connection);
            await prep.close();

            const db = new Penseur.Db('penseurtest');
            db.table({ test: { id: 'increment' } });

            await db.establish(['test']);
            await db.close();
        });

        it('customizes table options', async () => {

            const Override = class extends Penseur.Table {

                insert(items) {

                    items = [].concat(items);
                    for (let i = 0; i < items.length; ++i) {
                        items[i].flag = true;
                    }

                    return super.insert(items);
                }
            };

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            await db.establish({ test: { extended: Override }, user: false, other: true });
            expect(db.user).to.not.exist();
            expect(db.other).to.exist();

            await db.test.insert({ id: 1, value: 'x' });
            const item = await db.test.get(1);
            expect(item.value).to.equal('x');
            expect(item.flag).to.equal(true);
            await db.close();
        });

        it('customizes table options before establish', async () => {

            const Override = class extends Penseur.Table {

                insert(items) {

                    items = [].concat(items);
                    for (let i = 0; i < items.length; ++i) {
                        items[i].flag = true;
                    }

                    return super.insert(items);
                }
            };

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });
            db.table('test', { extended: Override });
            await db.establish('test');

            await db.test.insert({ id: 1, value: 'x' });
            const item = await db.test.get(1);
            expect(item.value).to.equal('x');
            expect(item.flag).to.equal(true);
            await db.close();
        });

        it('creates database with different table indexes', async () => {

            const db1 = new Penseur.Db('penseurtest');
            await db1.connect();
            await db1.establish({ test: { secondary: 'other' } });
            const result1 = await RethinkDB.db(db1.name).table('test').indexList().run(db1._connection);
            expect(result1).to.equal(['other']);
            await db1.close();

            const db2 = new Penseur.Db('penseurtest');
            await db2.connect();
            await db2.establish(['test']);
            const result2 = await RethinkDB.db(db2.name).table('test').indexList().run(db2._connection);
            expect(result2).to.equal([]);
            await db2.close();
        });

        it('creates database and retains table indexes', async () => {

            const db1 = new Penseur.Db('penseurtest');
            await db1.connect();
            await db1.establish({ test: { secondary: 'other' } });
            const result1 = await RethinkDB.db(db1.name).table('test').indexList().run(db1._connection);
            expect(result1).to.equal(['other']);
            await db1.close();

            const db2 = new Penseur.Db('penseurtest');
            await db2.connect();

            await db2.establish({ test: { secondary: false } });
            const result2 = await RethinkDB.db(db2.name).table('test').indexList().run(db2._connection);
            expect(result2).to.equal(['other']);
            await db2.close();
        });

        it('creates database with different table indexes (partial overlap)', async () => {

            const db1 = new Penseur.Db('penseurtest');
            await db1.connect();
            await db1.establish({ test: { secondary: ['a', 'b'] } });
            const result1 = await RethinkDB.db(db1.name).table('test').indexList().run(db1._connection);
            expect(result1).to.equal(['a', 'b']);
            await db1.close();

            const db2 = new Penseur.Db('penseurtest');
            await db2.connect();
            await db2.establish({ test: { secondary: ['b', 'c'] } });
            const result2 = await RethinkDB.db(db2.name).table('test').indexList().run(db2._connection);
            expect(result2).to.equal(['b', 'c']);
            await db2.close();
        });

        it('retains records in existing table', async () => {

            const db1 = new Penseur.Db('penseurtest');
            await db1.connect();
            await db1.establish(['test']);
            await db1.test.insert({ id: 1 });
            await db1.close();

            const db2 = new Penseur.Db('penseurtest');
            await db2.connect();
            await db2.establish({ test: { purge: false } });
            const item = await db2.test.get(1);
            expect(item.id).to.equal(1);
            await db2.close();
        });

        it('fails creating a database', async () => {

            const db = new Penseur.Db('penseur-test');

            await expect(db.establish(['test'])).to.reject();
            await db.close();
        });

        it('fails connecting to missing server', async () => {

            const db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 0.001 });

            await expect(db.establish(['test'])).to.reject();
            await db.close();
        });

        it('errors on database dbList() error', async () => {

            const db = new Penseur.Db('penseurtest');
            const orig = RethinkDB.dbList;
            RethinkDB.dbList = function () {

                RethinkDB.dbList = orig;

                return {
                    run: function (connection) {

                        throw Error('Bad database');
                    }
                };
            };

            await expect(db.establish(['test'])).to.reject();
            await db.close();
        });

        it('errors creating new table', async () => {

            const db = new Penseur.Db('penseurtest');

            await expect(db.establish(['bad name'])).to.reject();
            await db.close();
        });

        it('errors emptying existing table', async () => {

            const Override = class extends Penseur.Table {

                empty() {

                    throw new Error('failed');
                }
            };

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            await expect(db.establish({ test: { extended: Override } })).to.reject();
            await db.close();
        });
    });

    describe('_createTable()', () => {

        it('creates table with custom primary key', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish({ test: { primary: 'other', id: 'uuid' } });
            const id = await db.test.insert({ a: 1 });
            const item = await db.test.get(id);
            expect(item).to.equal({ other: id, a: 1 });
            await db.close();
        });

        it('errors on database tableList() error', async () => {

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
                                    run: function (connection) {

                                        throw new Error('Bad database');
                                    }
                                };
                            }
                        };
                    }
                };
            };

            await expect(db.establish(['test'])).to.reject();
            await db.close();
        });
    });

    describe('_verify()', () => {

        it('errors on create table error (id)', async () => {

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

            await prep.establish(settings);
            await prep.close();

            const db = new Penseur.Db('penseurtest');
            db.table(settings);

            db.test._db._createTable = (options) => {

                throw new Error('Failed');
            };

            await expect(db.connect()).to.reject();
            await db.close();
        });

        it('errors on create table error (unique)', async () => {

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

            await prep.establish(settings);
            await prep.close();

            const db = new Penseur.Db('penseurtest');
            db.table(settings);

            db.test._db._createTable = (options) => {

                throw new Error('Failed');
            };

            await expect(db.connect()).to.reject();
            await db.close();
        });
    });

    describe('run()', () => {

        it('makes custom request', async () => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            await db.establish(['test']);
            const result = await db.run(db.test.raw.insert({ id: 1, value: 'x' }));
            expect(result).to.equal({
                deleted: 0,
                errors: 0,
                inserted: 1,
                replaced: 0,
                skipped: 0,
                unchanged: 0
            });

            const item = await db.run(db.test.raw.get(1).pluck('id'), {});
            expect(item).to.equal({ id: 1 });

            const all = await db.run(db.test.raw);
            expect(all).to.equal([{ id: 1, value: 'x' }]);
            await db.close();
        });

        it('makes custom request (options)', async () => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

            await db.establish(['test']);
            const result = await db.run(db.test.raw.insert({ id: 1, value: 'x' }), { profile: true });
            expect(result.profile).to.exist();
            await db.close();
        });

        it('includes table name on error', async () => {

            const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });
            db.table('test123');
            const err1 = await expect(db.run(db.test123.raw.insert({ id: 1, value: 'x' }))).to.reject('Database disconnected');
            expect(err1.data.table).to.equal('test123');

            const err2 = await expect(db.run(db.test123.raw.get(1).pluck('id'))).to.reject('Database disconnected');
            expect(err2.data.table).to.equal('test123');

            const err3 = await expect(db.run(db.test123.raw)).to.reject('Database disconnected');
            expect(err3.data.table).to.equal('test123');
        });
    });

    describe('_run()', () => {

        it('errors on invalid cursor', async () => {

            const db = new Penseur.Db('penseurtest');
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }]);

            const cursor = await db.test.raw.filter({ a: 1 }).run(db._connection);

            const proto = Object.getPrototypeOf(cursor);
            const orig = proto.toArray;
            proto.toArray = function () {

                proto.toArray = orig;
                throw new Error('boom');
            };

            cursor.close();

            await expect(db.test.query({ a: 1 })).to.reject();
        });
    });

    describe('is()', () => {

        it('errors on invalid number of arguments (3)', () => {

            expect(() => {

                Penseur.Db.is('=', 5, '<');
            }).to.throw('Cannot have odd number of arguments');
        });

        it('errors on invalid number of arguments (1)', () => {

            expect(() => {

                Penseur.Db.is('=');
            }).to.throw('Missing value argument');
        });
    });

    describe('test mode', () => {

        it('logs actions', async () => {

            const test = {};
            const db = new Penseur.Db('penseurtest', { test });
            await db.establish(['test']);
            await db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }]);
            const item = await db.test.get([1, 3]);
            expect(item).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);

            expect(test).to.equal({
                test: [
                    { action: 'empty', inputs: null },
                    { action: 'index', inputs: null },
                    { action: 'insert', inputs: { items: [{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], options: {} } },
                    { action: 'get', inputs: { ids: [1, 3], options: {} } }
                ]
            });
        });

        describe('disable()', () => {

            it('simulates an error', async () => {

                const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

                await db.establish(['test']);
                await db.test.insert({ id: 1, value: 'x' });
                const item1 = await db.test.get(1);
                expect(item1.value).to.equal('x');

                db.disable('test', 'get');
                const err = await expect(db.test.get(1)).to.reject();
                expect(err.data).to.equal({ table: 'test', action: 'get', inputs: undefined });

                db.enable('test', 'get');
                const item3 = await db.test.get(1);
                expect(item3.value).to.equal('x');
                await db.close();
            });

            it('simulates a response', async () => {

                const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

                await db.establish(['test']);
                await db.test.insert({ id: 1, value: 'x' });
                const item1 = await db.test.get(1);
                expect(item1.value).to.equal('x');

                db.disable('test', 'get', { value: 'hello' });
                const item2 = await db.test.get(1);
                expect(item2).to.equal('hello');

                db.disable('test', 'get', { value: null });
                const item3 = await db.test.get(1);
                expect(item3).to.be.null();

                db.disable('test', 'get', { value: new Error('stuff') });
                await expect(db.test.get(1)).to.reject('stuff');

                db.enable('test', 'get');
                const item4 = await db.test.get(1);

                expect(item4.value).to.equal('x');
                await db.close();
            });

            it('simulates a changes error', async () => {

                const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

                await db.establish(['test']);
                db.disable('test', 'changes');

                await expect(db.test.changes({ a: 1 }, Hoek.ignore)).to.reject();
            });

            it('simulates a changes update error', async () => {

                const team = new Teamwork({ meetings: 1 });
                const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

                await db.establish(['test']);
                db.disable('test', 'changes', { updates: true });

                const each = (err, update) => {

                    expect(err).to.exist();
                    expect(err.flags.willReconnect).to.be.true();
                    expect(err.data).to.equal({ table: 'test', action: 'changes', inputs: undefined });
                    team.attend();
                };

                await db.test.changes({ a: 1 }, each);
                await team.work;
            });

            it('simulates a changes update error (flags)', async () => {

                const team = new Teamwork({ meetings: 1 });
                const db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015, test: true });

                await db.establish(['test']);
                db.disable('test', 'changes', { updates: true, flags: { willReconnect: false } });

                const each = (err, update) => {

                    expect(err).to.exist();
                    expect(err.flags.willReconnect).to.be.false();
                    team.attend();
                };

                await db.test.changes({ a: 1 }, each);
                await team.work;
            });
        });
    });

    describe('append()', () => {

        it('errors on unique with multiple values', () => {

            const db = new Penseur.Db('penseurtest');

            expect(() => db.append([10, 11], { single: true, unique: true })).to.not.throw();
            expect(() => db.append([10, 11], { unique: true })).to.throw('Cannot append multiple values with unique requirements');
        });
    });
});
