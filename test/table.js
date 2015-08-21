// Load modules

var Code = require('code');
var Hoek = require('hoek');
var Lab = require('lab');
var Penseur = require('..');


// Declare internals

var internals = {};


// Test shortcuts

var lab = exports.lab = Lab.script();
var describe = lab.describe;
var it = lab.it;
var expect = Code.expect;


describe('Table', { parallel: false }, function () {

    describe('get()', function () {

        it('fails on database error', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect(function (err) {

                db.invalid.get(1, function (err, item) {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('returns the requested objects', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.get([1, 3], function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.deep.include([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (partial)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.get([1, 3, 4], function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.have.length(2);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (duplicates)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.get([1, 3, 3], function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.have.length(3);
                        done();
                    });
                });
            });
        });

        it('returns the requested objects found (none)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.get([4, 5, 6], function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal([]);
                        done();
                    });
                });
            });
        });
    });

    describe('query()', function () {

        it('returns the requested objects', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.query({ a: 1 }, function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.deep.include([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });
    });

    describe('single()', function () {

        it('returns the requested object', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.single({ a: 2 }, function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.deep.equal({ id: 2, a: 2 });
                        done();
                    });
                });
            });
        });

        it('returns nothing', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.single({ a: 3 }, function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.equal(null);
                        done();
                    });
                });
            });
        });

        it('errors on multiple matches', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.single({ a: 1 }, function (err, result) {

                        expect(err).to.exist();
                        expect(err.message).to.equal('Database error');
                        done();
                    });
                });
            });
        });
    });

    describe('count()', function () {

        it('returns the number requested object', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.count({ a: 1 }, 'filter', function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.equal(2);
                        done();
                    });
                });
            });
        });

        it('returns the number of object with given field', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.count(['a'], 'fields', function (err, result) {

                        expect(err).to.not.exist();
                        expect(result).to.equal(3);
                        done();
                    });
                });
            });
        });
    });

    describe('insert()', function () {

        it('returns the generate key', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert({ a: 1 }, function (err, keys) {

                    expect(err).to.not.exist();
                    expect(keys).to.match(/\w+/);
                    done();
                });
            });
        });

        it('returns the generate keys', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ a: 1 }, { a: 2 }], function (err, keys) {

                    expect(err).to.not.exist();
                    expect(keys).to.have.length(2);
                    done();
                });
            });
        });
    });

    describe('update()', function () {

        it('updates a record', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.update(1, { a: 2 }, function (err) {

                        expect(err).to.not.exist();

                        db.test.get(1, function (err, item) {

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(2);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                db.test.update(2, { a: 2 }, function (err) {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('increment()', function () {

        it('updates a record', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.increment(1, 'a', 5, function (err) {

                        expect(err).to.not.exist();

                        db.test.get(1, function (err, item) {

                            expect(err).to.not.exist();
                            expect(item.a).to.equal(6);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                db.test.increment(1, 'a', 5, function (err) {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('append()', function () {

        it('updates a record', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: [1] }, function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.append(1, 'a', 5, function (err) {

                        expect(err).to.not.exist();

                        db.test.get(1, function (err, item) {

                            expect(err).to.not.exist();
                            expect(item.a).to.deep.equal([1, 5]);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                db.test.append(1, 'a', 5, function (err) {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('unset()', function () {

        it('updates a record', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.unset(1, 'a', function (err) {

                        expect(err).to.not.exist();

                        db.test.get(1, function (err, item) {

                            expect(err).to.not.exist();
                            expect(item.a).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                db.test.unset(1, 'a', function (err) {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });

    describe('remove()', function () {

        it('removes a record', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert({ id: 1, a: 1 }, function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.remove(1, function (err) {

                        expect(err).to.not.exist();

                        db.test.get(1, function (err, item) {

                            expect(err).to.not.exist();
                            expect(item).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });

        it('removes multiple records', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.remove([1, 2], function (err) {

                        expect(err).to.not.exist();

                        db.test.count({ a: 1 }, 'filter', function (err, count) {

                            expect(err).to.not.exist();
                            expect(count).to.equal(0);
                            done();
                        });
                    });
                });
            });
        });

        it('removes records using criteria', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test.remove({ a: 1 }, function (err) {

                        expect(err).to.not.exist();

                        db.test.count({ a: 1 }, 'filter', function (err, count) {

                            expect(err).to.not.exist();
                            expect(count).to.equal(0);
                            done();
                        });
                    });
                });
            });
        });

        it('errors on unknown key', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                db.test.remove(1, function (err) {

                    expect(err).to.exist();
                    done();
                });
            });
        });

        it('ignored error on unknown keys', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                db.test.remove([1], function (err) {

                    expect(err).to.not.exist();
                    done();
                });
            });
        });
    });

    describe('_run()', function () {

        it('errors on invalid cursor', { parallel: false }, function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 1 }], function (err, keys) {

                    expect(err).to.not.exist();

                    db.test._table.filter({ a: 1 }).run(db._connection, function (err, cursor) {

                        var proto = Object.getPrototypeOf(cursor);
                        var orig = proto.toArray;
                        proto.toArray = function (callback) {

                            proto.toArray = orig;
                            return callback(new Error('boom'));
                        };

                        cursor.close();

                        db.test.query({ a: 1 }, function (err, result) {

                            expect(err).to.exist();
                            done();
                        });
                    });
                });
            });
        });
    });

    describe('changes()', function () {

        it('reports on a record update (*)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                var changes = [];
                var each = function (err, item) {

                    changes.push(item.after.id);
                };

                db.test.changes('*', each, function (err, cursor) {

                    expect(err).to.not.exist();

                    db.test.insert({ id: 1, a: 1 }, function (err, keys) {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, function (err) {

                            expect(err).to.not.exist();

                            expect(changes).to.deep.equal([1, 1]);
                            cursor.close();
                            done();
                        });
                    });
                });
            });
        });

        it('reports on a record update (id)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                var changes = [];
                var each = function (err, item) {

                    changes.push(item.after.id);
                };

                db.test.insert([{ id: 1, a: 1 }], function (err, keys1) {

                    expect(err).to.not.exist();

                    db.test.changes(1, each, function (err, cursor) {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, function (err, keys2) {

                            expect(err).to.not.exist();

                            db.test.insert({ id: 2, a: 2 }, function (err) {

                                expect(err).to.not.exist();

                                expect(changes).to.deep.equal([1]);
                                cursor.close();
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (ids)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                var changes = [];
                var each = function (err, item) {

                    changes.push(item.after.id);
                };

                db.test.insert([{ id: 1, a: 1 }], function (err, keys1) {

                    expect(err).to.not.exist();

                    db.test.changes([1, 2], each, function (err, cursor) {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, function (err, keys2) {

                            expect(err).to.not.exist();

                            db.test.insert({ id: 2, a: 2 }, function (err) {

                                expect(err).to.not.exist();

                                expect(changes).to.deep.equal([1, 2]);
                                cursor.close();
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (query)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                var changes = [];
                var each = function (err, item) {

                    changes.push(item.after.id);
                };

                db.test.insert([{ id: 1, a: 1 }], function (err, keys1) {

                    expect(err).to.not.exist();

                    db.test.changes({ a: 2 }, each, function (err, cursor) {

                        expect(err).to.not.exist();

                        db.test.update(1, { a: 2 }, function (err, keys2) {

                            expect(err).to.not.exist();

                            db.test.insert({ id: 2, a: 2 }, function (err) {

                                expect(err).to.not.exist();

                                expect(changes).to.deep.equal([1, 2]);
                                cursor.close();
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('reports on a record update (delete)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                var changes = [];
                var each = function (err, item) {

                    changes.push(item.before.id + ':' + (item.after === null));
                };

                db.test.insert([{ id: 1, a: 1 }], function (err, keys1) {

                    expect(err).to.not.exist();

                    db.test.changes(1, each, function (err, cursor) {

                        expect(err).to.not.exist();

                        db.test.remove(1, function (err) {

                            expect(err).to.not.exist();
                            expect(changes).to.deep.equal(['1:true']);
                            cursor.close();
                            done();
                        });
                    });
                });
            });
        });

        it('reports on a record update (id missing)', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                var changes = [];
                var each = function (err, item) {

                    changes.push(item.after.id);
                };

                db.test.changes(1, each, function (err, cursor) {

                    expect(err).to.not.exist();

                    db.test.insert({ id: 1, a: 1 }, function (err, keys) {

                        expect(err).to.not.exist();
                        expect(changes).to.deep.equal([1]);
                        cursor.close();
                        done();
                    });
                });
            });
        });

        it('errors on bad cursor', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.establish(['test'], function (err) {

                expect(err).to.not.exist();

                var changes = [];
                var each = function (err, item) {

                    if (err) {
                        expect(err.message).to.equal('Database error');
                        done();
                    }
                };

                db.test.changes('*', each, function (err, cursor) {

                    expect(err).to.not.exist();

                    var orig = cursor._cursor._next;
                    cursor._cursor._next = function (next) {

                        cursor._cursor._next = orig;
                        return next(new Error('kaboom'));
                    };

                    db.test.insert({ id: 1, a: 1 }, function (err, keys) {

                        expect(err).to.not.exist();
                    });
                });
            });
        });

        it('errors on invalid table', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.table('invalid');
            db.connect(function (err) {

                db.invalid.changes('*', function (err, item) {

                    expect(err).to.exist();
                    done();
                });
            });
        });
    });
});
