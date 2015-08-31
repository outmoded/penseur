// Load modules

var Code = require('code');
var Lab = require('lab');
var Penseur = require('..');
var RethinkDB = require('rethinkdb');


// Declare internals

var internals = {};


// Test shortcuts

var lab = exports.lab = Lab.script();
var describe = lab.describe;
var it = lab.it;
var expect = Code.expect;


describe('Db', function () {

    it('establishes and interacts with a database', function (done) {

        var db = new Penseur.Db('penseurtest', { host: 'localhost', port: 28015 });

        db.establish(['test'], function (err) {

            expect(err).to.not.exist();
            db.test.insert({ id: 1, value: 'x' }, function (err, result) {

                expect(err).to.not.exist();

                db.test.get(1, function (err, item) {

                    expect(err).to.not.exist();
                    expect(item.value).to.equal('x');
                    db.close(done);
                });
            });
        });
    });

    describe('connect()', function () {

        it('uses default server location', function (done) {

            var db = new Penseur.Db('penseurtest');

            db.connect(function (err) {

                expect(err).to.not.exist();
                db.close(done);
            });
        });

        it('fails connecting to missing server', function (done) {

            var db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 0.001 });

            db.connect(function (err) {

                expect(err).to.exist();
                db.close(done);
            });
        });
    });

    describe('close()', function () {

        it('ignores unconnected state', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.close(done);
        });

        it('allows no callback', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.close();
            done();
        });
    });

    describe('table()', function () {

        it('skips decorating object when table name conflicts', function (done) {

            var db = new Penseur.Db('penseurtest');

            db.establish(['connect'], function (err) {

                expect(err).to.not.exist();
                expect(typeof db.connect).to.equal('function');
                expect(db.tables.connect).to.exist();
                db.close(done);
            });
        });
    });

    describe('establish()', function () {

        it('creates new database', function (done) {

            var db = new Penseur.Db('penseurtest');
            db.connect(function (err) {

                expect(err).to.not.exist();

                RethinkDB.dbDrop(db._name).run(db._connection, function (err, dropped) {

                    db.establish(['test'], function (err) {

                        expect(err).to.not.exist();
                        db.close(done);
                    });
                });
            });
        });

        it('fails creating a database', function (done) {

            var db = new Penseur.Db('penseur-test');

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('fails connecting to missing server', function (done) {

            var db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 0.001 });

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('errors on database dbList() error', { parallel: false }, function (done) {

            var db = new Penseur.Db('penseurtest');
            var orig = RethinkDB.dbList;
            RethinkDB.dbList = function () {

                RethinkDB.dbList = orig;

                return {
                    run: function (connection, next) {

                        return next(new Error('Bad database'));
                    }
                };
            };

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('errors on database dbCreate() error', { parallel: false }, function (done) {

            var db = new Penseur.Db('penseur-test');
            db.connect(function (err) {

                expect(err).to.not.exist();

                RethinkDB.dbDrop(db._name).run(db._connection, function (err, dropped) {

                    var orig = RethinkDB.dbCreate;
                    RethinkDB.dbCreate = function () {

                        RethinkDB.dbCreate = orig;

                        return {
                            run: function (connection, next) {

                                return next(new Error('Bad database'));
                            }
                        };
                    };

                    db.establish(['test'], function (err) {

                        expect(err).to.exist();
                        db.close(done);
                    });
                });
            });
        });
    });

    describe('_createTable', function () {

        it('errors on database dbList() error', { parallel: false }, function (done) {

            var db = new Penseur.Db('penseurtest');
            var orig = RethinkDB.db;
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

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                db.close(done);
            });
        });
    });
});
