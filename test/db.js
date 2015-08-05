// Load modules

var Code = require('code');
var Lab = require('lab');
var Penseur = require('..');


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

            var db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 1 });

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

        it('fails creating a database', function (done) {

            var db = new Penseur.Db('penseur-test');

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                db.close(done);
            });
        });

        it('fails connecting to missing server', function (done) {

            var db = new Penseur.Db('penseurtest', { host: 'example.com', timeout: 1 });

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                db.close(done);
            });
        });
    });
});
