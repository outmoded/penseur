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
                    done();
                });
            });
        });
    });

    describe('connect()', function () {

        it('uses default server location', function (done) {

            var db = new Penseur.Db('penseurtest');

            db.establish(['test'], function (err) {

                expect(err).to.not.exist();
                done();
            });
        });

        it('fails connecting to missing server', function (done) {

            var db = new Penseur.Db('penseurtest', { host: 'no.such.home.example.com' });

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                done();
            });
        });
    });

    describe('table()', function () {

        it('skips decorating object when table name conflicts', function (done) {

            var db = new Penseur.Db('penseurtest');

            db.establish(['connect'], function (err) {

                expect(err).to.not.exist();
                expect(typeof db.connect).to.equal('function');
                expect(db.tables.connect).to.exist();
                done();
            });
        });
    });

    describe('establish()', function () {

        it('fails creating a database', function (done) {

            var db = new Penseur.Db('penseur-test');

            db.establish(['test'], function (err) {

                expect(err).to.exist();
                done();
            });
        });
    });
});
