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
    });
});
