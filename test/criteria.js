'use strict';

// Load modules

const Code = require('code');
const Lab = require('lab');
const Penseur = require('..');


// Declare internals

const internals = {};


// Test shortcuts

const lab = exports.lab = Lab.script();
const describe = lab.describe;
const it = lab.it;
const expect = Code.expect;


describe('Criteria', { parallel: false }, () => {

    it('parses empty criteria', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({}, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result.length).to.equal(3);
                    done();
                });
            });
        });
    });

    it('parses multiple keys', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: 2 }, { id: 2, a: 2, b: 1 }, { id: 3, a: 1, b: 1 }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: 1 }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: 1 }]);
                    done();
                });
            });
        });
    });

    it('parses nested keys', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: 1 } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: 1 } }]);
                    done();
                });
            });
        });
    });

    it('parses or', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: 1, b: { c: db.or([1, 2]) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: 1 } }, { id: 1, a: 1, b: { c: 2 } }]);
                    done();
                });
            });
        });
    });

    it('parses or with comparator', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 3 }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: db.or([db.is('>=', 3), db.is('eq', 1)]) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 3 }, { id: 1, a: 1 }]);
                    done();
                });
            });
        });
    });

    it('parses or unset', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, b: 1 }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: db.or([2, db.unset()]) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, b: 1 }, { id: 2, a: 2 }]);
                    done();
                });
            });
        });
    });

    it('parses or unset nested', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 1, b: { d: 3 } }, { id: 3, a: 1, b: { c: 66 } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.or([66, db.unset()]) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: 66 } }, { id: 2, a: 1, b: { d: 3 } }]);
                    done();
                });
            });
        });
    });

    it('parses not unset nested', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 1, b: { d: 3 } }, { id: 3, a: 1, b: { c: 66 } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.not([66, db.unset()]) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 1, a: 1, b: { c: 2 } }]);
                    done();
                });
            });
        });
    });

    it('parses or isEmpty', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();

            db.test.insert([{ id: 1, a: [] }, { id: 2, a: 1 }, { id: 3, a: [2] }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: db.or([1, db.isEmpty()]) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 2, a: 1 }, { id: 1, a: [] }]);
                    done();
                });
            });
        });
    });

    it('parses or isEmpty nested', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();

            db.test.insert([{ id: 1, a: 1, b: { c: [1] } }, { id: 2, a: 1, b: { c: [] } }, { id: 3, a: 1, b: { c: 66 } }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: 1, b: { c: db.or([66, db.isEmpty()]) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: 66 } }, { id: 2, a: 1, b: { c: [] } }]);
                    done();
                });
            });
        });
    });

    it('parses not isEmpty nested', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();

            db.test.insert([{ id: 1, a: 1, b: { c: [1] } }, { id: 2, a: 1, b: { c: [] } }, { id: 3, a: 1, b: { c: 66 } }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: 1, b: { c: db.not([66, db.isEmpty()]) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 1, a: 1, b: { c: [1] } }]);
                    done();
                });
            });
        });
    });

    it('parses or root', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([
                { id: 1, a: 1, b: { c: 2 } },
                { id: 2, a: 2, b: { c: 1 } },
                { id: 3, a: 3, b: { c: 3 } },
                { id: 4, a: 1 }
            ], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query(db.or([{ a: 1 }, { b: { c: 3 } }]), (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 4, a: 1 }, { id: 3, a: 3, b: { c: 3 } }, { id: 1, a: 1, b: { c: 2 } }]);
                    done();
                });
            });
        });
    });

    it('parses or objects', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([
                { id: 1, x: { a: 1, b: { c: 2 } } },
                { id: 2, x: { a: 2, b: { c: 1 } } },
                { id: 3, x: { a: 3, b: { c: 3 } } },
                { id: 4, x: { a: 1 } }
            ], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ x: db.or([{ a: 1 }, { b: { c: 3 } }]) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 4, x: { a: 1 } }, { id: 3, x: { a: 3, b: { c: 3 } } }, { id: 1, x: { a: 1, b: { c: 2 } } }]);
                    done();
                });
            });
        });
    });

    it('parses is', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 3 }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: db.is('<', 2) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 1, a: 1 }]);
                    done();
                });
            });
        });
    });

    it('parses is (multiple conditions)', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 3 }], (err, keys) => {

                expect(err).to.not.exist();
                db.test.query({ a: db.is('>', 1, '<', 3) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 2, a: 2 }]);
                    done();
                });
            });
        });
    });

    it('parses contains', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.contains(1) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                    done();
                });
            });
        });
    });

    it('parses contains or', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.contains([1, 2], { condition: 'or' }) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: [2, 3] } }, { id: 1, a: 1, b: { c: [1, 2] } }]);
                    done();
                });
            });
        });
    });

    it('parses contains and', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.contains([1, 2], { condition: 'and' }) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                    done();
                });
            });
        });
    });

    it('parses contains and default', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { c: [3, 4] } }, { id: 3, a: 1, b: { c: [2, 3] } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.contains([1, 2]) } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                    done();
                });
            });
        });
    });

    it('parses contains key', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3] } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: db.contains('c', { keys: true }) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 1, a: 1, b: { c: [1, 2] } }]);
                    done();
                });
            });
        });
    });

    it('parses contains keys or', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3] } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: db.contains(['c', 'e'], { keys: true, condition: 'or' }) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { e: [2, 3] } }, { id: 1, a: 1, b: { c: [1, 2] } }]);
                    done();
                });
            });
        });
    });

    it('parses contains keys and', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: [1, 2] } }, { id: 2, a: 2, b: { d: [3, 4] } }, { id: 3, a: 1, b: { e: [2, 3], f: 'x' } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: db.contains(['f', 'e'], { keys: true, condition: 'and' }) }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { e: [2, 3], f: 'x' } }]);
                    done();
                });
            });
        });
    });

    it('parses unset key', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: { c: 2 } }, { id: 2, a: 1, b: { d: 3 } }, { id: 3, a: 1, b: { c: null } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.unset() } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: null } }, { id: 2, a: 1, b: { d: 3 } }]);
                    done();
                });
            });
        });
    });

    it('parses isEmpty key', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();

            db.test.insert([{ id: 1, a: 1, b: { c: null } }, { id: 2, a: 1, b: { c: [2] } }, { id: 3, a: 1, b: { c: [] } }, { id: 4, a: 1, b: { c: 3 } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: db.isEmpty() } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: [] } }]);
                    done();
                });
            });
        });
    });

    it('handles query into nested object where item is not an object', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([{ id: 1, a: 1, b: false }, { id: 2, a: 2, b: { c: 1 } }, { id: 3, a: 1, b: { c: 1 } }], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ a: 1, b: { c: 1 } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 3, a: 1, b: { c: 1 } }]);
                    done();
                });
            });
        });
    });

    it('handles query into double nested object where item is not an object', (done) => {

        const db = new Penseur.Db('penseurtest');
        db.establish(['test'], (err) => {

            expect(err).to.not.exist();
            db.test.insert([
                { id: 1, a: 1, b: false },
                { id: 2, a: 2, b: { c: { d: 4 } } },
                { id: 3, a: 1, b: { c: 1 } }
            ], (err, keys) => {

                expect(err).to.not.exist();

                db.test.query({ b: { c: { d: 4 } } }, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.equal([{ id: 2, a: 2, b: { c: { d: 4 } } }]);
                    done();
                });
            });
        });
    });

    describe('select()', () => {

        it('selects by secondary index', (done) => {

            const db = new Penseur.Db('penseurtest');
            db.establish({ test: { secondary: 'a' } }, (err) => {

                expect(err).to.not.exist();
                db.test.insert([{ id: 1, a: 1 }, { id: 2, a: 2 }, { id: 3, a: 1 }], (err, keys) => {

                    expect(err).to.not.exist();
                    db.test.query(db.by('a', 1 ), (err, result) => {

                        expect(err).to.not.exist();
                        expect(result).to.equal([{ id: 3, a: 1 }, { id: 1, a: 1 }]);
                        done();
                    });
                });
            });
        });
    });
});
