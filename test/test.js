const antidoteSQL = require('../src/antidoteSQL');

const assert = require('assert');
const utils = require('./utils');

function connectionTest() {
    const remote = antidoteSQL.connect(8087, '127.0.0.1');
    return utils
        .reset()
        .then(_ => {
            console.log('Connection established');
            return antidoteSQL.close(remote);
        })
        .catch(e => console.log('Error', e));
}

// Test w(k, v)->r(k, v) inside a single transaction
function readWriteTest() {
    const remote = utils.createRemote();
    return antidoteSQL
        .runTransaction(remote, tx => {
            return antidoteSQL
                .createTable(tx, 'tableA', ['idA', 'fieldA'])
                .then(_ => antidoteSQL.insert(tx, 'tableA', {
                    fieldA: 'foo'
                }))
                .then(_ => antidoteSQL.select(tx, '*', 'tableA'))
                .then(res => {
                    // expect res === [{ idA: 1, fieldA: 'content' }]
                    assert.ok(res.length === 1);
                    assert(res[0]['idA'] === 1);
                    assert(res[0]['fieldA'] === 'foo');
                });
        })
        .then(_ => antidoteSQL.close(remote));
}

// Test w(k, v)->r(k, v) across transactions
function sequentialReadTest() {
    const remote = utils.createRemote();

    const first = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.createTable(tx, 'tableA', ['idA', 'fieldA']).then(_ => {
            return antidoteSQL.insert(tx, 'tableA', { fieldA: 'content' });
        });
    });

    const second = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.select(tx, '*', 'tableA').then(res => {
            // expect res === [{ idA: 1, fieldA: 'content' }]
            assert.ok(res.length === 1);
            assert(res[0]['idA'] === 1);
            assert(res[0]['fieldA'] === 'content');
        });
    });

    return first(remote).then(_ => second(remote));
}

// Make sure that updates work as expected
function updateTest() {
    const remote = utils.createRemote();

    return antidoteSQL.runTransaction(remote, tx => {
        return antidoteSQL
            .createTable(tx, 'tableA', ['idA', 'fieldA'])
            .then(_ => antidoteSQL.insert(tx, 'tableA', { fieldA: 'content' }))
            .then(_ => {
                return antidoteSQL.select(tx, '*', 'tableA').then(res => {
                    assert.ok(res.length === 1);
                    assert(res[0]['idA'] === 1);
                    assert(res[0]['fieldA'] === 'content');
                });
            })
            .then(_ => antidoteSQL.update(tx, 'tableA', { fieldA: 'updated content' }))
            .then(_ => {
                return antidoteSQL.select(tx, '*', 'tableA').then(res => {
                    assert.ok(res.length === 1);
                    assert(res[0]['idA'] === 1);
                    assert(res[0]['fieldA'] === 'updated content');
                });
            })
            .then(_ => {
                return antidoteSQL.update(tx, 'tableA', {
                    fieldA: s => s + ' with a function'
                });
            })
            .then(_ => {
                return antidoteSQL.select(tx, '*', 'tableA').then(res => {
                    assert.ok(res.length === 1);
                    assert(res[0]['idA'] === 1);
                    assert(res[0]['fieldA'] === 'updated content with a function');
                });
            });
    });
}

// Make sure that the key set is appropiately refreshed between transactions
function ksetRefreshTest() {
    const remote = utils.createRemote();

    const setup = db => antidoteSQL.runTransaction(db, tx => {
        console.log('Setup');
        utils.printKSets(tx);
        return antidoteSQL.createTable(tx, 'tableA', ['idA', 'fieldA']);
    });

    const insert = db => antidoteSQL.runTransaction(db, tx => {
        console.log('Insert');
        utils.printKSets(tx);
        return antidoteSQL.insert(tx, 'tableA', {
            fieldA: 'content'
        });
    });

    const read = db => antidoteSQL.runTransaction(db, tx => {
        console.log('Read');
        utils.printKSets(tx);
        return antidoteSQL.select(tx, '*', 'tableA').then(res => {
            // expect res === [{ idA: 1, fieldA: 'content' }]
            assert.ok(res.length === 1);
            assert(res[0]['idA'] === 1);
            assert(res[0]['fieldA'] === 'content');
        });
    });

    return setup(remote).then(_ => insert(remote)).then(_ => read(remote));
}

// Make sure that autoincremented PK don't change on aborted transactions
function autoCounterTest() {
    const remote = utils.createRemote();

    const setup = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.createTable(tx, 'tableA', ['idA', 'fieldA']);
    });

    const abortedInsert = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.insert(tx, 'tableA', { fieldA: 'wrong' }).then(_ => {
            throw new Error('Purposeful abort');
        });
    });

    const successfulInsert = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.insert(tx, 'tableA', { fieldA: 'right' });
    });

    const read = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.select(tx, 'idA', 'tableA').then(res => {
            const [{ idA }] = res;
            assert.ok(idA === 1);
        });
    });

    return setup(remote)
        .then(_ => abortedInsert(remote).catch(_ => successfulInsert(remote)))
        .then(_ => read(remote));
}

// Ensure that concurrent conflicting transactions don't overwrite each other
function wwConflictTest() {
    const remote = utils.createRemote();

    const createA = () => antidoteSQL.runTransaction(remote, tx => {
        return antidoteSQL.createTable(tx, 'tableA', ['aId', 'fieldAA', 'fieldAB']);
    });

    const createB = () => antidoteSQL.runTransaction(remote, tx => {
        return antidoteSQL.createTable(tx, 'tableB', ['bId', 'fieldBA', 'fieldBB']);
    });

    return Promise.all([createA(), createB()])
        .catch(_ => {
            console.log('WW conflicts are forbidden');
            return [];
        })
        .then(_ => utils.printContents(remote))
        .then(_ => antidoteSQL.close(remote));
}

// Ensure that foreign key integrity is respected
function foreignKeyTest() {
    const remote = utils.createRemote();

    return antidoteSQL.runTransaction(remote, tx => {
        return antidoteSQL
            .createTable(tx, 'tableA', ['idA', 'fieldA'])
            .then(_ => antidoteSQL.createTable(tx, 'tableB', ['idB', 'fieldB']))
            .then(_ => antidoteSQL.createFK(tx, 'tableA', {
                alias: 'fieldA',
                field_name: 'idB',
                reference_table: 'tableB'
            }))
            .then(_ => {
                return antidoteSQL
                    .insert(tx, 'tableA', {
                        fieldA: 1
                    })
                    .catch(_ => {
                        console.log('Errored as expected');
                        return antidoteSQL.insert(tx, 'tableB', {
                            fieldB: 'foo'
                        });
                    })
                    .then(_ => antidoteSQL.insert(tx, 'tableA', {
                        fieldA: 1
                    }));
            });
    });
}

function uniqueIndexTest() {
    const remote = utils.createRemote();

    const setup = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.createTable(tx, 'tableA', ['idA', 'fieldA']).then(_ => {
            return antidoteSQL.createUniqueIndex(tx, 'tableA', {
                index_name: 'tableA_uniqueIndex',
                field_names: 'fieldA'
            });
        });
    });

    const insert = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.insert(tx, 'tableA', {
            fieldA: 'unique content'
        });
    });

    const modifyUniqueIndex = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.update(tx, 'tableA', { fieldA: 'something different' }, {
            fieldA: 'unique content'
        });
    });

    return setup(remote)
        .then(_ => insert(remote))
        .then(_ => {
            return insert(remote).catch(_ => {
                console.log('Failed, uniqueness was preserved');
            });
        })
        .then(_ => modifyUniqueIndex(remote))
        .then(_ => {
            return insert(remote).then(_ => {
                console.log('Index was modified, insert went through');
            });
        });
}

function joinCheck() {
    const remote = utils.createRemote();

    const setup = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL
            .createTable(tx, 'tableA', ['idA', 'fieldA'])
            .then(_ => {
                return antidoteSQL.createTable(tx, 'tableB', [
                    'idB',
                    'reference',
                    'content'
                ]);
            })
            .then(_ => {
                return antidoteSQL.createFK(tx, 'tableB', {
                    alias: 'reference',
                    field_name: 'idA',
                    reference_table: 'tableA'
                });
            });
    });

    const insert = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL
            .insert(tx, 'tableA', { fieldA: 'first' })
            .then(_ => antidoteSQL.insert(tx, 'tableA', { fieldA: 'second' }))
            .then(_ => antidoteSQL.insert(tx, 'tableB', { reference: 1, content: 'foo' }))
            .then(_ => antidoteSQL.insert(tx, 'tableB', { reference: 1, content: 'bar' }))
            .then(_ => antidoteSQL.insert(tx, 'tableB', { reference: 2, content: 'baz' }))
            .then(_ => {
                return antidoteSQL.insert(tx, 'tableB', { reference: 2, content: 'qux' });
            });
    });

    const simpleJoin = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL
            .select(tx, '*', ['tableB', 'tableA'], {
                using: ['reference', 'idA']
            })
            .then(utils.passThen(console.log));
    });

    const predicateJoin = db => antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL
            .select(tx, '*', ['tableB', 'tableA'], {
                using: ['reference', 'idA'],
                tableB: {
                    content: ['foo', 'baz']
                }
            })
            .then(utils.passThen(console.log));
    });

    return setup(remote)
        .then(_ => insert(remote))
        .then(_ => simpleJoin(remote))
        .then(_ => predicateJoin(remote));
}

const toRun = [
    // Sanity check
    connectionTest,
    // Basic causality checks
    readWriteTest,
    sequentialReadTest,
    updateTest,
    ksetRefreshTest,
    // Ensure correctness
    autoCounterTest,
    wwConflictTest,
    foreignKeyTest,
    joinCheck
];

toRun
    .reduce(
        (acc, fn) => {
            return acc.then(_ => {
                console.log('Executing', fn.name);
                return fn().then(_ => utils.reset());
            });
        },
        utils.reset()
    )
    .then(_ => process.exit());
