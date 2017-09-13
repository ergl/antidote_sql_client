const antidoteSQL = require('../src/antidoteSQL');
const orderedKeySet = require('../src/db/orderedKeySet');

function passThen(fn) {
    return function(r) {
        fn(r);
        return Promise.resolve(r);
    };
}

function createRemote() {
    return antidoteSQL.connect(8087, '127.0.0.1');
}

function printKSets(remote) {
    const allSets = remote.kset;
    if (allSets.length === 0) {
        console.log('Transaction handle sets are empty');
    } else {
        allSets.forEach(({ tableName, set }) => {
            console.log(
                'Kset for table',
                tableName,
                ':',
                orderedKeySet.printContents(set)
            );
        });
    }
}

function resetInternal(db) {
    return antidoteSQL.runTransaction(db, tx => {
        return antidoteSQL.reset(tx);
    });
}

function resetDB() {
    console.log('Database reset');
    const remote = createRemote();
    return resetInternal(remote)
        .then(_ => {
            return antidoteSQL.close(remote);
        })
        .catch(e => {
            console.error('Reset Error', e);
        });
}

function printContents(db) {
    return antidoteSQL.runTransaction(db, tx => {
        console.log('Database contents:');
        const sets = tx.kset;
        sets.forEach(({ set }) => {
            console.log(orderedKeySet.printContents(set));
        });
        return Promise.resolve([]);
    });
}

function dumpTransaction() {
    const remote = createRemote();
    return printContents(remote).then(_ => antidoteSQL.close(remote));
}

module.exports = {
    passThen,
    createRemote,
    reset: resetDB,
    printContents,
    printKSets,
    dumpTransaction
};
