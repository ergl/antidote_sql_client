const antidote_client = require('antidote_ts_client');

const utils = require('./../utils');
const keyEncoding = require('./keyEncoding');
const orderedKeySet = require('./orderedKeySet');

function createRemote(port, url, opts = { bucket: 'bucket' }) {
    const remote = antidote_client.connect(port, url);
    const bucket = opts.bucket || 'default-bucket';
    return Object.assign(remote, { defaultBucket: bucket });
}

function closeRemote(remote) {
    remote.close();
}

// Create a transaction handle
function createHandle(antidoteConnection, sets) {
    return { __handle_v1: true, remote: antidoteConnection, kset: sets };
}

function getHandleConnection(handle) {
    if (handle.__handle_v1) {
        return handle.remote;
    }
}

function getHandleKset(handle) {
    if (handle.__handle_v1) {
        return handle.kset;
    }
}

function setHandleKset(handle, ksets) {
    return Object.assign(handle, { kset: ksets });
}

// The interface for connections and transaction handles is pretty much
// identical, but the current API doesn't allow nested transaction.
function isTxHandle(remote) {
    if (remote === undefined) throw new Error('Undefined remote');

    if (remote.__handle_v1) {
        return true;
    }

    // If it's a raw Antidote connection, check for internal state (hack)
    return !remote.hasOwnProperty('minSnapshotTime');
}

function startT(antidoteConnection) {
    return antidoteConnection.startTransaction();
}

function commitT(antidoteConnection) {
    return antidoteConnection.commit();
}

function abortT(antidoteConnection) {
    return antidoteConnection.abort();
}

// Refresh the setList of the transaction handle
//
// A transaction handle is {remote: Connection, kset},
// where kset is a list of {tableName: string, set: Kset}
//
// This will sync the local handle state with the one in the database
function populateSet(txHandle) {
    const conn = getHandleConnection(txHandle);
    return readSet(conn).then(setList => {
        const oldSets = getHandleKset(txHandle);
        oldSets.forEach(set => {
            if (!setList.find(elt => elt === set)) {
                setList.push(set);
            }
        });

        return setHandleKset(txHandle, setList);
    });
}

// Fetch the list of {tableName: string, set: Kset} sets for the given connection
function readSet(antidoteConnection) {
    const f_allSets = raw_readSummary(antidoteConnection);
    return f_allSets.then(allSets => {
        const all = allSets.map(({ tableName, setKey }) => {
            const f_set = readSingleSet(antidoteConnection, setKey);
            return f_set.then(set => ({ tableName, set }));
        });

        return Promise.all(all);
    });
}

// Fetch the kset object for the given setKey
function readSingleSet(antidoteConnection, setKey) {
    const ref = generateRef(antidoteConnection, setKey);
    return ref.read().then(v => {
        if (v === null) {
            return orderedKeySet.empty();
        }

        return orderedKeySet.deserialize(v);
    });
}

// Flush back to the database the kset objects modified during
// this transaction
function writeSet(txHandle) {
    const remote = getHandleConnection(txHandle);
    const kset = getHandleKset(txHandle);

    const all = kset.map(({ tableName, set }) => {
        return writeSingleSet(remote, { tableName, set });
    });

    return Promise.all(all);
}

// Update the kset object in the database for the given table
//
// If the kset was not modified (read-only tx), then skip
function writeSingleSet(antidoteConnection, { tableName, set }) {
    if (!orderedKeySet.wasChanged(set)) {
        return Promise.resolve([]);
    }

    const setKey = keyEncoding.generateSetKey(tableName);
    const ref = generateRef(antidoteConnection, setKey);
    const ser = orderedKeySet.serialize(set);
    return antidoteConnection.update(ref.set(ser));
}

function readSummary(txHandle) {
    const connection = getHandleConnection(txHandle);
    return raw_readSummary(connection);
}

// Get the summary for the database
//
// The summary is a list of objects {tableName: string, setKey: Key},
// where the set key points to the table-specific Key Set stored in Antidote
function raw_readSummary(antidoteConnection) {
    const summaryKey = keyEncoding.summaryKey();
    const ref = generateRef(antidoteConnection, summaryKey);
    return ref.read().then(v => {
        return v === null ? [] : v;
    });
}

// Overwrite the summary for the database
function writeSummary(txHandle, summary) {
    const conn = getHandleConnection(txHandle);

    const summaryKey = keyEncoding.summaryKey();
    const ref = generateRef(conn, summaryKey);
    return conn.update(ref.set(summary));
}

function runT(remote, fn) {
    // If the given remote is a transaction handle,
    // execute the function with the current one.
    if (isTxHandle(remote)) {
        return fn(remote);
    }

    const runnable = dbConnection => {
        // TODO: Fetch the kset objects on demand?
        return readSet(dbConnection)
            .then(set => createHandle(dbConnection, set))
            .then(txHandle => {
                const f_result = fn(txHandle).then(result => {
                    return writeSet(txHandle).then(_ => result);
                });

                return f_result.then(result => {
                    const f_ct = commitT(getHandleConnection(txHandle));
                    return f_ct.then(ct => ({ ct, result }));
                });
            })
            .catch(e => {
                return abortT(dbConnection).then(_ => {
                    console.error('Transaction aborted, reason:', e);
                    throw e;
                });
            });
    };

    return startT(remote).then(runnable);
}

function put(txHandle, key, value) {
    if (!isTxHandle(txHandle)) {
        throw new Error('Calling put outside a transaction');
    }

    const connection = getHandleConnection(txHandle);

    const keys = utils.arreturn(key);
    const readable_keys = keys.map(keyEncoding.toString);
    const values = utils.arreturn(value);

    const refs = readable_keys.map(k => generateRef(connection, k));
    const ops = refs.map((r, i) => r.set(values[i]));

    const kset = getHandleKset(txHandle);
    // If put is successful, add the keys to the kset
    return connection.update(ops).then(ct => {
        keys.forEach(key => addKey(kset, key));
        return ct;
    });
}

// condPut(_, k, v, e) will succeed iff get(_, k) = e | get(_, k) = ⊥
function condPut(remote, key, value, expected) {
    return runT(remote, function(tx) {
        return get(tx, key, { unsafe: true }).then(vs => {
            const exp = utils.arreturn(expected);
            if (exp.length !== vs.length) {
                throw new Error(
                    `Conditional put failed, expected ${expected}, got ${
                        vs
                    } from ${key.map(keyEncoding.toString)}`
                );
            }

            const equals = cond_match(vs, exp);
            if (!equals) {
                throw new Error(
                    `Conditional put failed, expected ${expected}, got ${
                        vs
                    } from ${key.map(keyEncoding.toString)}`
                );
            }

            return put(tx, key, value);
        });
    });
}

function cond_match(got, expected) {
    const empty = got.every(g => g === null);
    const match = expected.every((elt, ix) => elt === got[ix]);
    return empty || match;
}

function get(txHandle, key, { unsafe } = { unsafe: false }) {
    if (!isTxHandle(txHandle)) {
        throw new Error('Calling get outside a transaction');
    }

    const connection = getHandleConnection(txHandle);
    const keys = utils.arreturn(key);
    if (keys.length === 0) {
        return Promise.resolve([]);
    }

    const readable_keys = keys.map(keyEncoding.toString);

    const refs = readable_keys.map(k => generateRef(connection, k));
    return connection.readBatch(refs).then(read_values => {
        if (unsafe) return read_values;

        const { valid, values } = invalidValues(readable_keys, read_values);
        if (!valid) {
            throw new Error(`Empty get on key: ${values}`);
        }

        if (read_values.length === 1) {
            return read_values[0];
        }

        return read_values;
    });
}

function invalidValues(keys, values) {
    const invalid = values.reduce((acc, v, ix) => {
        return v === null ? acc.concat(keys[ix]) : acc;
    }, []);

    const all_valid = invalid.length === 0;
    return { valid: all_valid, values: invalid };
}

function generateRef(antidoteConnection, key) {
    return antidoteConnection.register(key);
}

function addKey(kset, key) {
    const table = keyEncoding.keyBucket(key);
    const { set } = kset.find(({ tableName }) => tableName === table);
    return orderedKeySet.add(key, set);
}

function subkeyBatch(txHandle, table, key) {
    const kset = getHandleKset(txHandle);
    const { set } = kset.find(({ tableName }) => tableName === table);
    return orderedKeySet.subkeys(key, set);
}

function strictSubkeyBatch(txHandle, table, key) {
    const kset = getHandleKset(txHandle);
    const { set } = kset.find(({ tableName }) => tableName === table);
    return orderedKeySet.strictSubkeys(key, set);
}

function removeKey(txHandle, table, key) {
    const kset = getHandleKset(txHandle);
    const { set } = kset.find(({ tableName }) => tableName === table);
    return orderedKeySet.remove(key, set);
}

function reset(txHandle) {
    const kset = getHandleKset(txHandle);

    const allSets = kset.map(({ set }) => set);
    const allKeys = allSets.map(set => ({
        set,
        keys: orderedKeySet.dumpKeys(set)
    }));
    const allValues = allKeys.map(({ keys }) => {
        return [...new Array(keys.length)].fill(null);
    });

    const f_removeAll = allKeys.map(({ keys }, ix) => {
        return put(txHandle, keys, allValues[ix]);
    });

    return Promise.all(f_removeAll).then(_ => {
        allKeys.forEach(({ set, keys }) => {
            keys.forEach(key => orderedKeySet.remove(key, set));
        });

        return writeSummary(txHandle, null);
    });
}

module.exports = {
    readSummary,
    writeSummary,
    populateSet,
    createRemote,
    closeRemote,
    runT,
    get,
    put,
    condPut,
    subkeyBatch,
    strictSubkeyBatch,
    removeKey,
    reset
};
