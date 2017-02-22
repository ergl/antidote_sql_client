const antidote_client = require('antidote_ts_client');

const utils = require('./../utils');
const keyEncoding = require('./keyEncoding');
const orderedKeySet = require('./orderedKeySet');

function createRemote(port, url, opts = { bucket: 'default-bucket' }) {
    const remote = antidote_client.connect(port, url);
    const bucket = opts.bucket || 'default-bucket';
    return Object.assign(remote, { defaultBucket: bucket });
}

function closeRemote(remote) {
    remote.close();
}

// The interface for connections and transaction handles is pretty much
// identical, but the current API doesn't allow nested transaction.
function isTxHandle(remote) {
    if (remote === undefined) throw new Error('Undefined remote');
    return !remote.hasOwnProperty('minSnapshotTime');
}

function startT(remote) {
    return remote.startTransaction();
}

function commitT(remote) {
    return remote.commit();
}

function abortT(remote) {
    return remote.abort();
}

function read_set(remote) {
    const set_key = keyEncoding.set_key();
    const ref = generateRef(remote, set_key);
    return ref.read().then(v => {
        return v === null ? orderedKeySet.empty() : v;
    });
}

function write_set({ remote, kset }) {
    const set_key = keyEncoding.set_key();
    const ref = generateRef(remote, set_key);
    return remote.update(ref.set(kset));
}

function runT(remote, fn) {
    // If the given remote is a transaction handle,
    // execute the function with the current one.
    if (isTxHandle(remote)) {
        return fn(remote);
    }

    const runnable = tx_handle => {
        return read_set(tx_handle)
            .then(set => ({ remote: tx_handle, kset: set }))
            .then(tx => fn(tx).then(v => write_set(tx).then(_ => {
                return commitT(tx.remote).then(ct => ({ ct, result: v }));
            })))
            .catch(e => {
                return abortT(tx_handle).then(_ => {
                    console.error('Transaction aborted, reason:', e);
                    throw e;
                });
            });
    };

    return startT(remote).then(runnable);
}

function put({ remote, kset }, key, value) {
    if (!isTxHandle(remote)) throw new Error('Calling put outside a transaction');

    const keys = utils.arreturn(key);
    const readable_keys = keys.map(({ key }) => keyEncoding.toString(key));
    const values = utils.arreturn(value);

    const refs = readable_keys.map(k => generateRef(remote, k));
    const ops = refs.map((r, i) => r.set(values[i]));
    // If put is successful, add the keys to the kset
    return remote.update(ops).then(ct => {
        keys.forEach(({ key }) => orderedKeySet.add(key, kset));
        return ct;
    });
}

// condPut(_, k, v, e) will succeed iff get(_, k) = e
function condPut(remote, key, value, expected) {
    return runT(remote, function(tx) {
        return get(tx, key).then(vs => {
            const exp = utils.arreturn(expected);

            if (exp.length !== vs.length) {
                throw new Error(`ConditionalPut failed, expected ${expected}, got ${vs}`);
            }

            const equals = exp.every((elt, idx) => elt === vs[idx]);
            if (!equals) {
                throw new Error(`Condional put failed, expected ${expected}, got ${vs}`);
            }

            return put(tx, key, value);
        });
    });
}

function get({ remote }, key) {
    if (!isTxHandle(remote)) throw new Error('Calling get outside a transaction');

    const keys = utils.arreturn(key);
    const readable_keys = keys.map(({ key }) => keyEncoding.toString(key));

    const refs = readable_keys.map(k => generateRef(remote, k));
    return remote.readBatch(refs).then(read_values => {
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
    const invalid = values.reduce(
        (acc, v, ix) => {
            return v === null ? acc.concat(keys[ix]) : acc;
        },
        []
    );

    const all_valid = invalid.length === 0;
    return { valid: all_valid, values: invalid };
}

function generateRef(remote, key) {
    return remote.register(key);
}

module.exports = {
    createRemote,
    closeRemote,
    runT,
    get,
    put,
    condPut
};
