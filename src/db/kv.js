const utils = require('./../utils');

const antidote_client = require('antidote_ts_client');

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
    return !remote.hasOwnProperty('minSnapshotTime');
}

function startT(remote) {
    return remote.startTransaction();
}

function commitT(remote) {
    return remote.commit();
}

function runT(remote, fn) {
    // If the given remote is a transaction handle,
    // execute the function with the current one.
    if (isTxHandle(remote)) {
        return fn(remote);
    }

    const runnable = tx => {
        return fn(tx).then(v => {
            return commitT(tx).then(ct => ({ ct, result: v }));
        });
    };

    return startT(remote).then(runnable);
}

function put(remote, key, value) {
    const keys = utils.arreturn(key);
    const values = utils.arreturn(value);

    const refs = keys.map(k => generateRef(remote, k));
    const ops = refs.map((r, i) => r.set(values[i]));
    return remote.update(ops);
}

// condPut(_, k, v, e) will succeed iff get(_, k) = e
function condPut(remote, key, value, expected) {
    const run = tx => {
        return get(tx, key).then(vs => {
            const exp = utils.arreturn(expected);

            if (exp.length !== vs.length) {
                throw `ConditionalPut failed, expected ${expected}, got ${vs}`;
            }

            const equals = exp.every((elt, idx) => elt === vs[idx]);
            if (!equals) {
                throw `Condional put failed, expected ${expected}, got ${vs}`;
            }

            return put(tx, key, value);
        });
    };

    // Only care about commit time
    return runT(remote, run);
}

function get(remote, key) {
    const keys = utils.arreturn(key);
    const refs = keys.map(k => generateRef(remote, k));
    return remote.readBatch(refs);
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
