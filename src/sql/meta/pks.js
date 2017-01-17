const kv = require('./../../db/kv')
const metaCont = require('./metaCont')
const keyEncoding = require('./../../db/keyEncoding')

// Given a table name, return the name of the field
// that acts as primary key for the table.
//
// Will fail if there is no primary key. (As all tables must have
// a primary key set).
//
function getPKField(remote, table_name) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    const meta_pk_key = keyEncoding.encodeMetaPK(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(meta_pk_key)
    }).then(pk_field => {
        if (pk_field === undefined) {
            throw `Fatal: ${table_name} doesn't have a primary key`
        }

        return pk_field
    })
}

// Given table name, perform a fetch-and-add on its key counter reference,
// and return the new value.
//
// To perform FAA atomically, this function must start a new transaction. However,
// given that the current antidote API doesn't allow nested transactions, this function
// must be called with `{in_tx: true}` if used inside another transaction.
//
// Will start a new transaction by default.
//
function fetchAddPrimaryKey_T(remote, table_name, {in_tx} = {in_tx: false}) {
    const runnable = tx => {
        return incrKey(tx, table_name).then(_ => getCurrentKey(tx, table_name))
    }

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable)
}

function getCurrentKey(remote, table_name) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    const count_key = keyEncoding.encodeMetaCounter(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.counterValue(count_key)
    })
}

// Atomically increment the primary key counter value.
function incrKey(remote, table_name) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    return remote.update(updateOps(meta_ref, table_name, {increment_pk: 1}))
}

// Generate the appropiate update operations to set the primary key value
function updateOps(meta_ref, table_name, opts) {
    const increment_pk = opts.increment_pk || 0
    const pk_field = opts.pk_field || null

    const meta_pk_ref = meta_ref.register(keyEncoding.encodeMetaPK(table_name))
    const meta_pk_counter_ref = meta_ref.counter(keyEncoding.encodeMetaCounter(table_name))

    const ops = [meta_pk_counter_ref.increment(increment_pk)]

    if (pk_field !== null) {
        ops.push(meta_pk_ref.set(pk_field))
    }

    return ops
}

module.exports = {
    getPKField,
    getCurrentKey,
    fetchAddPrimaryKey_T,

    updateOps
}
