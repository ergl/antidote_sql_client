const kv = require('./../../db/kv')
const schema = require('./schema')
const metaCont = require('./metaCont')
const keyEncoding = require('./../../db/keyEncoding')

// Given a table name, and a map `{field_name, index_name}`,
// create a new index named `index_name` over `table.field_name`
//
// Will fail if the given field name doesn't exist inside the table schema.
//
// This function will start a new transaction by default. However,
// given that the current antidote API doesn't allow nested transactions, this function
// must be called with `{in_tx: true}` if used inside another transaction.
//
function addIndex_T(remote, table_name, mapping, {in_tx} = {in_tx: false}) {
    const runnable = tx => {
        return schema.validateSchemaSubset(remote, table_name, mapping.field_name).then(r => {
            if (!r) throw "Can't add index on non-existent field"

            return getIndices(tx, table_name).then(index_table => {
                return setIndex(tx, table_name, index_table.concat(mapping))
            })
        })
    }

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable)
}

// Given a table name, return a list of maps
// `{field_name, index_name}` describing the indices of that table.
//
// Will return the empty list if there are no indices.
//
function getIndices(remote, table_name) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    const index_key = keyEncoding.encodeMetaIndex(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(index_key)
    }).then(idx => idx === undefined ? [] : idx)
}

// setIndex(r, t, idxs) will set the index map list of the table `t` to `idxs`
function setIndex(remote, table_name, indices) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    return remote.update(updateOps(meta_ref, table_name, {indices: indices}))
}

// Generate the appropiate update operations to set the indices in the meta table
function updateOps(meta_ref, table_name, {indices}) {
    const meta_index_ref = meta_ref.register(keyEncoding.encodeMetaIndex(table_name))
    return meta_index_ref.set(indices)
}

// Given an index name, and the table it references, perform
// a fetch-and-add on its key counter reference, and return the new value.
//
// To perform FAA atomically, this function must start a new transaction. However,
// given that the current antidote API doesn't allow nested transactions, this function
// must be called with `{in_tx: true}` if used inside another transaction.
//
// Will start a new transaction by default.
//
function fetchAddIndexKey_T(remote, table_name, index_name, {in_tx} = {in_tx: false}) {
    const runnable = tx => {
        return incrIndexKey(tx, table_name, index_name).then(_ct => {
            return getIndexKey_Unsafe(tx, table_name, index_name)
        })
    }

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable)
}

// Atomically increment the index key counter value.
function incrIndexKey(remote, table_name, index_name) {
    const ref = generateIndexRef(remote, table_name, index_name)
    return remote.update(ref.increment(1))
}

// Given an index name, and the table it references, return
// the current index key counter value.
//
// Will fail if the given index name doesn't reference the given table.
//
function getIndexKey_Unsafe(remote, table_name, index_name) {
    return isIndex(remote, table_name, index_name).then(r => {
        if (!r) throw `${index_name} doesn't reference a field in ${table_name}`

        const ref = generateIndexRef(remote, table_name, index_name)
        return ref.read()
    })
}

// Given an index name, and the table it references, return
// a reference to the index key counter.
function generateIndexRef(remote, table_name, index_name) {
    return remote.counter(keyEncoding.encodeIndex(table_name, index_name))
}

// Given a table name and one of its field, return a list of indexes,
// or the empty list if no indices are found.
function indexOfField(remote, table_name, indexed_field) {
    return getIndices(remote, table_name).then(indices => {

        // Same as filter(f => f.field_name === indexed_field).map(f => f.index_name)
        const match_index = (acc, {field_name, index_name}) => {
            if (field_name === indexed_field) {
                return acc.concat(index_name)
            }

            return acc
        }

        return indices.reduce(match_index, [])
    })
}

// Given a table and an index name, checks if
// the index references a field in that table.
function isIndex(remote, table_name, idx_name) {
    return getIndices(remote, table_name).then(indices => {
        return indices.map(idx => idx.index_name).includes(idx_name)
    })
}

module.exports = {
    isIndex,
    indexOfField,

    addIndex_T,
    fetchAddIndexKey_T,

    updateOps
}