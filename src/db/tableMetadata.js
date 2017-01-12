const kv = require('./kv')
const keyEncoding = require('./keyEncoding')

function createMeta(remote, table_name) {
    const ops = generateMetaOps(remote, table_name)
    return remote.update(ops)
}

function getNextKey(remote, table_name) {
    const meta_ref = generateMetaRef(remote, table_name)
    const count_key = keyEncoding.encodeMetaCounter(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.counterValue(count_key)
    })
}

function incrKey(remote, table_name) {
    return remote.update(generateMetaOps(remote, table_name, {
        increment: 1,
        indices: 'no_set'
    }))
}

function incrAndGetKey(remote, table_name, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return incrKey(tx, table_name).then(_ => getNextKey(tx, table_name))
    }

    if (in_tx) {
        return kv.runT(remote, runnable).then(({result}) => result)
    }

    return runnable(remote)
}

function getIndices(remote, table_name) {
    const meta_ref = generateMetaRef(remote, table_name)
    const index_key = keyEncoding.encodeMetaIndex(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(index_key)
    })
}

function setIndex(remote, table_name, indices = 'no_set') {
    return remote.update(generateMetaOps(remote, table_name, {
        increment: 0,
        indices: indices
    }))
}

function addIndex(remote, table_name, mapping, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return getIndices(tx, table_name).then(index_table => {
            return setIndex(tx, table_name, index_table.concat(mapping))
        })
    }

    if (in_tx) {
        return kv.runT(remote, runnable)
    }

    return runnable(remote)

}

// TODO: Figure out if we need this
function fieldIndexed(remote, table_name, field) {
    const fields = Array.isArray(field) ? field : [field]
    return getIndices(remote, table_name).then(indices => {
        const index_fields = indices.map(({field}) => field)
        return fields.every(f => {
            return index_fields.includes(f)
        })
    })
}

function indexOfField(remote, table_name, indexed_field) {
    return getIndices(remote, table_name).then(indices => {
        const index = indices.reduce((acc, {field, index_name}) => {
            if (field === indexed_field) {
                return acc.concat(index_name)
            }

            return acc
        }, [])
        return index.length === 0 ? index[0] : index
    })
}

function generateMetaOps(remote, table_name, opts = {increment: 0, indices: []}) {
    const inc = opts.increment || 0
    const index_tuples = opts.indices || []

    const meta_ref = generateMetaRef(remote, table_name)
    const keyrange = meta_ref.counter(keyEncoding.encodeMetaCounter(table_name))
    const indices = meta_ref.register(keyEncoding.encodeMetaIndex(table_name))

    if (index_tuples === 'no_set') {
        return [keyrange.increment(inc)]
    }

    return [
        keyrange.increment(inc),
        indices.set(index_tuples)
    ]
}

function generateMetaRef(remote, table_name) {
    return remote.map(keyEncoding.encodeMeta(table_name))
}

module.exports = {
    createMeta,
    incrAndGetKey,
    getNextKey,
    addIndex,
    indexOfField,
    fieldIndexed
}
