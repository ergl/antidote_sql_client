const kv = require('./kv')
const keyEncoding = require('./keyEncoding')

function createMeta(remote, table_key) {
    const ops = generateMetaOps(remote, table_key)
    return remote.update(ops)
}

function getNextKey(remote, table_key) {
    const meta_ref = generateMetaRef(remote, table_key)
    const count_key = keyEncoding.encodeMetaCounter(table_key)
    return meta_ref.read().then(meta_values => {
        return meta_values.counterValue(count_key)
    })
}

function incrKey(remote, table_key) {
    return remote.update(generateMetaOps(remote, table_key, {
        increment: 1,
        indices: 'no_set'
    }))
}

function incrAndGetKey(remote, table_key, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return incrKey(tx, table_key).then(_ => getNextKey(tx, table_key))
    }

    if (in_tx) {
        return kv.runT(remote, runnable).then(({result}) => result)
    }

    return runnable(remote)
}

function getIndices(remote, table_key) {
    const meta_ref = generateMetaRef(remote, table_key)
    const index_key = keyEncoding.encodeMetaIndex(table_key)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(index_key)
    })
}

function setIndex(remote, table_key, indices = 'no_set') {
    return remote.update(generateMetaOps(remote, table_key, {
        increment: 0,
        indices: indices
    }))
}

function addIndex(remote, table_key, mapping, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return getIndices(tx, table_key).then(index_table => {
            return setIndex(tx, table_key, index_table.concat(mapping))
        })
    }

    if (in_tx) {
        return kv.runT(remote, runnable)
    }

    return runnable(remote)

}

function fieldIndexed(remote, table_key, field) {
    const fields = Array.isArray(field) ? field : [field]
    return getIndices(remote, table_key).then(indices => {
        const index_fields = indices.map(({field}) => field)
        return fields.every(f => {
            return index_fields.includes(f)
        })
    })
}

function indexOfField(remote, table_key, indexed_field) {
    return getIndices(remote, table_key).then(indices => {
        const index = indices.reduce((acc, {field, index_name}) => {
            if (field === indexed_field) {
                return acc.concat(index_name)
            }

            return acc
        }, [])
        return index.length === 0 ? index[0] : index
    })
}

function generateMetaOps(remote, table_key, opts = {increment: 0, indices: []}) {
    const inc = opts.increment || 0
    const index_tuples = opts.indices || []

    const meta_ref = generateMetaRef(remote, table_key)
    const keyrange = meta_ref.counter(keyEncoding.encodeMetaCounter(table_key))
    const indices = meta_ref.register(keyEncoding.encodeMetaIndex(table_key))

    if (index_tuples === 'no_set') {
        return [keyrange.increment(inc)]
    }

    return [
        keyrange.increment(inc),
        indices.set(index_tuples)
    ]
}

function generateMetaRef(remote, table_key) {
    return remote.map(keyEncoding.encodeMeta(table_key))
}

module.exports = {
    createMeta,
    incrAndGetKey,
    getNextKey,
    addIndex,
    indexOfField,
    fieldIndexed
}
