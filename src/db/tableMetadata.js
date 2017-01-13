const kv = require('./kv')
const keyEncoding = require('./keyEncoding')

function createMeta(remote, table_name, schema) {
    const ops = generateMetaOps(remote, table_name, {
        increment: 0,
        indices: [],
        schema: schema
    })
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
        indices: 'no_set',
        schema: 'no_set'
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

function getNextIndexKey(remote, table_name, index_name) {
    const ref = generateIndexRef(remote, table_name, index_name)
    return ref.read()
}

function incrIndexKey(remote, table_name, index_name) {
    const ref = generateIndexRef(remote, table_name, index_name)
    return remote.update(ref.increment(1))
}

function incrAndGetIndexKey(remote, table_name, index_name, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return incrIndexKey(tx, table_name, index_name).then(_ => {
            return getNextIndexKey(tx, table_name, index_name)
        })
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
        indices: indices,
        schema: 'no_set'
    }))
}

function addIndex(remote, table_name, mapping, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return getSchema(remote, table_name).then(schema => {
            if (!schema.includes(mapping.field)) {
                throw "Can't add index on non-existent field"
            }

            return getIndices(tx, table_name).then(index_table => {
                return setIndex(tx, table_name, index_table.concat(mapping))
            })
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

function getSchema(remote, table_name) {
    const meta_ref = generateMetaRef(remote, table_name)
    const schema_key = keyEncoding.encodeMetaSchema(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(schema_key)
    })
}

function validateSchema(remote, table_name, schema) {
    return getSchema(remote, table_name).then(sch => {
        return schema.every(f => sch.includes(f))
    })
}

function generateMetaOps(remote, table_name, opts = {increment: 0, indices: [], schema: []}) {
    const inc = opts.increment || 0
    const index_tuples = opts.indices || []
    const schema_list = opts.schema || []

    const meta_ref = generateMetaRef(remote, table_name)
    const keyrange = meta_ref.counter(keyEncoding.encodeMetaCounter(table_name))
    const indices = meta_ref.register(keyEncoding.encodeMetaIndex(table_name))
    const schema = meta_ref.register(keyEncoding.encodeMetaSchema(table_name))

    const ops = [keyrange.increment(inc)]

    if (index_tuples !== 'no_set') {
        ops.push(indices.set(index_tuples))
    }

    if (schema !== 'no_set') {
        ops.push(schema.set(schema_list))
    }

    return ops
}

function generateMetaRef(remote, table_name) {
    return remote.map(keyEncoding.encodeTableName(table_name))
}

function generateIndexRef(remote, table_name, index_name) {
    return remote.counter(keyEncoding.encodeIndex(table_name, index_name))
}

module.exports = {
    createMeta,
    validateSchema,

    incrAndGetKey,
    incrAndGetIndexKey,

    addIndex,
    indexOfField
}
