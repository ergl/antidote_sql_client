const metaCont = require('./meta/metaCont')

const pks = require('./meta/pks')
const fks = require('./meta/fks')
const schema = require('./meta/schema')
const indices = require('./meta/indices')

function createMeta(remote, table_name, pk_field, schema) {
    const ops = aggregateOps(remote, table_name, pk_field, schema)
    return remote.update(ops)
}

function aggregateOps(remote, table_name, pk_field, schema_map) {
    const meta_ref = metaCont.metaRef(remote, table_name)

    // This feels so bad, but so good at the same time
    const nested_ops = [pks, fks, schema, indices].map(module => {
        return module.updateOps(meta_ref, table_name, {
            increment_pk: 0,
            pk_field: pk_field,
            schema: schema_map,

            fks: [],
            indices: []
        })
    })

    return flatten(nested_ops)
}

// Because, apparently, JS doesn't have Array.flatten ???
function flatten(arr) {
    return arr.reduce((a, b) => {
        return a.concat(Array.isArray(b) ? flatten(b) : b)
    }, [])
}

module.exports = {
    createMeta
}
