const kv = require('../db/kv')
const keyEncoding = require('../db/keyEncoding')
const tableMetadata = require('../db/tableMetadata')

// TODO: Right now we pick the first field as primary key,
// but maybe let the user pick?
function create(remote, name, schema) {
    return tableMetadata.createMeta(remote, name, schema[0], schema)
}

// TODO: Take fks and indices into account
// Given the table name, the primary key column name,
// and a mapping of field names to values
// 1. Check schema is correct. If it's not, throw
// 2. Get new pk value by reading the meta keyrange (incrAndGet)
// 3. Inside a transaction:
// 3/1. encode(pk) -> pk_value
// 3/2. for (k, v) in schema: encode(k) -> v
function insertInto(remote, name, mapping, {in_tx} = {in_tx: true}) {
    const runnable = tx => rawInsert(tx, name, mapping)
    if (in_tx) {
        return kv.runT(remote, runnable)
    }

    return runnable(remote)
}

function rawInsert(remote, table, mapping) {
    const fields = Object.keys(mapping)
    const values = fields.map(f => mapping[f])

    return tableMetadata.getPKField(remote, table).then(pk_field => {
        return fields.concat(pk_field)
    }).then(schema => {
        return tableMetadata.validateSchema(remote, table, schema).then(r => {
            if (!r) throw "Invalid schema"
            return tableMetadata.incrAndGetKey(remote, table, {in_tx: false})
        })
    }).then(pk_value => {
        const pk_key = keyEncoding.encodePrimary(table, pk_value)
        return kv.put(remote, pk_key, pk_value).then(_ => {
            const field_keys = fields.map(f => keyEncoding.encodeField(table, pk_value, f))
            return kv.putPar(remote, field_keys, values)
        })
    })
}


module.exports = {
    create,
    insertInto,
}
