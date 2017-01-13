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

// TODO: Maybe change range from a simple array into something more comples
function scan(remote, table, range, {in_tx} = {in_tx: true}) {
    const runnable = tx => rawScan(tx, table, range)

    if (in_tx && remote.startTransaction !== undefined) {
        return kv.runT(remote, runnable)
    }

    return runnable(remote)
}

// For every key in range:
// - Read that key encoding
// -- For every field in schema (without pk field)
// -- Read encoding(key+field)
// TODO: Compare range against keyrange and throw on key outside of it?
// TODO: Support index keys
function rawScan(remote, table, range) {
    const f_schema = tableMetadata.getSchema(remote, table)
    return f_schema.then(schema => {
        const keys = range.map(k => keyEncoding.encodePrimary(table, k))

        const f_pk_field = tableMetadata.getPKField(remote, table)
        const f_only_fields = f_pk_field.then(pk_field => schema.filter(f => f !== pk_field))

        const f_results = keys.map((key, idx) => {
            return f_pk_field.then(pk_field => {
                return f_only_fields.then(only_fields => {
                    const field_keys = only_fields.map(f => keyEncoding.encodeField(table, range[idx], f))
                    return scanFields(remote, field_keys.concat(key), only_fields.concat(pk_field))
                })
            })
        })

        return Promise.all(f_results).catch(_ => {
            // FIXME: Return something else, this will swallow all other values
            return []
        })
    })
}

function scanFields(remote, field_keys, fields) {
    return kv.get(remote, field_keys).then(values => {
        return values.reduce((acc, val, idx) => {
            const field_name = fields[idx]
            return Object.assign(acc, {[field_name]: val})
        }, {})
    })
}

module.exports = {
    create,
    insertInto,
    scan
}
