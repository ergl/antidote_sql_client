const kv = require('../db/kv')
const pks = require('./meta/pks')
const _schema = require('./meta/schema')
const keyEncoding = require('../db/keyEncoding')
const tableMetadata = require('./tableMetadata')

// FIXME: Right now we pick the first field as primary key,
// let the user pick?
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
function insertInto_T(remote, name, mapping, {in_tx} = {in_tx: false}) {
    const runnable = tx => insertInto_Unsafe(tx, name, mapping)

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable, {ignore_ct: false})
}

function insertInto_Unsafe(remote, table, mapping) {
    const fields = Object.keys(mapping)
    const values = fields.map(f => mapping[f])

    return pks.getPKField(remote, table).then(pk_field => {
        return fields.concat(pk_field)
    }).then(schema => {
        return _schema.validateSchema(remote, table, schema).then(r => {
            if (!r) throw "Invalid schema"

            // We MUST be inside a transaction, so the call
            // to `fetchAddPrimaryKey_T` MUST NOT spawn a new transaction.
            return pks.fetchAddPrimaryKey_T(remote, table, {in_tx: true})
        })
    }).then(pk_value => {
        console.log(pk_value)
        const pk_key = keyEncoding.encodePrimary(table, pk_value)
        return kv.put(remote, pk_key, pk_value).then(_ct => {
            const field_keys = fields.map(f => keyEncoding.encodeField(table, pk_value, f))
            return kv.putPar(remote, field_keys, values)
        })
    })
}

// TODO: Support more complex selects
// Right now we only support queries against specific primary keys
function select_T(remote, table, fields, pk_value, {in_tx} = {in_tx: false}) {
    const run = tx => select_Unsafe(tx, table, fields, pk_value)

    if (in_tx) {
        return run(remote)
    }

    return kv.runT(remote, run)
}

// Should always be called from inside a transaction
function select_Unsafe(remote, table, fields, pk_value) {
    const pk_values = Array.isArray(pk_value) ? pk_value : [pk_value]
    const perform_scan = lookup_fields => {
        // We MUST be inside a transaction, so the call to `scan_T` MUST NOT
        // spawn a new transaction.
        return scan_T(remote, table, pk_values, {in_tx: true}).then(res => res.map(row => {
            return Object.keys(row)
                .filter(k => lookup_fields.includes(k))
                .reduce((acc, k) => Object.assign(acc, {[k]: row[k]}), {})
        }))
    }

    // If we query '*', get the entire schema
    if (Array.isArray(fields) && fields.length === 1 && fields[0] === '*') {
        return _schema.getSchema(remote, table).then(schema => perform_scan(schema))
    }

    return _schema.validateSchemaSubset(remote, table, fields).then(r => {
        if (!r) throw "Invalid schema"
        return perform_scan(fields)
    })

}

// TODO: Maybe change range from a simple array into something more comples
function scan_T(remote, table, range, {in_tx} = {in_tx: false}) {
    const runnable = tx => scan_Unsafe(tx, table, range)

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable)
}

// TODO: Support index keys
function scan_Unsafe(remote, table, range) {
    // Assumes keys are numeric
    const f_cutoff = pks.getCurrentKey(remote, table).then(m => {
        return range.find(e => e > m)
    })

    return f_cutoff.then(cutoff => {
        if (cutoff !== undefined) throw `Error: scan key ${cutoff} out of valid range`
        return _schema.getSchema(remote, table)
    }).then(schema => {
        // For every k in key range, encode k
        const keys = range.map(k => keyEncoding.encodePrimary(table, k))

        // Get the primary key field.
        const f_pk_field = pks.getPKField(remote, table)

        // And remove if from the schema, as the pk field is encoded differently.
        const f_non_pk_fields = f_pk_field.then(pk_field => schema.filter(f => f !== pk_field))

        // For every key, fetch and read the field subkeys
        const f_results = keys.map((key, idx) => {
            // `Promise.all` guarantees the same order in promises and results
            return Promise.all([f_pk_field, f_non_pk_fields]).then(([pk_field, fields]) => {
                const field_keys = fields.map(f => keyEncoding.encodeField(table, range[idx], f))
                // After encoding all fields, we append the pk key/field to the range we want to scan
                return scanFields(remote, field_keys.concat(key), fields.concat(pk_field))
            })
        })

        // Execute all scanFields calls in parallel
        return Promise.all(f_results)
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
    scan_T,
    select_T,
    insertInto_T
}
