const utils = require('../utils')

const kv = require('../db/kv')
const pks = require('./meta/pks')
const _schema = require('./meta/schema')
const indices = require('./meta/indices')
const keyEncoding = require('../db/keyEncoding')
const tableMetadata = require('./tableMetadata')

// TODO: Support user-defined primary keys
function create(remote, name, schema) {
    // Pick the head of the schema as an autoincremented primary key
    return tableMetadata.createMeta(remote, name, schema[0], schema)
}

// See insertInto_Unsafe for details.
//
// This function will start a new transaction by default. However,
// given that the current antidote API doesn't allow nested transactions, this function
// must be called with `{in_tx: true}` if used inside another transaction.
//
function insertInto_T(remote, name, mapping, {in_tx} = {in_tx: false}) {
    const runnable = tx => insertInto_Unsafe(tx, name, mapping)

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable, {ignore_ct: false})
}

// Given a table name, and a map of field names to values,
// insert those values into the table.
//
// If the primary key is in autoincrement mode, then the given map
// must not contain the primary key field or a value for it.
//
// The given field map must be complete, as there is no support for
// nullable fields right now. Will fail if the schema is not complete.
// (Sans the pk field restriction pointed above).
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// TODO: Take fks into account
// TODO: Allow null values into the database by omitting fields
// TODO: Support non-numeric primary key values
function insertInto_Unsafe(remote, table, mapping) {
    // 1 - Check schema is correct. If it's not, throw
    // 2 - Get new pk value by reading the meta keyrange (incrAndGet)
    // 3 - Inside a transaction:
    // 3.1 - encode(pk) -> pk_value
    // 3.2 - for (k, v) in schema: encode(k) -> v
    const fields = Object.keys(mapping)
    const values = fields.map(f => mapping[f])

    // Only support autoincrement keys, so calls to insert must not contain
    // the primary key. Hence, we fetch the primary key field name here.
    return pks.getPKField(remote, table).then(pk_field => {
        return fields.concat(pk_field)
    }).then(schema => {
        // Inserts must specify every field, don't allow nulls by default
        // FIXME: Easily solvable by inserting a bottom value.
        return _schema.validateSchema(remote, table, schema).then(r => {
            if (!r) throw "Invalid schema"

            // We MUST be inside a transaction, so the call
            // to `fetchAddPrimaryKey_T` MUST NOT spawn a new transaction.
            return pks.fetchAddPrimaryKey_T(remote, table, {in_tx: true})
        })
    }).then(pk_value => {
        const pk_key = keyEncoding.encodePrimary(table, pk_value)
        const field_keys = fields.map(f => keyEncoding.encodeField(table, pk_value, f))
        const prepare_indices = prepareBatchIndexInsert_Unsafe(remote, table, pk_value, fields)

        return prepare_indices.then(({keys: index_keys, values: index_values}) => {
            return {
                keys: field_keys.concat(pk_key).concat(index_keys),
                values: values.concat(pk_value).concat(index_values)
            }
        })
    }).then(({keys, values}) => {
        return kv.put(remote, keys, values)
    })
}

// For indexes:
// When inserting, check if any of the inserted
// fields has an index on the meta.
// For every field f that has an index
// index_name | (f, index_name) in meta.index
// put(FAA_index_key(index_name)/f, key(f))
// Depending
// FIXME: Support indices over more than one field
function prepareBatchIndexInsert_Unsafe(remote, table, pk, updated_fields) {
    const update_single = f => {
        // Get all the indices referencing this field
        return indices.indexOfField(remote, table, f).then(to_update => {
            const f_spec = to_update.map(idx => prepareSingleIndexInsert_Unsafe(remote, table, idx, {
                [f]: keyEncoding.encodeField(table, pk, f)
            }))

            return Promise.all(f_spec)
        })
    }

    const after = updated_fields.map(update_single)

    return Promise.all(after).then(res => {
        const to_update = utils.flatten(res)
        return to_update.reduce((acc, {keys, values}) => {
            return Object.assign(acc, {
                keys: acc.keys.concat(keys),
                values: acc.values.concat(values)
            })
        }, {keys: [], values: []})
    })
}

function prepareSingleIndexInsert_Unsafe(remote, table, index, mapping) {
    const fields = Object.keys(mapping)
    const values = fields.map(f => mapping[f])

    return indices.fetchAddIndexKey_T(remote, table, index, {in_tx: true}).then(pk_value => {
        const keys = fields.map(f => keyEncoding.encodeIndexField(table, index, pk_value, f))
        return {keys, values}
    })
}

// See select_Unsafe for details.
//
// This function will start a new transaction by default. However,
// given that the current antidote API doesn't allow nested transactions, this function
// must be called with `{in_tx: true}` if used inside another transaction.
//
function select_T(remote, table, fields, pk_value, {in_tx} = {in_tx: false}) {
    const run = tx => select_Unsafe(tx, table, fields, pk_value)

    if (in_tx) {
        return run(remote)
    }

    return kv.runT(remote, run)
}

// select_Unsafe(_, t, [f1, f2, ..., fn], pk) will perform
// SELECT f1, f2, ..., fn FROM t where {pk_field} = pk
//
// Only supports predicates against the primary key field, and restricted
// to the form `id = x` or `id = a AND id = b (...) AND id = z`. To select
// more than one key, pass a key list as the last parameter:
// `select_Unsafe(_, _, _, [k1, k2, ..., kn])`.
//
// Supports for wildard select by calling `select_Unsafe(_, _, '*', _)`
//
// Will fail if any of the given fields is not part of the table schema.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// TODO: Support complex predicates
function select_Unsafe(remote, table, field, pk_value) {
    const pk_values = utils.arreturn(pk_value)
    const fields = utils.arreturn(field)

    const perform_scan = lookup_fields => {
        // We MUST be inside a transaction, so the call to `scan_T` MUST NOT spawn a new transaction.
        return scan_T(remote, table, pk_values, {in_tx: true}).then(res => res.map(row => {
            return Object.keys(row)
                .filter(k => lookup_fields.includes(k))
                .reduce((acc, k) => Object.assign(acc, {[k]: row[k]}), {})
        }))
    }

    // If we query '*', get the entire schema
    if (fields.length === 1 && fields[0] === '*') {
        return _schema.getSchema(remote, table).then(schema => perform_scan(schema))
    }

    return _schema.validateSchemaSubset(remote, table, fields).then(r => {
        if (!r) throw "Invalid schema"
        return perform_scan(fields)
    })

}

// See scan_Unsafe for details.
//
// This function will start a new transaction by default. However,
// given that the current antidote API doesn't allow nested transactions, this function
// must be called with `{in_tx: true}` if used inside another transaction.
//
function scan_T(remote, table, range, {in_tx} = {in_tx: false}) {
    const runnable = tx => scan_Unsafe(tx, table, range)

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable)
}

// Given a table name, and a list of primary keys, will recursively
// retrieve all the subkeys of each key, returning a list of rows.
//
// Will fail if the scan goes out of bounds of max(table.pk_value)
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// TODO: Support any key with subkeys (pks, index names, index pks)
// TODO: Change range from array to something more complex
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

// Given a list of encoded keys, and a matching list of field names,
// build an object s.t. `{f: get(k)}` for every k in field_keys, f in fields
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
