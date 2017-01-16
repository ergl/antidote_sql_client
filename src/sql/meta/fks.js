const kv = require('./../../db/kv')
const schema = require('./schema')
const metaCont = require('./metaCont')
const keyEncoding = require('./../../db/keyEncoding')

// See addFK_Unsafe for details.
//
// This function will start a new transaction by default. However,
// given that the current antidote API doesn't allow nested transactions, this function
// must be called with `{in_tx: true}` if used inside another transaction.
//
function addFK_T(remote, table_name, mapping, {in_tx} = {in_tx: false}) {
    const runnable = tx => addFK_Unsafe(tx, table_name, mapping)

    if (in_tx) {
        return runnable(remote)
    }

    return kv.runT(remote, runnable)
}

// Given a table name, and a list of maps `{field_name, reference_table}`,
// create a foreign key for every element of the map list, such that
// `table.field_name` will be the foreign key pointing to `reference_table.field_name`
//
// Will fail if:
// a) This table – or any of the given `reference_table`s – don't exist
// b) Any of the given fields don't exist.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function addFK_Unsafe(remote, table_name, mapping) {
    // NOTE: Assumes you can't add more than FK per field.
    // Therefore we assume that `table_mapping` doesn't contain duplicates.
    const table_mapping = Array.isArray(mapping) ? mapping : [mapping]
    const reference_fields = table_mapping.map(o => o.field_name)

    // For all reference tables, check that
    // a) it exists
    // b) the field exists in its schema
    const constraints = table_mapping.map(({field_name, reference_table}) => {
        return schema.validateSchemaSubset(remote, reference_table, [field_name])
    })

    // We also add the constraint that the given fields are in our schema
    constraints.push(schema.validateSchemaSubset(remote, table_name, reference_fields))

    // Check if all the constraints are satisfied
    const check = Promise.all(constraints).then(r => r.reduce((prev, curr) => prev && curr))

    return check.then(r => {
        if (!r) throw "Can't add fk on non-existent field"

        return getFKs(remote, table_name).then(fk_tuples => {
            return setFK(remote, table_name, fk_tuples.concat(table_mapping))
        })
    })
}

// Given a table name, return a list of maps
// `{field_name, reference_table} describing the foreign keys of that table.
//
// Will return the empty list if there are no foreign keys.
//
function getFKs(remote, table_name) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    const fk_tuples_key = keyEncoding.encodeMetaFK(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(fk_tuples_key)
    }).then(fks => fks === undefined ? [] : fks)
}

// setFK(r, t, fk) will set the fks map list of the table `t` to `fk`
function setFK(remote, table_name, fks) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    return remote.update(updateOps(meta_ref, table_name, {fks: fks}))
}

// Generate the appropiate update operations to set the fks in the meta table
function updateOps(meta_ref, table_name, {fks}) {
    const meta_fk_ref = meta_ref.register(keyEncoding.encodeMetaFK(table_name))
    return meta_fk_ref.set(fks)
}

// Given a table name and one of its field, return a list of reference tables
// if that field is a foreign key, or the empty list otherwise.
function getForeignTable(remote, table_name, fk_field) {
    return getFKs(remote, table_name).then(fk_tuples => {
        const match_fk = ({field_name}) => {
            return field_name === fk_field
        }

        return fk_tuples.filter(match_fk).map(({reference_table}) => reference_table)
    })
}

// Given a table and one of its fields, check if that
// field is a foreign key.
function isFK(remote, table_name, field) {
    return getFKs(remote, table_name, field).then(r => r.length !== 0)
}

module.exports = {
    isFK,
    getForeignTable,

    addFK_T,

    updateOps
}
