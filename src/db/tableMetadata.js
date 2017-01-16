const kv = require('./kv')
const keyEncoding = require('./keyEncoding')

function createMeta(remote, table_name, pk_field, schema) {
    const ops = generateMetaOps(remote, table_name, {
        increment: 0,
        indices: [],
        fks: [],
        schema,
        pk_field
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
    return remote.update(generateMetaOps(remote, table_name, { increment: 1 }))
}

function fetchAddPrimaryKey_T(remote, table_name, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return incrKey(tx, table_name).then(_ => getNextKey(tx, table_name))
    }

    if (in_tx) {
        return kv.runT(remote, runnable)
    }

    return runnable(remote)
}

function getNextIndexKeyRaw(remote, table_name, index_name, {in_tx} = {in_tx: true}) {
    return isIndex(remote, table_name, index_name).then(r => {
        if (!r) throw `${index_name} doesn't reference a field in ${table_name}`

        const ref = generateIndexRef(remote, table_name, index_name)
        return ref.read()
    })
}

function incrIndexKey(remote, table_name, index_name) {
    const ref = generateIndexRef(remote, table_name, index_name)
    return remote.update(ref.increment(1))
}

function fetchAddIndexKey_T(remote, table_name, index_name, {in_tx} = {in_tx: true}) {
    const runnable = tx => {
        return incrIndexKey(tx, table_name, index_name).then(_ => {
            return getNextIndexKeyRaw(tx, table_name, index_name)
        })
    }

    if (in_tx) {
        return kv.runT(remote, runnable)
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

function setIndex(remote, table_name, indices) {
    return remote.update(generateMetaOps(remote, table_name, { indices: indices }))
}

function addIndex_T(remote, table_name, mapping, {in_tx} = {in_tx: true}) {
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

function indexOfField(remote, table_name, indexed_field) {
    return getIndices(remote, table_name).then(indices => {
        if (indices === undefined) throw `Cant't locate table ${table_name}`

        const index = indices.reduce((acc, {field, index_name}) => {
            if (field === indexed_field) {
                return acc.concat(index_name)
            }

            return acc
        }, [])
        return index.length === 0 ? index[0] : index
    })
}

// Given a table and an index name, checks if the index
// references a field in that table
function isIndex(remote, table_name, index_name) {
    return getIndices(remote, table_name).then(indices => {
        if (indices === undefined) throw `Cant't locate table ${table_name}`

        return indices.map(({index_name}) => index_name).includes(index_name)
    })
}

// Get the field list of the given table
// If the table doesn't exist, return the empty list
function getSchema(remote, table_name) {
    const meta_ref = generateMetaRef(remote, table_name)
    const schema_key = keyEncoding.encodeMetaSchema(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(schema_key)
    }).then(sch => sch === undefined ? [] : sch)
}

// Check if the given field list matches _exactly_ the
// schema of the given table name
function validateSchema(remote, table_name, schema) {
    return getSchema(remote, table_name).then(sch => {
        return sch.every(f => schema.includes(f))
    })
}

// Check if the given field list is a subset of the
// schema of the given table name
function validateSchemaSubset(remote, table_name, fields) {
    return getSchema(remote, table_name).then(sch => {
        return fields.every(f => sch.includes(f))
    })
}

function getPKField(remote, table_name) {
    const meta_ref = generateMetaRef(remote, table_name)
    const pk_field_key = keyEncoding.encodeMetaPK(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(pk_field_key)
    })
}

// Given a table name, return a map of foreign keys
// where the map contains `field_name` and `reference_table`
// If the table doesn't exist, return the empty list.
function getFKs(remote, table_name) {
    const meta_ref = generateMetaRef(remote, table_name)
    const fk_tuples_key = keyEncoding.encodeMetaFK(table_name)
    return meta_ref.read().then(meta_values => {
        return meta_values.registerValue(fk_tuples_key)
    }).then(fks => fks === undefined ? [] : fks)
}

// Given a table name, swap its foreign key map
// with the given one.
function setFK(remote, table_name, fks) {
    return remote.update(generateMetaOps(remote, table_name, { fks: fks }))
}

// Given a table name, and a list of maps `{field_name, reference_table}`,
// adds a foreign key on `table.field_name`, pointing to `reference_table.field_name`,
// for every element of the map list.
// Fails if:
// a) This table, or any of the given `reference_table`s don't exist
// b) Any of the given fields don't exist.
function addFK_T(remote, table_name, mapping, {in_tx} = {in_tx: true}) {
    // FIXME: Assumes you can't add more than FK per field
    // This array may contain duplicates, but we assume it doesn't
    const table_mapping = Array.isArray(mapping) ? mapping : [mapping]
    const reference_fields = table_mapping.map(({field_name}) => field_name)

    const runnable = tx => {
        // For all reference tables, check that
        // a) that table exists
        // b) the field referenced is in the foreign table schema
        const constraints = table_mapping.map(({field_name, reference_table}) => {
            return validateSchemaSubset(tx, reference_table, [field_name])
        })

        // We also add the constraint that the given fields are in our schema
        constraints.push(validateSchemaSubset(tx, table_name, reference_fields))

        // Check if all the constraints are satisfied
        const check = Promise.all(constraints).then(r => r.reduce((prev, curr) => prev && curr))

        return check.then(r => {
            if (!r) throw "Can't add fk on non-existent field"

            return getFKs(tx, table_name).then(fk_tuples => {
                return setFK(tx, table_name, fk_tuples.concat(table_mapping))
            })
        })
    }

    if (in_tx) {
        return kv.runT(remote, runnable)
    }

    return runnable(remote)
}


function getForeignTable_T(remote, table_name, fk_field, {in_tx} = {in_tx: true}) {
    const run = tx => getForeignTable(tx, table_name, fk_field)

    if (in_tx) {
        return kv.runT(remote, run)
    }

    return getForeignTable(remote)
}

// Given a table name, and one of its fields, return the associated
// foreign key table, if it exists, or undefined otherwise.
//
// If the given field references more than one table, only the first one
// is returned.
function getForeignTable(remote, table_name, fk_field) {
    return getFKs(remote, table_name).then(fk_tuples => {
        const tuples = fk_tuples.filter(({field_name}) => {
            return (field_name === fk_field)
        })

        if (tuples.length === 0) return undefined

        // Only return first
        return tuples[0].reference_table
    })
}

function generateMetaOps(remote, table_name, opts) {
    const inc = opts.increment || 0
    const index_tuples = opts.indices || null
    const schema_list = opts.schema || null
    const primary_key_field = opts.pk_field || null
    const fk_tuples = opts.fks || null

    const meta_ref = generateMetaRef(remote, table_name)
    const keyrange = meta_ref.counter(keyEncoding.encodeMetaCounter(table_name))
    const indices = meta_ref.register(keyEncoding.encodeMetaIndex(table_name))
    const schema = meta_ref.register(keyEncoding.encodeMetaSchema(table_name))
    const primary = meta_ref.register(keyEncoding.encodeMetaPK(table_name))
    const fks = meta_ref.register(keyEncoding.encodeMetaFK(table_name))

    const ops = [keyrange.increment(inc)]

    if (index_tuples !== null) {
        ops.push(indices.set(index_tuples))
    }

    if (schema_list !== null) {
        ops.push(schema.set(schema_list))
    }

    if (primary_key_field !== null) {
        ops.push(primary.set(primary_key_field))
    }

    if (fk_tuples !== null) {
        ops.push(fks.set(fk_tuples))
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

    getPKField,

    getFKs,
    addFK_T,
    getForeignTable_T,

    validateSchema,
    validateSchemaSubset,

    fetchAddIndexKey_T,
    fetchAddPrimaryKey_T,

    addIndex_T,
    indexOfField
}
