const metaCont = require('./metaCont')
const keyEncoding = require('./../../db/keyEncoding')

// Get the field list of the given table
// If the table doesn't exist, return the empty list
function getSchema(remote, table_name) {
    const meta_ref = metaCont.metaRef(remote, table_name)
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
function validateSchemaSubset(remote, table_name, field) {
    const fields = Array.isArray(field) ? field : [field]
    return getSchema(remote, table_name).then(sch => {
        return fields.every(f => sch.includes(f))
    })
}

// setSchema(r, t, sch) will set the schema of the table `t` to `sch`
function setSchema(remote, table_name, schema) {
    const meta_ref = metaCont.metaRef(remote, table_name)
    return remote.update(updateOps(meta_ref, table_name, {schema: schema}))
}

// Generate the appropiate update operations to set the indices in the meta table
function updateOps(meta_ref, table_name, {schema}) {
    const schema_ref = meta_ref.register(keyEncoding.encodeMetaSchema(table_name))
    return schema_ref.set(schema)
}

module.exports = {
    getSchema,
    validateSchema,
    validateSchemaSubset,

    updateOps
}
