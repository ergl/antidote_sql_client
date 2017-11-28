const utils = require('../../utils');

const kv = require('./../../db/kv');
const keyEncoding = require('./../../db/keyEncoding');

// Get the field list of the given table
// If the table doesn't exist, return the empty list
function getSchema(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.get(remote, meta_key, { fromCache: true }).then(meta => {
        const schema = meta.schema;
        return schema === undefined ? [] : schema;
    });
}

// Check if the given field list matches _exactly_ the
// schema of the given table name
function validateSchema(remote, table_name, schema) {
    return getSchema(remote, table_name).then(sch => {
        return sch.every(f => schema.includes(f));
    });
}

// Check if the given field list is a subset of the
// schema of the given table name
function validateSchemaSubset(remote, table_name, field) {
    const fields = utils.arreturn(field);
    return getSchema(remote, table_name).then(sch => {
        return fields.every(f => sch.includes(f));
    });
}

module.exports = {
    getSchema,
    validateSchema,
    validateSchemaSubset
};
