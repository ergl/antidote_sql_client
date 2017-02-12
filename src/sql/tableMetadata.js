const kv = require('./../db/kv');
const keyEncoding = require('./../db/keyEncoding');

function createMeta(remote, table_name, pk_field, schema) {
    const meta_key = keyEncoding.encodeTableName(table_name);
    const meta_content = {
        fks: [],
        indices: [],
        schema: schema,
        current_pk_value: 0,
        primary_key_field: pk_field
    };

    return kv.put(remote, meta_key, meta_content);
}

module.exports = {
    createMeta
};
