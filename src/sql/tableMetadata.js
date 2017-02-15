const kv = require('./../db/kv');
const keyEncoding = require('./../kset/keyEncoding');

function createMeta(remote, table_name, pk_field, schema) {
    const meta_key = keyEncoding.table(table_name);
    const meta_content = {
        fks: [],
        indices: [],
        schema: schema,
        current_pk_value: 0,
        primary_key_field: pk_field
    };

    return kv.runT(remote, function(tx) {
        return kv.put(tx, meta_key, meta_content);
    });
}

module.exports = {
    createMeta
};
