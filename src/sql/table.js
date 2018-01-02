// @ts-check

const kv = require('../db/kv');
const tableMetadata = require('./tableMetadata');

// TODO: Support user-defined primary keys (and non-numeric)
// If allowed, should create an unique index on it
// TODO: Allow null values into the database by omitting fields
function create(remote, name, schema) {
    // Pick the head of the schema as an autoincremented primary key
    // Sort the schema so it has the same order as in the key set
    // (see orderedKeySet)
    // TODO: Use locale-sensitive sort?
    const [pk_field, ...rest] = schema;
    rest.sort();
    return tableMetadata.createMeta(remote, name, pk_field, [
        pk_field,
        ...rest
    ]);
}

function reset(remote) {
    return kv.runT(remote, kv.reset);
}

module.exports = {
    create,
    reset
};
