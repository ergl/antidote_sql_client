// @ts-check

const kv = require('../db/kv');
const pks = require('./meta/pks');
const schema = require('./meta/schema');
const indices = require('./meta/indices');
const fkViolations = require('./fkViolations');
const keyEncoding = require('../db/keyEncoding');

// See internalInsert for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function insert(remote, name, mapping) {
    return kv.runT(remote, function(tx) {
        return internalInsert(tx, name, mapping);
    });
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
function internalInsert(remote, table, mapping) {
    // 1 - Check schema is correct. If it's not, throw
    // 2 - Get new pk value by reading the meta keyrange (incrAndGet)
    // 3 - Inside a transaction:
    // 3.1 - encode(pk) -> pk_value
    // 3.2 - for (k, v) in schema: encode(k) -> v

    // Only support autoincrement keys, so calls to insert must not contain
    // the primary key. Hence, we fetch the primary key field name here.
    // TODO: When adding user-defined PKs, change this
    return pks
        .getPKField(remote, table)
        .then(pk_field => {
            const field_names = Object.keys(mapping);
            return field_names.concat(pk_field);
        })
        .then(insertFields => {
            // Inserts must specify every field, don't allow nulls by default
            // Easily solvable by inserting a bottom value.
            // TODO: Add bottom value for nullable fields
            return schema
                .validateSchema(remote, table, insertFields)
                .then(r => {
                    if (!r) throw new Error('Invalid schema');
                    return fkViolations.checkOutFKs(remote, table, mapping);
                });
        })
        .then(valid => {
            if (!valid) throw new Error('FK constraint failed');
            return pks.fetchAddPrimaryKey(remote, table);
        })
        .then(pk_value => {
            return rawInsert(remote, table, pk_value, mapping);
        });
}

// Given a table, a primary key value, and a map of field names to field values
// (excluding the primary key), insert them into the database. This function will
// not check the validity of the primary key value, or that the fields are part of
// the table schema. However, this function will update all the related indices that
// are associated with this table, if any of the inserted fields is being indexed.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function rawInsert(remote, table, pkValue, mapping) {
    const fieldNames = Object.keys(mapping);
    const pkKey = keyEncoding.spk(table, pkValue);
    const fieldKeys = fieldNames.map(f => keyEncoding.field(table, pkValue, f));
    const fieldValues = fieldNames.map(f => mapping[f]);

    const keys = [pkKey, ...fieldKeys];
    const values = [pkValue, ...fieldValues];

    return kv
        .put(remote, keys, values)
        .then(_ => indices.updateIndices(remote, table, pkValue, mapping))
        .then(_ => {
            return indices.updateUniqueIndices(remote, table, pkValue, mapping);
        });
}

module.exports = {
    insert,
    __unsafeRawInsert: rawInsert
};
