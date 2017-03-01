const assert = require('assert');

const utils = require('../utils');

const scan = require('./scan');
const kv = require('../db/kv');
const pks = require('./meta/pks');
const fks = require('./meta/fks');
const _schema = require('./meta/schema');
const indices = require('./meta/indices');
const keyEncoding = require('../db/keyEncoding');
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
    return tableMetadata.createMeta(remote, name, pk_field, [pk_field, ...rest]);
}

// See insertInto_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function insertInto(remote, name, mapping) {
    return kv.runT(remote, function(tx) {
        return insertInto_Unsafe(tx, name, mapping);
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
function insertInto_Unsafe(remote, table, mapping) {
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
        .then(schema => {
            // Inserts must specify every field, don't allow nulls by default
            // Easily solvable by inserting a bottom value.
            // TODO: Add bottom value for nullable fields
            return _schema.validateSchema(remote, table, schema).then(r => {
                if (!r) throw new Error('Invalid schema');
                return checkFK_Unsafe(remote, table, mapping);
            });
        })
        .then(valid => {
            if (!valid) throw new Error('FK constraint failed');
            return pks.fetchAddPrimaryKey_T(remote, table);
        })
        .then(pk_value => {
            const field_names = Object.keys(mapping);
            const pk_key = keyEncoding.spk(table, pk_value);
            const field_keys = field_names.map(f => {
                return keyEncoding.field(table, pk_value, f);
            });
            const field_values = field_names.map(f => mapping[f]);

            const keys = field_keys.concat(pk_key);
            const values = field_values.concat(pk_value);

            return kv
                .put(remote, keys, values)
                .then(_ => {
                    return indices.updateIndices(remote, table, pk_value, mapping);
                })
                .then(_ => {
                    return indices.updateUIndices(remote, table, pk_value, mapping);
                });
        });
}

// Given a table, and a map of updated field names to their values,
// check if the new values satisfy foreign key constraints, following that:
//
// - A value X may only be inserted into the child column if X also exists in the parent column.
// - A value X in a child column may only be updated to a value Y if Y exists in the parent column.
//
// Return true if both conditions are met. Foreign keys are represented as regular fields,
// plus some metadata attached to the table. This means that every insert and update has
// to check in the parent table, and updates to the parent table will have to check
// referencing tables. In contrast, reads of foreign keys incur no extra cost.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// TODO: Check infks as well
function checkFK_Unsafe(remote, table, mapping) {
    const field_names = Object.keys(mapping);
    const correlated = fks.correlateFKs_T(remote, table, field_names);

    // TODO: Valid for now, change if primary keys are user defined, and / or when fks
    // may point to arbitrary fields
    //
    // Foreign keys may be only created against primary keys, not arbitrary fields
    // And given that primary keys are only autoincremented, and the database is append-only
    // We can check if a specific row exists by checking it its less or equal to the keyrange
    // The actual logic for the cutoff is implemented inside select
    return correlated.then(relation => {
        const valid_checks = relation.map(({ reference_table, field_name }) => {
            const range = mapping[field_name];
            // FIXME: Change if FK can be against non-primary fields
            const f_select = select(remote, reference_table, field_name, {
                [field_name]: range
            });

            return f_select
                .then(rows => {
                    // FIXME: Use unique index instead
                    assert(rows.length === 1);
                    const value = rows[0][field_name];
                    return value === mapping[field_name];
                })
                .catch(cutoff_error => {
                    console.log(cutoff_error);
                    return false;
                });
        });

        return Promise.all(valid_checks).then(all_checks => {
            return all_checks.every(Boolean);
        });
    });
}

// See select_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function select(remote, table, fields, predicate) {
    return kv.runT(remote, function(tx) {
        return select_Unsafe(tx, table, fields, predicate);
    });
}

// select_Unsafe(_, t, [f1, f2, ..., fn], predicate) will perform
// SELECT f1, f2, ..., fn FROM t where predicate = true
//
// The syntax for the predicate is
// { field_a: (value | values), field_b: (value | values), ...}
//
// This translates to
// SELECT [...]
//   FROM [...]
//  WHERE
//   field_a = value | field_a = value_1 OR value_2 OR ...
//   [AND field_b = value | field_b = value_1 OR value_2 OR ...]
//
// Currently we do not support OR between different fields (like A = B OR C = D).
//
// Supports for wildard select by calling `select_Unsafe(_, _, '*', _)`
//
// Will fail if any of the given fields is not part of the table schema.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// FIXME: Revisit WHERE once JOINS are implemented
// Detect indexed fields and scan the index instead.
// We would need JOIN to support that.
function select_Unsafe(remote, table, fields, predicate) {
    const predicateFields = Object.keys(predicate);
    const f_queriedFields = validateQueriedFields(remote, table, fields);
    const f_predicateFields = validatePredicateFields(remote, table, predicateFields);

    return f_queriedFields.then(queriedFields => {
        return f_predicateFields.then(predicateFields => {
            const f_scanFn = scan.selectScanFn(remote, table, predicateFields);

            return f_scanFn
                .then(scanFn => {
                    return scanFn(remote, table, predicate);
                })
                .then(rows => {
                    // Filter only the rows that satisfy the predicate
                    const filtered = rows.filter(row => {
                        const valid = predicateFields.map(field => {
                            const matchValues = utils.arreturn(predicate[field]);
                            return matchValues.includes(row[field]);
                        });

                        return valid.every(Boolean);
                    });

                    // Extract only the queried fields
                    return filtered.map(row => {
                        return utils.filterOKeys(row, key => queriedFields.includes(key));
                    });
                });
        });
    });
}

function validateQueriedFields(remote, table, field) {
    const queriedFields = utils.arreturn(field);
    if (queriedFields.length === 1 && queriedFields[0] === '*') {
        return _schema.getSchema(remote, table);
    }

    return _schema.validateSchemaSubset(remote, table, queriedFields).then(r => {
        if (!r) {
            throw new Error(`Invalid query fields ${queriedFields} on table ${table}`);
        }

        return queriedFields;
    });
}

function validatePredicateFields(remote, table, field) {
    const predicateFields = utils.arreturn(field);

    return _schema.validateSchemaSubset(remote, table, predicateFields).then(r => {
        if (!r) {
            throw new Error(
                `Invalid predicate fields ${predicateFields} on table ${table}`
            );
        }

        return predicateFields;
    });
}

module.exports = {
    create,
    select,
    insertInto
};
