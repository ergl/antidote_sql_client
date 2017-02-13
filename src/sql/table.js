const assert = require('assert');

const utils = require('../utils');

const kv = require('../db/kv');
const pks = require('./meta/pks');
const fks = require('./meta/fks');
const _schema = require('./meta/schema');
const indices = require('./meta/indices');
const legacyEncoding = require('../db/keyEncoding');
const keyEncoding = require('../kset/keyEncoding');
const tableMetadata = require('./tableMetadata');

// TODO: Support user-defined primary keys (and non-numeric)
// TODO: Allow null values into the database by omitting fields
function create(remote, name, schema) {
    // Pick the head of the schema as an autoincremented primary key
    return tableMetadata.createMeta(remote, name, schema[0], schema);
}

// See insertInto_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function insertInto_T(remote, name, mapping) {
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
                if (!r) throw 'Invalid schema';

                return swapFKReferences_Unsafe(remote, table, mapping);
            });
        })
        .then(({ valid, result }) => {
            if (!valid) throw 'FK constraint failed';
            const f_pk_value = pks.fetchAddPrimaryKey_T(remote, table);
            return f_pk_value.then(pk_value => ({ pk_value, result }));
        })
        .then(({ pk_value, result }) => {
            const field_names = Object.keys(result);
            const pk_key = keyEncoding.spk(table, keyEncoding.d_int(pk_value));
            const field_keys = field_names.map(f => {
                return keyEncoding.field(table, keyEncoding.d_int(pk_value), f);
            });
            const field_values = field_names.map(f => mapping[f]);

            const keys = field_keys.concat(pk_key);
            const values = field_values.concat(pk_value);

            return kv.put(remote, keys, values).then(_ => {
                return updateIndices(remote, table, pk_value, mapping);
            });
        });
}

// Given a table, and a map of updated field names to their values,
// check if the new values satisfy foreign key constraints, following that:
//
// - A value X may only be inserted into the child column if X also exists in the parent column.
// - A value X in a child column may only be updated to a value Y if Y exists in the parent column.
//
// If both conditions are met, return { valid: bool, result: mapping } where result represents the new
// mapping of fields -> values to be inserted in the database. Foreign keys are represented as pointers
// to the actual value they reference, obviating extra work during updates at the cost of an extra `get`
// off the database when reading that field.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function swapFKReferences_Unsafe(remote, table, mapping) {
    const field_names = Object.keys(mapping);
    const correlated = fks.correlateFKs_T(remote, table, field_names);

    // TODO: Valid for now, change if primary keys are user defined, and / or when fks
    // may point to arbitrary fields
    //
    // Foreign keys may be only created against primary keys, not arbitrary fields
    // And given that primary keys are only autoincremented, and the database is append-only
    // We can check if a specific row exists by checking it its less or equal to the keyrange
    // The actual logic for the cutoff is implemented inside select_T
    return correlated.then(relation => {
        const valid_checks = relation.map(({ reference_table, field_name }) => {
            const range = mapping[field_name];
            const f_select = select_T(remote, reference_table, field_name, range);
            const valid = f_select
                .then(row => {
                    assert(row.length === 1);
                    const value = row[0][field_name];
                    return value === mapping[field_name];
                })
                .catch(cutoff_error => {
                    console.log(cutoff_error);
                    return false;
                });

            return valid.then(v => {
                if (!v) throw 'FK constraint failed';
                return {
                    k: field_name,
                    v: keyEncoding.spk(table, keyEncoding.d_int(mapping[field_name]))
                };
            });
        });

        return Promise.all(valid_checks)
            .then(to_swap => {
                const swapped = to_swap.reduce(
                    (acc, { k, v }) => {
                        return Object.assign(acc, { [k]: v });
                    },
                    mapping
                );

                return {
                    valid: true,
                    result: swapped
                };
            })
            .catch(_ => {
                return {
                    valid: false,
                    result: undefined
                };
            });
    });
}

function updateIndices(remote, table, fk_value, mapping) {
    const field_names = Object.keys(mapping);
    const correlated = indices.legacy__correlateIndices_T(remote, table, field_names);

    return correlated.then(relation => {
        const ops = relation.reduce(
            (acc, { index_name, field_names }) => {
                // FIXME: field f might not be in the mapping
                const field_values = field_names.map(f => mapping[f]);
                const pr = updateSingleIndex(
                    remote,
                    table,
                    index_name,
                    fk_value,
                    field_names,
                    field_values
                );
                return Promise.all([acc, pr]).then(res => {
                    const [acc, { keys, values }] = res;
                    return {
                        keys: acc.keys.concat(keys),
                        values: acc.values.concat(values)
                    };
                });
            },
            Promise.resolve({ keys: [], values: [] })
        );

        return ops.then(({ keys, values }) => {
            return kv.put(remote, keys, values);
        });
    });
}

// FIXME: Generate super keys
// When adding to the kset, we should auto-insert the appropiate super keys to support
// subkey range scans. (Or maybe `subkeys` should be smarter thant that and derive the
// appropiate scan.
function updateSingleIndex(_, table, index, fk_value, field_names, field_values) {
    const index_keys = field_names.map((fld_name, i) => {
        return keyEncoding.index_key(
            table,
            index,
            fld_name,
            keyEncoding.d_string(field_values[i]),
            keyEncoding.d_int(fk_value)
        );
    });

    // TODO: Don't put these keys
    // Just sentinel keys, should add them to the kset instead
    const index_values = field_names.map(_ => undefined);

    return { keys: index_keys, values: index_values };
}

// See select_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function select_T(remote, table, fields, pk_value) {
    return kv.runT(remote, function(tx) {
        return select_Unsafe(tx, table, fields, pk_value);
    });
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
    const pk_values = utils.arreturn(pk_value);
    const fields = utils.arreturn(field);

    const perform_scan = lookup_fields => {
        // We MUST be inside a transaction, so the call to `scan_T` MUST NOT spawn a new transaction.
        return scan_T(remote, table, pk_values).then(res => res.map(row => {
            return Object.keys(row)
                .filter(k => lookup_fields.includes(k))
                .reduce((acc, k) => Object.assign(acc, { [k]: row[k] }), {});
        }));
    };

    // If we query '*', get the entire schema
    if (fields.length === 1 && fields[0] === '*') {
        return _schema.getSchema(remote, table).then(schema => perform_scan(schema));
    }

    return _schema.validateSchemaSubset(remote, table, fields).then(r => {
        if (!r) throw 'Invalid schema';
        return perform_scan(fields);
    });
}

// See scan_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function scan_T(remote, table, range) {
    return kv.runT(remote, function(tx) {
        return scan_Unsafe(tx, table, range);
    });
}

function scanIndex_T(remote, table, index_name, range) {
    return kv.runT(remote, function(tx) {
        return scanIndex_Unsafe(tx, table, index_name, range);
    });
}

function scanIndex_Unsafe(remote, table, index_name, range) {
    // Assumes keys are numeric
    const f_cutoff = indices.legacy__getIndexKey_T(remote, table, index_name).then(m => {
        return range.find(e => e > m);
    });

    return f_cutoff
        .then(cutoff => {
            if (cutoff !== undefined)
                throw `Error: scan key ${cutoff} out of valid range`;
            return indices.fieldsOfIndex(remote, table, index_name);
        })
        .then(indexed_fields_names => {
            // For every k in key range, encode k
            const keys = utils.flatten(
                range.map(k => {
                    return indexed_fields_names.map(f => {
                        // FIXME: Right now we're getting only the value, at this key
                        // The key encodeIndexPrimary(table, index_name, k) points to the
                        // pk key of those fields. Follow that if we need a join
                        // FIXME: Change to new encoding
                        return legacyEncoding.encodeIndexField(table, index_name, k, f);
                    });
                })
            );

            return kv.get(remote, keys);
        });
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
        return range.find(e => e > m);
    });

    return f_cutoff
        .then(cutoff => {
            if (cutoff !== undefined)
                throw `Error: scan key ${cutoff} out of valid range`;
            return _schema.getSchema(remote, table);
        })
        .then(schema => {
            // For every k in key range, encode k
            const keys = range.map(k => keyEncoding.spk(table, keyEncoding.d_int(k)));

            // Get the primary key field.
            const f_pk_field = pks.getPKField(remote, table);

            // And remove if from the schema, as the pk field is encoded differently.
            const f_non_pk_fields = f_pk_field.then(pk_field =>
                schema.filter(f => f !== pk_field));

            // For every key, fetch and read the field subkeys
            const f_results = keys.map((key, idx) => {
                // `Promise.all` guarantees the same order in promises and results
                return Promise.all([f_pk_field, f_non_pk_fields]).then((
                    [pk_field, fields]
                ) => {
                    const field_keys = fields.map(f => {
                        return keyEncoding.field(table, keyEncoding.d_int(range[idx]), f);
                    });
                    // After encoding all fields, we append the pk key/field to the range we want to scan
                    return scanRow(
                        remote,
                        field_keys.concat(key),
                        fields.concat(pk_field)
                    );
                });
            });

            // Execute all scanRow calls in parallel
            return Promise.all(f_results);
        });
}

// Given a list of encoded keys, and a matching list of field names,
// build an object s.t. `{f: get(k)}` for every k in field_keys, f in fields
function scanRow(remote, field_keys, fields) {
    return kv.get(remote, field_keys).then(values => {
        return values.reduce(
            (acc, val, idx) => {
                const field_name = fields[idx];
                return Object.assign(acc, { [field_name]: val });
            },
            {}
        );
    });
}

module.exports = {
    create,
    scan_T,
    select_T,
    insertInto_T,
    scanIndex_T
};
