const utils = require('./../../utils');

const kv = require('./../../db/kv');
const schema = require('./schema');
const keyEncoding = require('./../../db/keyEncoding');

// Given a table name, and a map `{index_name, field_names}`,
// create a new index named `index_name` over `table.field_name`
//
// Will fail if the given field name doesn't exist inside the table schema, or
// if the given index already exists on this table.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
// TODO: Make indices retroactive
function addIndex(remote, table_name, { index_name, field_names: field_name }) {
    const runnable = tx => {
        const field_names = utils.arreturn(field_name);
        return schema.validateSchemaSubset(tx, table_name, field_names).then(r => {
            if (!r) throw new Error("Can't add index on non-existent fields");

            return getIndices(tx, table_name).then(index_table => {
                const names = index_table.map(st => st.index_name);
                if (names.includes(index_name)) {
                    throw new Error(`Can't override index ${index_name}`);
                }

                return setIndex(
                    tx,
                    table_name,
                    index_table.concat({ index_name, field_names })
                );
            });
        });
    };

    return kv.runT(remote, runnable);
}

// Given a table name, and a map `{index_name, field_names}`,
// create a new unique index named `index_name` over `table.field_name`
//
// Will fail if the given field name doesn't exist inside the table schema, or
// if the given index already exists on this table.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
// TODO: Make unique indices retroactive
function addUniqueIndex(remote, table_name, { index_name, field_names: field_name }) {
    const runnable = tx => {
        const field_names = utils.arreturn(field_name);
        return schema.validateSchemaSubset(tx, table_name, field_names).then(r => {
            if (!r) throw new Error("Can't add unique index on non-existent fields");

            return getUniqueIndices(tx, table_name).then(index_table => {
                const names = index_table.map(st => st.index_name);
                if (names.includes(index_name)) {
                    throw new Error(`Can't override unique index ${index_name}`);
                }

                return setUniqueIndex(
                    tx,
                    table_name,
                    index_table.concat({ index_name, field_names })
                );
            });
        });
    };

    return kv.runT(remote, runnable);
}

// Given a table name, return a list of maps
// `{field_name, index_name}` describing the indices of that table.
//
// Will return the empty list if there are no indices.
//
function getIndices(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.get(remote, meta_key).then(meta => {
        const indices = meta.indices;
        return indices === undefined ? [] : indices;
    });
}

// Given a table name, return a list of maps
// `{field_name, index_name}` describing the unique indices of that table.
//
// Will return the empty list if there are no unique indices.
//
function getUniqueIndices(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.get(remote, meta_key).then(meta => {
        const indices = meta.uindices;
        return indices === undefined ? [] : indices;
    });
}

// setIndex(r, t, idxs) will set the index map list of the table `t` to `idxs`
function setIndex(remote, table_name, indices) {
    const meta_key = keyEncoding.table(table_name);
    return kv.runT(remote, function(tx) {
        return kv.get(tx, meta_key).then(meta => {
            return kv.put(tx, meta_key, Object.assign(meta, { indices }));
        });
    });
}

// setUniqueIndex(r, t, idxs) will set the unique index map list of the table `t` to `idxs`
function setUniqueIndex(remote, table_name, uindices) {
    const meta_key = keyEncoding.table(table_name);
    return kv.runT(remote, function(tx) {
        return kv.get(tx, meta_key).then(meta => {
            return kv.put(tx, meta_key, Object.assign(meta, { uindices }));
        });
    });
}

// Given a table name and one of its fields, return a list of indexes,
// or the empty list if no indices are found.
function indexOfField(remote, table_name, indexed_field) {
    return getIndices(remote, table_name).then(indices => {
        // Same as filter(f => f.field_name === indexed_field).map(f => f.index_name)
        const match_index = (acc, { index_name, field_names }) => {
            if (field_names.includes(indexed_field)) {
                return acc.concat(index_name);
            }

            return acc;
        };

        return indices.reduce(match_index, []);
    });
}

// Given a table name and one of its fields, return a list of indexes,
// or the empty list if no indices are found.
function uniqueIndexOfField(remote, table_name, indexed_field) {
    return getUniqueIndices(remote, table_name).then(indices => {
        // Same as filter(f => f.field_name === indexed_field).map(f => f.index_name)
        const match_index = (acc, { index_name, field_names }) => {
            if (field_names.includes(indexed_field)) {
                return acc.concat(index_name);
            }

            return acc;
        };

        return indices.reduce(match_index, []);
    });
}

function fieldsOfIndex(remote, table_name, index_name) {
    return getIndices(remote, table_name).then(indices => {
        const matching = indices.filter(idx_t => {
            const name = idx_t.index_name;
            return name === index_name;
        });

        return utils.flatten(matching.map(({ field_names }) => field_names));
    });
}

function fieldsOfUniqueIndex(remote, table_name, index_name) {
    return getUniqueIndices(remote, table_name).then(indices => {
        const matching = indices.filter(idx_t => {
            const name = idx_t.index_name;
            return name === index_name;
        });

        return utils.flatten(matching.map(({ field_names }) => field_names));
    });
}

// See correlateIndices_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function correlateIndices(remote, table_name, field_names) {
    return kv.runT(remote, function(tx) {
        return correlateIndices_Unsafe(tx, table_name, field_names);
    });
}

// Given a table name, and a list of field names, return a list of the indices
// on any of the fields, in the form [ {index_name, field_names} ].
//
// Whereas `indexOfField` only returns the index name, this function will also return
// all the fields of the index.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function correlateIndices_Unsafe(remote, table_name, field_name) {
    const field_names = utils.arreturn(field_name);

    const promises = field_names.map(f => indexOfField(remote, table_name, f));
    return Promise.all(promises).then(results => {
        // Flatten and remove duplicates
        const indices = utils.squash(utils.flatten(results));
        const correlate = (index_name, field_names) => {
            return { index_name: index_name, field_names };
        };

        const promises = indices.map(index => {
            return fieldsOfIndex(remote, table_name, index).then(fields => {
                return correlate(index, fields);
            });
        });

        return Promise.all(promises);
    });
}

function containsIndex(remote, table, fields) {
    return kv.runT(remote, function(tx) {
        const f_relation = correlateIndices(tx, table, fields);
        return f_relation.then(relation => {
            if (relation.length === 0) {
                return false;
            }

            return { contained: true, indexRelation: relation };
        });
    });
}

// See correlateUniqueIndices_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function correlateUniqueIndices(remote, table_name, field_names) {
    return kv.runT(remote, function(tx) {
        return correlateUniqueIndices_Unsafe(tx, table_name, field_names);
    });
}

// Given a table name, and a list of field names, return a list of the unique
// indices on any of the fields, in the form [ {index_name, field_names} ].
//
// Whereas `uniqueIndexOfField` only returns the index name, this function will also return
// all the fields of the index.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function correlateUniqueIndices_Unsafe(remote, table_name, field_name) {
    const field_names = utils.arreturn(field_name);

    const promises = field_names.map(f => uniqueIndexOfField(remote, table_name, f));
    return Promise.all(promises).then(results => {
        // Flatten and remove duplicates
        const indices = utils.squash(utils.flatten(results));
        const correlate = (index_name, field_names) => {
            return { index_name: index_name, field_names };
        };

        const promises = indices.map(index => {
            return fieldsOfUniqueIndex(remote, table_name, index).then(fields => {
                return correlate(index, fields);
            });
        });

        return Promise.all(promises);
    });
}

function containsUniqueIndex(remote, table, fields) {
    return kv.runT(remote, function(tx) {
        const f_relation = correlateUniqueIndices(tx, table, fields);
        return f_relation.then(relation => {
            if (relation.length === 0) {
                return false;
            }

            return { contained: true, indexRelation: relation };
        });
    });
}

function updateIndices(remote, table, fk_value, mapping) {
    const field_names = Object.keys(mapping);
    const correlated = correlateIndices(remote, table, field_names);

    return correlated.then(relation => {
        const indexMapping = relation.reduce(
            (acc, { index_name, field_names }) => {
                // FIXME: field f might not be in the mapping
                const field_values = field_names.map(f => mapping[f]);
                const { keys, values } = updateSingleIndex(
                    table,
                    index_name,
                    fk_value,
                    field_names,
                    field_values
                );

                return {
                    keys: acc.keys.concat(keys),
                    values: acc.values.concat(values)
                };
            },
            { keys: [], values: [] }
        );

        const { keys, values } = indexMapping;
        return kv.put(remote, keys, values);
    });
}

function updateSingleIndex(table, index, fk_value, field_names, field_values) {
    const indexKeys = updateSingleIndexKeys(
        table,
        index,
        fk_value,
        field_names,
        field_values
    );

    // TODO: If bottom value is defined, use it for these keys
    // Just sentinel keys, should add them to the kset instead
    const indexValues = indexKeys.map(_ => undefined);

    return { keys: indexKeys, values: indexValues };
}

function updateSingleIndexKeys(table, index, fkValue, fieldNames, fieldValues) {
    const nested_index_keys = fieldNames.map((fieldName, ix) => {
        const fieldValue = fieldValues[ix];
        return [
            // Sentinel super key for scans
            keyEncoding.raw_index_field_value(table, index, fieldName, fieldValue),
            keyEncoding.index_key(table, index, fieldName, fieldValue, fkValue)
        ];
    });

    return utils.flatten(nested_index_keys);
}

function updateUniqueIndices(remote, table, fkValue, mapping) {
    const field_names = Object.keys(mapping);
    const correlated = correlateUniqueIndices(remote, table, field_names);

    return correlated.then(relation => {
        const uIndexMapping = relation.reduce(
            (acc, { index_name, field_names }) => {
                // FIXME: field f might not be in the mapping
                const fieldValues = field_names.map(f => mapping[f]);
                const { keys, values } = updateSingleUniqueIndex(
                    table,
                    index_name,
                    fkValue,
                    field_names,
                    fieldValues
                );

                return {
                    keys: acc.keys.concat(keys),
                    values: acc.values.concat(values),
                    // As this is an unique index, we only want this operation to succeed if
                    // the current value of the index is either fkValue or null (bottom)
                    // If it fails, it means that this index key is already used, and therefore
                    // there is an uniqueness guarantee violation.
                    expected: acc.expected.concat(fkValue)
                };
            },
            { keys: [], values: [], expected: [] }
        );

        const { keys, values, expected } = uIndexMapping;

        // TODO: Tag condPut error
        // guarantee that the returned error is an uniqueness violation
        return kv.condPut(remote, keys, values, expected).catch(e => {
            console.log(e);
            throw new Error(`Uniqueness guarantee violation on ${table}`);
        });
    });
}

// TODO: Generate super keys if we want sequential scans over unique indices
function updateSingleUniqueIndex(table, index, fk_value, field_names, field_values) {
    const uindex_keys = updateSingleUniqueIndexKeys(
        table,
        index,
        field_names,
        field_values
    );
    const uindex_values = field_names.map(_ => fk_value);
    return { keys: uindex_keys, values: uindex_values };
}

function updateSingleUniqueIndexKeys(table, index, fieldNames, fieldValues) {
    return fieldNames.map((fieldName, ix) => {
        return keyEncoding.uindex_key(table, index, fieldName, fieldValues[ix]);
    });
}

function pruneIndices(remote, table, fkValues, rows) {
    const swaps = fkValues.map((fkValue, ix) => {
        return pruneRowIndices(remote, table, fkValue, rows[ix]);
    });

    return Promise.all(swaps);
}

function pruneRowIndices(remote, table, fkValue, row) {
    const fieldNames = Object.keys(row);
    const correlated = correlateIndices(remote, table, fieldNames);

    return correlated.then(relation => {
        const nested_sentinelKeys = relation.map(({ index_name, field_names }) => {
            const fieldValues = field_names.map(f => row[f]);
            // FIXME: Assumes that it only returns two keys
            // Won't be the case if we support multi-field indices
            const [sentinel, indexKey] = updateSingleIndexKeys(
                table,
                index_name,
                fkValue,
                field_names,
                fieldValues
            );

            kv.removeKey(remote, table, indexKey);
            return sentinel;
        });

        const sentinelKeys = utils.flatten(nested_sentinelKeys);
        sentinelKeys.forEach(sentinel => {
            const subkeys = kv.strictSubkeyBatch(remote, table, sentinel);
            // If the sentinel key still has any subkeys, don't remove it
            // We need the sentinel key for index scans
            if (subkeys.length === 0) {
                kv.removeKey(remote, table, sentinel);
            }
        });
    });
}

function pruneUniqueIndices(remote, table, rows) {
    const swaps = rows.map(row => {
        return pruneRowUniqueIndices(remote, table, row);
    });

    return Promise.all(swaps);
}

function pruneRowUniqueIndices(remote, table, row) {
    const fieldNames = Object.keys(row);
    const correlated = correlateUniqueIndices(remote, table, fieldNames);

    return correlated.then(relation => {
        const nested_keys = relation.map(({ index_name, field_names }) => {
            const fieldValues = field_names.map(f => row[f]);
            return updateSingleUniqueIndexKeys(
                table,
                index_name,
                field_names,
                fieldValues
            );
        });

        const keys = utils.flatten(nested_keys);
        keys.forEach(key => {
            kv.removeKey(remote, table, key);
        });
    });
}

module.exports = {
    addIndex,
    addUniqueIndex,
    containsIndex,
    containsUniqueIndex,
    updateIndices,
    updateUniqueIndices,
    pruneIndices,
    pruneUniqueIndices
};
