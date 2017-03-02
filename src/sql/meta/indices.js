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

            return getUindices(tx, table_name).then(index_table => {
                const names = index_table.map(st => st.index_name);
                if (names.includes(index_name)) {
                    throw new Error(`Can't override unique index ${index_name}`);
                }

                return setUindex(
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
function getUindices(remote, table_name) {
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

// setUindex(r, t, idxs) will set the unique index map list of the table `t` to `idxs`
function setUindex(remote, table_name, uindices) {
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
function uindexOfField(remote, table_name, indexed_field) {
    return getUindices(remote, table_name).then(indices => {
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

function fieldsOfUindex(remote, table_name, index_name) {
    return getUindices(remote, table_name).then(indices => {
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

// See correlateUindices_Unsafe for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function correlateUniqueIndices(remote, table_name, field_names) {
    return kv.runT(remote, function(tx) {
        return correlateUindices_Unsafe(tx, table_name, field_names);
    });
}

// Given a table name, and a list of field names, return a list of the unique
// indices on any of the fields, in the form [ {index_name, field_names} ].
//
// Whereas `uindexOfField` only returns the index name, this function will also return
// all the fields of the index.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function correlateUindices_Unsafe(remote, table_name, field_name) {
    const field_names = utils.arreturn(field_name);

    const promises = field_names.map(f => uindexOfField(remote, table_name, f));
    return Promise.all(promises).then(results => {
        // Flatten and remove duplicates
        const indices = utils.squash(utils.flatten(results));
        const correlate = (index_name, field_names) => {
            return { index_name: index_name, field_names };
        };

        const promises = indices.map(index => {
            return fieldsOfUindex(remote, table_name, index).then(fields => {
                return correlate(index, fields);
            });
        });

        return Promise.all(promises);
    });
}

function updateIndices(remote, table, fk_value, mapping) {
    const field_names = Object.keys(mapping);
    const correlated = correlateIndices(remote, table, field_names);

    return correlated.then(relation => {
        const ops = relation.reduce(
            (acc, { index_name, field_names }) => {
                // FIXME: field f might not be in the mapping
                const field_values = field_names.map(f => mapping[f]);
                const pr = updateSingleIndex(
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
function updateSingleIndex(table, index, fk_value, field_names, field_values) {
    const index_keys = field_names.map((fld_name, i) => {
        return keyEncoding.index_key(table, index, fld_name, field_values[i], fk_value);
    });

    // TODO: If bottom value is defined, use it for these keys
    // Just sentinel keys, should add them to the kset instead
    const index_values = field_names.map(_ => undefined);

    return { keys: index_keys, values: index_values };
}

function updateUIndices(remote, table, fk_value, mapping) {
    const field_names = Object.keys(mapping);
    const correlated = correlateUniqueIndices(remote, table, field_names);

    return correlated.then(relation => {
        const ops = relation.reduce(
            (acc, { index_name, field_names }) => {
                // FIXME: field f might not be in the mapping
                const field_values = field_names.map(f => mapping[f]);
                const pr = updateSingleUIndex(
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
                        values: acc.values.concat(values),
                        expected: acc.expected.concat(fk_value)
                    };
                });
            },
            Promise.resolve({ keys: [], values: [], expected: [] })
        );

        return ops.then(({ keys, values, expected }) => {
            // TODO: Tag condPut error
            // guarantee that the returned error is an uniqueness violation
            return kv.condPut(remote, keys, values, expected).catch(e => {
                console.log(e);
                throw new Error(`Uniqueness guarantee violation on ${table}`);
            });
        });
    });
}

// TODO: Generate super keys if we want sequential scans over unique indices
function updateSingleUIndex(table, index, fk_value, field_names, field_values) {
    const uindex_keys = field_names.map((fld_name, i) => {
        return keyEncoding.uindex_key(table, index, fld_name, field_values[i]);
    });

    const uindex_values = field_names.map(_ => fk_value);
    return { keys: uindex_keys, values: uindex_values };
}

module.exports = {
    addIndex,
    addUniqueIndex,
    updateIndices,
    updateUIndices
};
