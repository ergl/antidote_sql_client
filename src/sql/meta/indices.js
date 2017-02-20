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

// setIndex(r, t, idxs) will set the index map list of the table `t` to `idxs`
function setIndex(remote, table_name, indices) {
    const meta_key = keyEncoding.table(table_name);
    return kv.runT(remote, function(tx) {
        return kv.get(tx, meta_key).then(meta => {
            return kv.put(tx, meta_key, Object.assign(meta, { indices }));
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

function fieldsOfIndex(remote, table_name, index_name) {
    return getIndices(remote, table_name).then(indices => {
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
function legacy__correlateIndices_T(remote, table_name, field_names) {
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

// Given a table and an index name, checks if
// the index references a field in that table.
function isIndex(remote, table_name, idx_name) {
    return getIndices(remote, table_name).then(indices => {
        return indices.map(idx => idx.index_name).includes(idx_name);
    });
}

module.exports = {
    addIndex,
    legacy__correlateIndices_T
};
