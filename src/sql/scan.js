const utils = require('../utils');

const kv = require('../db/kv');
const pks = require('./meta/pks');
const schema = require('./meta/schema');
const indices = require('./meta/indices');
const keyEncoding = require('../db/keyEncoding');

// Given a table name, and a list of fields that make up the predicate
// of a query, select the most appropiate scan function.
// Depending on the fields being selected, it will use either primary (fast)
// or sequential (slow) scan functions.
//
// Returns a function that should be called with a remote, a table name, and the
// complete predicate object.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// TODO: Refactor
function selectScanFn(remote, table, predicateFields) {
    const f_containsUniqueIndex = indices.containsUniqueIndex(
        remote,
        table,
        predicateFields
    );

    return f_containsUniqueIndex.then(({ contained, indexRelation }) => {
        if (contained) {
            const { index, fieldName } = chooseBestIndex(indexRelation, predicateFields);
            return function(remote, table, predicate) {
                return scanUniqueIndex(
                    remote,
                    table,
                    index,
                    fieldName,
                    predicate[fieldName]
                );
            };
        }

        const f_containsIndex = indices.containsIndex(remote, table, predicateFields);
        return f_containsIndex.then(({ contained, indexRelation }) => {
            if (contained) {
                const { index, fieldName } = chooseBestIndex(
                    indexRelation,
                    predicateFields
                );

                return function(remote, table, predicate) {
                    return scanIndex(
                        remote,
                        table,
                        index,
                        fieldName,
                        predicate[fieldName]
                    );
                };
            }

            const f_containsPk = pks.containsPK(remote, table, predicateFields);
            return f_containsPk.then(({ contained, pkField }) => {
                if (contained) {
                    return function(remote, table, predicate) {
                        return scanFast(remote, table, predicate[pkField]);
                    };
                }

                // If the predicate fields don't contain a primary key, we have to
                // perform a sequential scan of all the keys in the table.
                // Ideally an index should exist on a field for fast scanning.
                return function(remote, table, _) {
                    return scanSequential(remote, table);
                };
            });
        });
    });
}

function chooseBestIndex(indexRelation, predicateFields) {
    const bestFit = indexRelation.filter(({ field_names }) => {
        return field_names.every(e => predicateFields.includes(e));
    });

    // TODO: Do better, assumes that there's at least one
    const { index_name, field_names } = bestFit[0];
    const chosenField = field_names[0];

    return { index: index_name, fieldName: chosenField };
}

// Given a table name, and a range of primary keys (in the form of [start, end]),
// will fetch the appropiate subkey batch and get all the values from those.
// Only supports selects against primary keys.
//
// Will fail if the scan goes out of bounds of max(table.pk_value)
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// FIXME: Fetch only appropiate keys
// Right now the scan is too eager, as it fetches the keys for all the
// fields, even if we only use a single result. Not trivial to know, however,
// as selects that are part of joins might not now which fields are going to
// be used as part of the join.
function scanFast(remote, table, pkValue) {
    const pkValues = utils.arreturn(pkValue);

    // Assumes keys are numeric
    // Only useful for primary keys
    //
    // FIXME: Change if using user-defined primary keys
    // In that case, should look in the appropiate unique index
    const f_validRange = pks.getCurrentKey(remote, table).then(max => {
        return pkValues.every(pk => pk <= max);
    });

    return f_validRange
        .then(validRange => {
            if (!validRange) {
                throw new Error(
                    `scanFast of keys ${pkValues} on ${table} went out of valid range`
                );
            }

            return schema.getSchema(remote, table);
        })
        .then(schema => {
            const keyBatch = utils.flatten(
                pkValues.map(pkValue => {
                    const rootKey = keyEncoding.spk(table, pkValue);
                    return kv.subkeyBatch(remote, table, rootKey);
                })
            );

            return kv.get(remote, keyBatch).then(results => {
                return toRowExt(results, schema);
            });
        });
}

// Slow scan through every data subkey of the table
// (excluding indices and unique indices)
function scanSequential(remote, table) {
    const f_schema = schema.getSchema(remote, table);

    const rootKey = keyEncoding.table(table);
    const keys = kv.subkeyBatch(remote, table, rootKey).filter(keyEncoding.isData);
    return kv.get(remote, keys).then(values => {
        return f_schema.then(schema => {
            return toRowExt(values, schema);
        });
    });
}

// Given a table, the name of an index, the name of a field indexed by it,
// and a list of expected values (predicate), fetch the corresponding primary keys,
// and then fetch the appropiate tables where this field exists with any of those values.
// This scan should be faster than both scanFast and scanSequential.
//
// TODO: Return early if query only wanted primary key values
// If the query was SELECT id FROM [...] WHERE indexedField = "foo"
// this scan will be used, but after getting the id, they will be followed
// and the entire table will be fetched. Then a filter will happen, extracting
// only the id. If we know that only the id is used, we don't even have to make
// a roundtrip to the database, and this scan will be free.
//
function scanIndex(remote, table, index, field, value) {
    const values = utils.arreturn(value);

    const matchKeys = values.map(v => {
        return keyEncoding.raw_index_field_value(table, index, field, v);
    });

    const f_allBatches = matchKeys.map(k => {
        const matchedKeys = kv.strictSubkeyBatch(remote, table, k);
        const pkValues = matchedKeys.map(keyEncoding.getIndexData);
        return fetchBatch(remote, table, pkValues);
    });

    return Promise.all(f_allBatches).then(utils.flatten);
}

// Given a table, the name of an unique index, the name of a field indexed by it,
// and a list of expected values (predicate), fetch the corresponding primary keys,
// and then fetch the appropiate tables where this field exists with any of those values.
// This scan should be faster than both scanFast and scanSequential.
//
// TODO: Return early if query only wanted primary key values
// If the query was SELECT id FROM [...] WHERE indexedField = "foo"
// this scan will be used, but after getting the id, they will be followed
// and the entire table will be fetched. Then a filter will happen, extracting
// only the id. If we know that only the id is used, we only need a single roundtrip
// instead of two.
//
function scanUniqueIndex(remote, table, index, field, value) {
    const values = utils.arreturn(value);

    const matchKeys = values.map(v => {
        return keyEncoding.uindex_key(table, index, field, v);
    });

    const f_pkValues = matchKeys.map(matchKey => {
        // Don't throw on empty gets, this means that the given value
        // is not in the index.
        return kv.get(remote, matchKey, { unsafe: true });
    });

    return Promise.all(f_pkValues).then(maybeNestedpkValues => {
        const pkValues = utils.flatten(maybeNestedpkValues);
        // If the values were not in the index, they don't exist
        // in the table either. (Assuming retroactive indices).
        if (pkValues.every(e => e === null)) {
            return Promise.resolve([]);
        }

        const f_allBatches = pkValues.map(pkValue => fetchBatch(remote, table, pkValue));
        return Promise.all(f_allBatches).then(utils.flatten);
    });
}

function fetchBatch(remote, table, pkValue) {
    const pkValues = utils.arreturn(pkValue);
    const pks = pkValues.map(k => keyEncoding.spk(table, k));
    const keyBatch = utils.flatten(pks.map(pk => kv.subkeyBatch(remote, table, pk)));

    const f_schema = schema.getSchema(remote, table);
    const f_result = kv.get(remote, keyBatch);

    return Promise.all([f_schema, f_result]).then(([schema, results]) => {
        return toRowExt(results, schema);
    });
}

// Given a list of results from a scan, and a list of field names,
// build an object { field: value }.
//
// If length(row) > length(field_names), it will build multiple rows.
// Assumes length(row) = N * length(field_names)
// For example:
// toRowExt([1,2,3,1,2,3], ['foo','bar','baz'])
// => [ { foo: 1, bar: 2, baz: 3 }, { foo: 1, bar: 2, baz: 3 } ]
function toRowExt(map, field_names) {
    const row = utils.arreturn(map);
    const res = [];

    let vi = 0;
    while (vi < row.length) {
        const obj = field_names.reduce(
            (acc, f, ix) => {
                return Object.assign(acc, { [f]: row[ix + vi] });
            },
            {}
        );
        res.push(obj);
        vi = vi + field_names.length;
    }

    return res;
}

module.exports = {
    selectScanFn
};
