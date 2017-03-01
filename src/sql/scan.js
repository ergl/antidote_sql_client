const utils = require('../utils');

const kv = require('../db/kv');
const pks = require('./meta/pks');
const schema = require('./meta/schema');
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
// TODO: Add selections for index scans
function selectScanFn(remote, table, predicateFields) {
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
        // TODO: scanIndex
        return function(remote, table, _) {
            return scanSequential(remote, table);
        };
    });
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
function scanFast(remote, table, pkRange) {
    const pkBatch = utils.arreturn(pkRange);
    const [pkStart, pkEnd] = pkBatch.length === 1 ? [pkBatch, pkBatch] : pkBatch;

    // Assumes keys are numeric
    // Only useful for primary keys
    //
    // FIXME: Change if using user-defined primary keys
    // In that case, should look in the appropiate unique index
    const f_validRange = pks.getCurrentKey(remote, table).then(max => {
        return pkEnd <= max;
    });

    return f_validRange
        .then(validRange => {
            if (!validRange) {
                throw new Error(
                    `scanPrimary of key ${pkEnd} on ${table} is out of valid range`
                );
            }

            return schema.getSchema(remote, table);
        })
        .then(schema => {
            // If called with just one key, like scan(A, A),
            // fetch subkeys(A) instead
            if (pkBatch.length === 1) {
                const [pkStart] = pkBatch;
                const key = keyEncoding.spk(table, pkStart);
                const keyBatch = kv.subkeyBatch(remote, key);
                return kv.get(remote, keyBatch).then(r => {
                    return [toRow(r, schema)];
                });
            }

            const [startKey, endKey] = [
                keyEncoding.spk(table, pkStart),
                keyEncoding.spk(table, pkEnd)
            ];

            // Given that we're interested in the subkeys of the primary key,
            // we combine batch(A,B) + strictSubkeys(B)
            const firstBatch = kv.keyBatch(remote, startKey, endKey);
            const subkeyBatch = kv.strictSubkeyBatch(remote, endKey);
            const keyBatch = firstBatch.concat(subkeyBatch);

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
    const keys = kv.subkeyBatch(remote, rootKey).filter(keyEncoding.isData);
    return kv.get(remote, keys).then(values => {
        return f_schema.then(schema => {
            return toRowExt(values, schema);
        });
    });
}

// Given a list of results from a scan, and a list of field names,
// build an object { field: value }.
//
// Assumes length(row) = length(field_names)
function toRow(row, field_names) {
    return row.reduce(
        (acc, curr, ix) => {
            return Object.assign(acc, { [field_names[ix]]: curr });
        },
        {}
    );
}

// Given a list of results from a scan, and a list of field names,
// build an object { field: value }.
//
// If length(row) > length(field_names), it will build multiple rows.
// Assumes length(row) = N * length(field_names)
// For example:
// toRowExt([1,2,3,1,2,3], ['foo','bar','baz'])
// => [ { foo: 1, bar: 2, baz: 3 }, { foo: 1, bar: 2, baz: 3 } ]
function toRowExt(row, field_names) {
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
