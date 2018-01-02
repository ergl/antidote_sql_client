// @ts-check

const utils = require('../utils');

const scan = require('./scan');
const kv = require('../db/kv');
const pks = require('./meta/pks');
const schema = require('./meta/schema');

// See internalDispatchSelect for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function select(remote, fields, table, predicate) {
    return kv.runT(remote, function(tx) {
        return internalDispatchSelect(tx, fields, table, predicate);
    });
}

function internalDispatchSelect(remote, field, table, predicate) {
    // If we're querying more than one table, use an implicit
    // inner join between all of them.
    if (Array.isArray(table)) {
        return internalJoin(remote, field, table, predicate);
    }

    return internalSelect(remote, field, table, predicate);
}

// internalSelect(_, [f1, f2, ..., fn], t, predicate) will perform
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
// Supports for wildard select by calling `internalSelect(_, '*', _, _)`
//
// Will fail if any of the given fields is not part of the table schema.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function internalSelect(remote, fields, table, predicate) {
    const f_validPredicate = validatePredicate(remote, table, predicate);

    const f_predicateFields = f_validPredicate.then(Object.keys);
    const f_queriedFields = validateQueriedFields(remote, table, fields);

    const f_validPredicateFields = f_predicateFields.then(predicateFields => {
        return validatePredicateFields(remote, table, predicateFields);
    });

    return f_validPredicate.then(validPredicate => {
        return f_queriedFields.then(queriedFields => {
            return f_validPredicateFields.then(predicateFields => {
                const f_scanFn = scan.selectScanFn(
                    remote,
                    table,
                    predicateFields
                );

                return f_scanFn
                    .then(scanFn => {
                        return scanFn(remote, table, validPredicate);
                    })
                    .then(rows => {
                        // Filter only the rows that satisfy the predicate
                        const filtered = rows.filter(row => {
                            const valid = predicateFields.map(field => {
                                const matchValues = utils.arreturn(
                                    validPredicate[field]
                                );
                                return matchValues.includes(row[field]);
                            });

                            return valid.every(Boolean);
                        });

                        // Extract only the queried fields
                        return filtered.map(row => {
                            return utils.filterOKeys(row, key => {
                                return queriedFields.includes(key);
                            });
                        });
                    });
            });
        });
    });
}

// For queries, a missing predicate should implicitly satisfy
// all the rows in a table. This method will swap an undefined
// predicate for one that selects all rows in the table.
function validatePredicate(remote, table, predicate) {
    if (predicate !== undefined) return Promise.resolve(predicate);

    const f_pkField = pks.getPKField(remote, table);
    const f_maxPkValue = pks.getCurrentKey(remote, table);

    return Promise.all([f_pkField, f_maxPkValue]).then(
        ([pkField, maxPkValue]) => {
            const pkRange = [...new Array(maxPkValue + 1).keys()];
            pkRange.shift();
            return { [pkField]: pkRange };
        }
    );
}

function validateQueriedFields(remote, table, field) {
    const queriedFields = utils.arreturn(field);
    if (queriedFields.length === 1 && queriedFields[0] === '*') {
        return schema.getSchema(remote, table);
    }

    return schema.validateSchemaSubset(remote, table, queriedFields).then(r => {
        if (!r) {
            throw new Error(
                `Invalid query fields ${queriedFields} on table ${table}`
            );
        }

        return queriedFields;
    });
}

function validatePredicateFields(remote, table, field) {
    const predicateFields = utils.arreturn(field);

    return schema
        .validateSchemaSubset(remote, table, predicateFields)
        .then(r => {
            if (!r) {
                throw new Error(
                    `Invalid predicate fields ${predicateFields} on table ${table}`
                );
            }

            return predicateFields;
        });
}

// predicate: {
// using: [ { A:field_a, B:field_b }, ... ]
// -- interpreted as where A.field_a = B.field_b AND ...
// [table]: { [table.field]: value (same as any select predicate)
// }
function internalJoin(remote, field, tables, joinPredicate) {
    const fields = utils.arreturn(field);

    const validPredicate = validateJoinPredicate(tables, joinPredicate);
    const usingMap = validPredicate.using;

    const perTableQueryFields = computeQueriedFields(tables, fields);

    // Get all tables and prefix them
    const gatherAll = tables.map(table => {
        const willJoinOnFields = getQueriedFieldsForTable(table, usingMap);

        let queryFields = perTableQueryFields[table];
        if (queryFields === undefined) {
            queryFields = willJoinOnFields;
        } else if (queryFields !== '*') {
            willJoinOnFields.forEach(f => {
                if (!queryFields.includes(f)) {
                    queryFields.push(f);
                }
            });
        }

        const tablePredicate = validPredicate[table];
        return select(remote, queryFields, table, tablePredicate).then(r => {
            // Put extra information for multi-way join
            if (tables.length >= 3) {
                return { table: table, rows: prefixTableName(table, r) };
            }

            return prefixTableName(table, r);
        });
    });

    const f_rows = Promise.all(gatherAll);

    return f_rows.then(markedRows => {
        const prefixedMap = prefixUsingMap(usingMap);
        const joined = multiInnerJoin(markedRows, prefixedMap);

        if (joined.length === 0) {
            return joined;
        }

        let queriedFields;
        if (field === '*') {
            // If asking for all, pick all the fields
            // from the first entry—it doesn't matter
            queriedFields = Object.keys(joined[0]);
        } else {
            queriedFields = fields;
        }

        return joined.map(row => {
            return utils.filterOKeys(row, key => queriedFields.includes(key));
        });
    });
}

function validateJoinPredicate(tables, predicate) {
    const usingMap = predicate['using'];
    if (usingMap === undefined) {
        throw new Error(
            `join: Wrong predicate on ${tables}. Remember to use a 'using' predicate`
        );
    }

    return Object.assign(predicate, {
        using: validateUsingMap(tables, usingMap)
    });
}

// For each entry, collect the keys, and make sure they match with the
// given tables
// The shape of an usingMap is:
//
// const using = [
//     {
//         tableA: 'fieldA',
//         tableB: 'fieldB'
//     },
//     ...
// ];
// interpreted as WHERE tableA.fieldA = tableB.fieldB
// AND ...
function validateUsingMap(queriedTables, usingMap) {
    const usingMaps = utils.arreturn(usingMap);

    const keySet = new Set();

    usingMaps.forEach(entry => {
        const entryKeys = Object.keys(entry);
        if (entryKeys.length != 2) {
            throw new Error(
                'join: Each predicate entry should at least have two tables'
            );
        }

        entryKeys.forEach(key => {
            keySet.add(key);
            if (!queriedTables.includes(key)) {
                throw new Error(
                    `join: Unknown table ${key}. Check the 'tables' key`
                );
            }
        });
    });

    for (let table of queriedTables) {
        if (!keySet.has(table)) {
            throw new Error(
                `join: Missing table ${table}. Check the 'tables' field`
            );
        }
    }

    return usingMaps;
}

// Given a list of tables and a list of fields (in table.field syntax)
// return a map { table: [fields], ... } with the appropiate fields
// to query for
function computeQueriedFields(tables, fields) {
    const fieldMap = {};

    if (fields[0] === '*') {
        tables.forEach(t => (fieldMap[t] = '*'));
        return fieldMap;
    }

    fields.forEach(field => {
        const [table, ...rest] = field.split('.');
        if (!tables.includes(table)) {
            return;
        }

        // Just in case fields contain a dot
        const reField = rest.join('.');
        const oldFields = fieldMap[table];
        if (oldFields === undefined) {
            fieldMap[table] = [reField];
        } else {
            oldFields.push(reField);
        }
    });

    return fieldMap;
}

function getQueriedFieldsForTable(table, usingMap) {
    const fields = [];

    usingMap.forEach(entry => {
        const keys = Object.keys(entry);
        if (keys.includes(table)) {
            fields.push(entry[table]);
        }
    });

    return fields;
}

function prefixTableName(tableName, row) {
    const rows = utils.arreturn(row);
    return rows.map(row => {
        return utils.mapOKeys(row, key => prefixField(tableName, key));
    });
}

function prefixField(tableName, field) {
    const prefix = '.';
    return tableName + prefix + field;
}

function prefixUsingMap(usingMap) {
    return usingMap.map(entry => {
        return utils.mapO(entry, (key, value) => {
            return {
                [key]: prefixField(key, value)
            };
        });
    });
}

function multiInnerJoin(markedRows, usingMap) {
    if (markedRows.length === 2) {
        const map = usingMap[0];
        const [lField, rField] = Object.keys(map).map(k => {
            return map[k];
        });
        const [lRows, rRows] = markedRows;
        return innerJoin(lRows, rRows, lField, rField);
    }

    let target = null;

    for (let map of usingMap) {
        const selectedTables = Object.keys(map);
        const [lTable, rTable] = selectedTables;
        const selectedRows = selectedTables.map(table => {
            return utils.flatten(
                markedRows
                    .filter(nestedRow => nestedRow.table === table)
                    .map(entry => {
                        return entry.rows;
                    })
            );
        });
        const [lRows, rRows] = selectedRows;
        if (target === null) {
            target = innerJoin(lRows, rRows, map[lTable], map[rTable]);
        } else {
            target = innerJoin(target, rRows, map[lTable], map[rTable]);
        }
    }

    return target;
}

// Given two lists of rows, and two fields,
// perform an inner join on them such that all the
// field entries are equal
// innerJoin([{foo: "a", bar: "b"}], [{foo: "a", baz: "c"}], 'foo', 'foo')
// => [ { foo: "a", bar: "b", baz: "c" } ]
function innerJoin(lRows, rRows, lField, rField) {
    const nestedRows = lRows.map(lRow => {
        const lval = lRow[lField];
        const matches = rRows.filter(rRow => rRow[rField] === lval);
        const nestedCombine = matches.map(match => {
            return Object.assign(lRow, match);
        });
        return utils.flatten(nestedCombine);
    });

    return utils.flatten(nestedRows);
}

module.exports = {
    select
};
