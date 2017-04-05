const assert = require('assert');

const utils = require('../utils');

const scan = require('./scan');
const kv = require('../db/kv');
const pks = require('./meta/pks');
const fks = require('./meta/fks');
const schema = require('./meta/schema');
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
            return schema.validateSchema(remote, table, insertFields).then(r => {
                if (!r) throw new Error('Invalid schema');
                return checkOutFKViolation(remote, table, mapping);
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
        .then(_ => indices.updateUniqueIndices(remote, table, pkValue, mapping));
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
function checkOutFKViolation(remote, table, mapping) {
    const fieldNames = Object.keys(mapping);
    const f_relation = fks.correlateFKs(remote, table, fieldNames);

    // TODO: Change if primary keys are user defined, and / or when fks may point to arbitrary fields
    //
    // Foreign keys may be only created against primary keys, not arbitrary fields
    // And given that primary keys are only autoincremented, and the database is append-only
    // We can check if a specific row exists by checking it its less or equal to the keyrange
    // The actual logic for the cutoff is implemented inside select
    return f_relation.then(relation => {
        const validChecks = relation.map(({ reference_table, field_name, alias }) => {
            const range = mapping[alias];
            // FIXME: Change if FK can be against non-primary fields
            const f_select = select(remote, field_name, reference_table, {
                [field_name]: range
            });

            return f_select
                .then(rows => {
                    // FIXME: Use unique index instead
                    assert(rows.length === 1);
                    const row = rows[0];
                    return row[field_name] === mapping[alias];
                })
                // TODO: Tag cutoff error
                .catch(cutoff_error => {
                    console.log(cutoff_error);
                    return false;
                });
        });

        return Promise.all(validChecks).then(allChecks => allChecks.every(Boolean));
    });
}

// Given a table, and a map of field names to their values (candidate to be updated),
// check if the new values satisfy foreign key constraints, following that:
//
// - A value X in the parent column may only be changed or deleted if X does not exist in the child column.
//
// Return true if the condition is met. Foreign keys are represented as regular fields,
// plus some metadata attached to the table. This means that every insert and update has
// to check in the parent table, and updates to the parent table will have to check
// referencing tables. In contrast, reads of foreign keys incur no extra cost.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
// TODO: Revisit assumptions
// Assumptions: Given that FK can only be placed on primary keys,
// we don't have to check that the new value is repeated, as that is asserted by
// the current `update` behaviour. We only need to check that the old value is not being used
function checkInFKViolation(remote, table, oldRow, fieldsToUpdate) {
    const f_inFKs = fks.getInFKs(remote, table);

    // TODO: Change if primary keys are user defined, and / or when fks may point to arbitrary fields
    //
    // Foreign keys may be only created against primary keys, not arbitrary fields
    // And given that primary keys are only autoincremented, and the database is append-only
    // We can check if a specific row exists by checking it its less or equal to the keyrange
    // The actual logic for the cutoff is implemented inside select
    return f_inFKs.then(inFKs => {
        const validChecks = inFKs.map(({ reference_table, field_name, alias }) => {
            // If the update doesn't concern a referenced field, skip
            if (!fieldsToUpdate.includes(field_name)) {
                return true;
            }

            // The predicate will be "WHERE alias = OLD_FK_VALUE"
            // This should return 0 rows to be value
            const predicate = { [alias]: oldRow[field_name] };
            const f_select = select(remote, alias, reference_table, predicate);

            // In this case, a cutoff error should not happen,
            // as we're selecting a non-pk value
            return f_select.then(rows => {
                return rows.length === 0;
            });
        });

        return Promise.all(validChecks).then(allChecks => allChecks.every(Boolean));
    });
}

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
                const f_scanFn = scan.selectScanFn(remote, table, predicateFields);

                return f_scanFn
                    .then(scanFn => {
                        return scanFn(remote, table, validPredicate);
                    })
                    .then(rows => {
                        // Filter only the rows that satisfy the predicate
                        const filtered = rows.filter(row => {
                            const valid = predicateFields.map(field => {
                                const matchValues = utils.arreturn(validPredicate[field]);
                                return matchValues.includes(row[field]);
                            });

                            return valid.every(Boolean);
                        });

                        // Extract only the queried fields
                        return filtered.map(row => {
                            return utils.filterOKeys(row, key =>
                                queriedFields.includes(key));
                        });
                    });
            });
        });
    });
}

// predicate:
// { using: [a.field, b.field], (interpreted as where a.field = b.field, ...
//   [table]: { [table.field]: value (same as any select predicate)
// }
function internalJoin(remote, fields, tables, predicate) {
    const validPredicate = validateJoinPredicate(tables, predicate);
    const onFields = validPredicate.using;
    const prefixedFields = onFields.map((f, ix) => prefixField(tables[ix], f));

    // Get all tables and prefix them
    // TODO: Don't fetch all fields, just the ones we need
    const gatherAll = tables.map(table => {
        const tablePredicate = predicate[table];
        return select(remote, '*', table, tablePredicate).then(r => {
            return prefixTableName(table, r);
        });
    });

    const f_rows = Promise.all(gatherAll);

    return f_rows.then(allRows => {
        const joined = multiInnerJoin(allRows, prefixedFields);

        if (joined.length === 0) {
            return joined;
        }

        let queriedFields;
        if (fields === '*') {
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
    const joinFields = predicate['using'];
    if (joinFields === undefined) {
        throw new Error(
            `join: Wrong predicate on ${tables}. Remember to use a 'using' predicate`
        );
    }

    return Object.assign(predicate, { using: validateJoinFields(tables, joinFields) });
}

function validateJoinFields(tables, onField) {
    const onFields = utils.arreturn(onField);

    if (onFields.length !== tables.length) {
        if (onFields.length === 1) {
            // Make all fields the same if the user only specifies one
            return [...new Array(tables.length)].fill(onField);
        } else {
            throw new Error(
                `join: Wrong number of predicate fields. Expected ${tables.length}, got ${onFields.length}`
            );
        }
    }

    return onFields;
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

// Given two lists of rows, and two fields,
// perform an inner join on them such that all the
// field entries are equal
// innerJoin([{foo: "a", bar: "b"}], [{foo: "a", baz: "c"}], 'foo', 'foo')
// => [ { foo: "a", bar: "b", baz: "c" } ]
function innerJoin(lRows, rRows, lField, rField) {
    const nestedRows = lRows.map(lRow => {
        const lval = lRow[lField];
        const matches = rRows.filter(rRow => rRow[rField] === lval);
        const nestedCombine = matches.map(match => combine(lRow, match, lField, rField));
        return utils.flatten(nestedCombine);
    });

    return utils.flatten(nestedRows);
}

// Same as innerJoin, but for an arbitrary number of tables
// Assumes at least two lists of rows and an equal number of fields
function multiInnerJoin(nestedRows, onFields) {
    const [first, ...rest] = nestedRows;
    return rest.reduce(
        (acc, curr, ix) => {
            return innerJoin(acc, curr, onFields[ix], onFields[ix + 1]);
        },
        first
    );
}

function combine(lrow, rrow, onl, onr = onl) {
    const comb = Object.assign(lrow, rrow);
    if (onl === onr) return comb;
    return utils.filterOKeys(comb, f => f !== onr);
}

function update(remote, table, mapping, predicate) {
    return kv.runT(remote, function(tx) {
        return internalUpdate(tx, table, mapping, predicate);
    });
}

function internalUpdate(remote, table, mapping, predicate) {
    const fieldsToUpdate = Object.keys(mapping);

    const f_pkNotPresent = pks.containsPK(remote, table, fieldsToUpdate);

    return f_pkNotPresent
        // Check if trying to update a primary key
        // If it is, abort the transaction
        .then(({ contained, pkField }) => {
            if (contained) {
                throw new Error(
                    `Updates to autoincremented primary keys are not allowed`
                );
            }

            const f_oldRows = select(remote, '*', table, predicate);

            // Check if any of the affected rows is being referenced by another table
            const f_rowsWereNotReferenced = f_oldRows.then(oldRows => {
                const f_checks = oldRows.map(oldRow => {
                    return checkInFKViolation(remote, table, oldRow, fieldsToUpdate);
                });

                return Promise.all(f_checks).then(checks => {
                    return checks.every(Boolean);
                });
            });

            const wait = Promise.all([f_oldRows, f_rowsWereNotReferenced]);
            return wait.then(([oldRows, rowsWereNotReferenced]) => {
                return { oldRows, rowsWereNotReferenced, pkField };
            });
        })
        .then(({ oldRows, rowsWereNotReferenced, pkField }) => {
            // If any of the old rows was referenced, abort the transaction
            if (!rowsWereNotReferenced) {
                throw new Error(
                    `Can't update table ${table} as it is referenced by another table`
                );
            }

            const updatedRows = oldRows.map(oldRow => {
                return utils.mapO(oldRow, (k, oldValue) => {
                    let newValue;
                    if (fieldsToUpdate.includes(k)) {
                        const update = mapping[k];
                        // We might pass a function that receives the old value
                        newValue = utils.isFunction(update) ? update(oldValue) : update;
                    } else {
                        newValue = oldValue;
                    }
                    return { [k]: newValue };
                });
            });

            const f_inserts = updatedRows.map(row => {
                // Our foreign key guarantees say
                // A value X in a child column may only be updated to a value Y
                // if Y exists in the parent column.
                // This will check that the new row satisifies this point
                // If it violates the guarantee, abort the transaction
                const validFKs = checkOutFKViolation(remote, table, row);

                return validFKs.then(valid => {
                    if (!valid) throw new Error('FK constraint failed');

                    const pkValue = row[pkField];
                    const mapping = utils.filterOKeys(row, k => k !== pkField);
                    return rawInsert(remote, table, pkValue, mapping);
                });
            });

            const fkValues = updatedRows.map(row => row[pkField]);

            return Promise.all(f_inserts)
                .then(_ => {
                    return indices.pruneIndices(remote, table, fkValues, oldRows);
                })
                .then(_ => {
                    return indices.pruneUniqueIndices(remote, table, oldRows);
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

    return Promise.all([f_pkField, f_maxPkValue]).then(([pkField, maxPkValue]) => {
        const pkRange = [...new Array(maxPkValue + 1).keys()];
        pkRange.shift();
        return { [pkField]: pkRange };
    });
}

function validateQueriedFields(remote, table, field) {
    const queriedFields = utils.arreturn(field);
    if (queriedFields.length === 1 && queriedFields[0] === '*') {
        return schema.getSchema(remote, table);
    }

    return schema.validateSchemaSubset(remote, table, queriedFields).then(r => {
        if (!r) {
            throw new Error(`Invalid query fields ${queriedFields} on table ${table}`);
        }

        return queriedFields;
    });
}

function validatePredicateFields(remote, table, field) {
    const predicateFields = utils.arreturn(field);

    return schema.validateSchemaSubset(remote, table, predicateFields).then(r => {
        if (!r) {
            throw new Error(
                `Invalid predicate fields ${predicateFields} on table ${table}`
            );
        }

        return predicateFields;
    });
}

function reset(remote) {
    return kv.runT(remote, kv.reset);
}

module.exports = {
    create,
    select,
    insert,
    update,
    reset
};
