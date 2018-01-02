// @ts-check

const utils = require('../utils');

const kv = require('../db/kv');
const pks = require('./meta/pks');
const fks = require('./meta/fks');
const insert = require('./insert');
const select = require('./select');
const indices = require('./meta/indices');
const fkViolations = require('./fkViolations');

function update(remote, table, mapping, predicate) {
    return kv.runT(remote, function(tx) {
        return internalUpdate(tx, table, mapping, predicate);
    });
}

// TODO: Refactor
function internalUpdate(remote, table, mapping, predicate) {
    const fieldsToUpdate = Object.keys(mapping);

    const f_pkNotPresent = pks.containsPK(remote, table, fieldsToUpdate);

    return (
        f_pkNotPresent
            // Check if trying to update a primary key
            // If it is, abort the transaction
            .then(({ contained, pkField }) => {
                if (contained) {
                    throw new Error(
                        `Updates to autoincremented primary keys are not allowed`
                    );
                }

                const f_oldRows = select.select(remote, '*', table, predicate);

                // Check if any of the affected rows is being referenced by another table
                const f_rowsWereNotReferenced = f_oldRows.then(oldRows => {
                    const f_checks = oldRows.map(oldRow => {
                        return fkViolations.checkInFKs(
                            remote,
                            table,
                            oldRow,
                            fieldsToUpdate
                        );
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
                            newValue = utils.isFunction(update)
                                ? update(oldValue)
                                : update;
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
                    const validFKs = fkViolations.checkOutFKs(
                        remote,
                        table,
                        row
                    );

                    return validFKs.then(valid => {
                        if (!valid) throw new Error('FK constraint failed');

                        const pkValue = row[pkField];
                        const mapping = utils.filterOKeys(
                            row,
                            k => k !== pkField
                        );
                        return insert.__unsafeRawInsert(
                            remote,
                            table,
                            pkValue,
                            mapping
                        );
                    });
                });

                const fkValues = updatedRows.map(row => row[pkField]);

                return Promise.all(f_inserts)
                    .then(_ => {
                        return indices.pruneIndices(
                            remote,
                            table,
                            fkValues,
                            oldRows,
                            fieldsToUpdate
                        );
                    })
                    .then(_ => {
                        return indices.pruneUniqueIndices(
                            remote,
                            table,
                            oldRows,
                            fieldsToUpdate
                        );
                    });
            })
    );
}

module.exports = {
    update
};
