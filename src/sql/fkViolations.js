// @ts-check

const assert = require('assert');

const fks = require('./meta/fks');
const select = require('./select');

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
function checkInFKs(remote, table, oldRow, fieldsToUpdate) {
    const f_inFKs = fks.getInFKs(remote, table);

    // TODO: Change if primary keys are user defined, and / or when fks may point to arbitrary fields
    //
    // Foreign keys may be only created against primary keys, not arbitrary fields
    // And given that primary keys are only autoincremented, and the database is append-only
    // We can check if a specific row exists by checking it its less or equal to the keyrange
    // The actual logic for the cutoff is implemented inside select
    return f_inFKs.then(inFKs => {
        const validChecks = inFKs.map(
            ({ reference_table, field_name, alias }) => {
                // If the update doesn't concern a referenced field, skip
                if (!fieldsToUpdate.includes(field_name)) {
                    return true;
                }

                // The predicate will be "WHERE alias = OLD_FK_VALUE"
                // This should return 0 rows to be value
                const predicate = { [alias]: oldRow[field_name] };
                const f_select = select.select(
                    remote,
                    alias,
                    reference_table,
                    predicate
                );

                // In this case, a cutoff error should not happen,
                // as we're selecting a non-pk value
                return f_select.then(rows => {
                    return rows.length === 0;
                });
            }
        );

        return Promise.all(validChecks).then(allChecks => {
            return allChecks.every(Boolean);
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
function checkOutFKs(remote, table, newRow) {
    const fieldNames = Object.keys(newRow);
    const f_relation = fks.correlateFKs(remote, table, fieldNames);

    // TODO: Change if primary keys are user defined, and / or when fks may point to arbitrary fields
    //
    // Foreign keys may be only created against primary keys, not arbitrary fields
    // And given that primary keys are only autoincremented, and the database is append-only
    // We can check if a specific row exists by checking it its less or equal to the keyrange
    // The actual logic for the cutoff is implemented inside select
    return f_relation.then(relation => {
        const validChecks = relation.map(
            ({ reference_table, field_name, alias }) => {
                const range = newRow[alias];
                // FIXME: Change if FK can be against non-primary fields
                const f_select = select.select(
                    remote,
                    field_name,
                    reference_table,
                    {
                        [field_name]: range
                    }
                );

                return (
                    f_select
                        .then(rows => {
                            // FIXME: Use unique index instead
                            assert(rows.length === 1);
                            const row = rows[0];
                            return row[field_name] === newRow[alias];
                        })
                        // TODO: Tag cutoff error
                        .catch(cutoff_error => {
                            console.log(cutoff_error);
                            return false;
                        })
                );
            }
        );

        return Promise.all(validChecks).then(allChecks => {
            return allChecks.every(Boolean);
        });
    });
}

module.exports = {
    checkInFKs,
    checkOutFKs
};
