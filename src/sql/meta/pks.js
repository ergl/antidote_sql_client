const kv = require('./../../db/kv');
const keyEncoding = require('./../../db/keyEncoding');

// Given a table name, return the name of the field
// that acts as primary key for the table.
//
// Will fail if there is no primary key. (As all tables must have
// a primary key set).
//
function getPKField(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.get(remote, meta_key).then(meta => {
        const pk_field = meta.primary_key_field;
        if (pk_field === undefined) {
            throw new Error(`Fatal: ${table_name} doesn't have a primary key`);
        }

        return pk_field;
    });
}

// Given table name, perform a fetch-and-add on its key counter reference,
// and return the new value.
//
// To perform FAA atomically, this function will start a new transaction by default,
// unless called from inside another transaction (given that the current API doesn't
// allow nested transaction).
//
// In that case, all operations will be executed in the current transaction.
//
function fetchAddPrimaryKey_T(remote, table_name) {
    return kv.runT(remote, function(tx) {
        return incrKey(tx, table_name).then(_ => getCurrentKey(tx, table_name));
    });
}

function getCurrentKey(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.get(remote, meta_key).then(meta => meta.current_pk_value);
}

// Atomically increment the primary key counter value.
function incrKey(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.runT(remote, function(tx) {
        return kv.get(tx, meta_key).then(meta => {
            const pk_value = meta.current_pk_value;
            return kv.put(
                tx,
                meta_key,
                Object.assign(meta, {
                    current_pk_value: pk_value + 1
                })
            );
        });
    });
}

// Given a list of fields, return if it contains a primary key
// Return { contained : true, pkField : string } if found,
// { contained : false, pkField : string } otherwise
function containsPK(remote, table, fields) {
    return getPKField(remote, table).then(pkField => {
        if (fields.includes(pkField)) {
            return { contained: true, pkField };
        }

        return { contained: false, pkField };
    });
}

module.exports = {
    containsPK,
    getPKField,
    getCurrentKey,
    fetchAddPrimaryKey_T
};
