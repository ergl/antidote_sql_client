const utils = require('../../utils/index');

const schema = require('./schema');
const kv = require('./../../db/kv');
const keyEncoding = require('./../../db/keyEncoding');

// See internalAddFK for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
// TODO: Move this to table creation
function createFK(remote, tableName, mapping) {
    return kv.runT(remote, function(tx) {
        return internalAddFK(tx, tableName, mapping);
    });
}

// Given a table name, and a list of maps `{alias, field_name, reference_table}`,
// create a foreign key for every element of the map list, such that
// `table.alias` will be the foreign key pointing to `reference_table.field_name`
//
// Will fail if:
// a) This table – or any of the given `reference_table`s – don't exist
// b) Any of the given fields don't exist.
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function internalAddFK(remote, tableName, fkMap) {
    // NOTE: Assumes you can't add more than FK per field.
    // Therefore we assume that `fkMaps` doesn't contain duplicates.
    const fkMaps = utils.arreturn(fkMap);

    // If an alias is not specified, use the same name as the referenced
    // table field name
    const aliasedMapping = fkMaps.map(fkMap => {
        if (fkMap.hasOwnProperty('alias')) {
            return fkMap;
        }

        return Object.assign(fkMap, { alias: fkMap.field_name });
    });

    // Get the field name for our own table
    const aliases = aliasedMapping.map(o => o.alias);

    // For all reference tables, check that
    // a) it exists
    // b) the field exists in its schema
    const constraints = fkMaps.map(({ field_name, reference_table }) => {
        return schema.validateSchemaSubset(remote, reference_table, field_name);
    });

    // We also add the constraint that the given fields are in our schema
    constraints.push(schema.validateSchemaSubset(remote, tableName, aliases));

    // Check if all the constraints are satisfied
    const check = Promise.all(constraints).then(r => {
        return r.reduce((prev, curr) => prev && curr);
    });

    return check.then(r => {
        if (!r) throw new Error("Can't add fk on non-existent field");

        const f_outfk = getFKs(remote, tableName).then(fkTuples => {
            return setFK(remote, tableName, fkTuples.concat(fkMaps));
        });

        const f_infks = fkMaps.map(fkMap => {
            const referencedTable = fkMap.reference_table;
            return getInFKs(remote, referencedTable).then(fkTuples => {
                const inFKMapping = utils.mapO(fkMap, (k, v) => {
                    return {
                        [k]: k === 'reference_table' ? tableName : v
                    };
                });
                return setInFK(remote, referencedTable, fkTuples.concat(inFKMapping));
            });
        });

        return Promise.all([f_infks, f_outfk]);
    });
}

// Given a table name, return a list of maps
// `{field_name, reference_table} describing the foreign keys of that table.
//
// Will return the empty list if there are no foreign keys.
//
function getFKs(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.get(remote, meta_key).then(meta => {
        const fks = meta.outfks;
        return fks === undefined ? [] : fks;
    });
}

// Given a table name, return a list of maps
// `{field_name, reference_table} describing the foreign keys pointing to that table.
//
// Will return the empty list if there are no foreign keys.
//
function getInFKs(remote, table_name) {
    const meta_key = keyEncoding.table(table_name);
    return kv.get(remote, meta_key).then(meta => {
        const fks = meta.infks;
        return fks === undefined ? [] : fks;
    });
}

// setFK(r, t, fk) will set the outgoing fk map list of the table `t` to `fk`
function setFK(remote, table_name, fks) {
    const meta_key = keyEncoding.table(table_name);
    return kv.runT(remote, function(tx) {
        return kv.get(tx, meta_key).then(meta => {
            return kv.put(tx, meta_key, Object.assign(meta, { outfks: fks }));
        });
    });
}

// setFK(r, t, fk) will set the inbound fk map list of the table `t` to `fk`
function setInFK(remote, table_name, fks) {
    const meta_key = keyEncoding.table(table_name);
    return kv.runT(remote, function(tx) {
        return kv.get(tx, meta_key).then(meta => {
            return kv.put(tx, meta_key, Object.assign(meta, { infks: fks }));
        });
    });
}

// See internalCorrelateFKs for details.
//
// This function will start a new transaction by default, unless called from inside
// another transaction (given that the current API doesn't allow nested transaction).
// In that case, all operations will be executed in the current transaction.
//
function correlateFKs(remote, tableName, fieldNames) {
    return kv.runT(remote, function(tx) {
        return internalCorrelateFKs(tx, tableName, fieldNames);
    });
}

// Given a table name, and a list of field names, return a list of the foreign key structure
// for any of the fields, in the form [ {reference_table, field_name, alias} ].
//
// This function is unsafe. It MUST be ran inside a transaction.
//
function internalCorrelateFKs(remote, tableName, fieldName) {
    const fieldNames = utils.arreturn(fieldName);
    return getFKs(remote, tableName).then(fks => {
        return fks.filter(({ alias }) => {
            return fieldNames.includes(alias);
        });
    });
}

module.exports = {
    createFK,
    getInFKs,
    correlateFKs
};
