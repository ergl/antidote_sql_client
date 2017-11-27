const kv = require('./../db/kv');
const keyEncoding = require('./../db/keyEncoding');

function createMeta(remote, table_name, pk_field, schema) {
    const meta_key = keyEncoding.table(table_name);
    // TODO: Split into several keys
    // instead of putting everthing under the meta_key (table key)
    const meta_content = {
        infks: [],
        outfks: [],
        indices: [],
        uindices: [],
        schema: schema,
        current_pk_value: 0,
        primary_key_field: pk_field
    };

    return kv.runT(remote, function(tx) {
        return createSummaryEntry(tx, table_name)
            .then(_ => kv.populateSet(tx))
            .then(tx => kv.put(tx, meta_key, meta_content));
    });
}

// Create a summary entry for the given table name
//
// If the summary doesn't exist, create it
function createSummaryEntry(remote, tableName) {
    const setKey = keyEncoding.generateSetKey(tableName);
    const summaryEntry = { tableName, setKey };

    return kv.runT(remote, function(tx) {
        return kv
            .readSummary(tx)
            .then(oldSummary => {
                const elt = oldSummary.find(elt => elt.tableName === tableName);
                if (elt === undefined) {
                    return oldSummary.concat(summaryEntry);
                }

                Object.assign(elt, summaryEntry);
                return oldSummary;
            })
            .then(summary => {
                return kv.writeSummary(tx, summary);
            });
    });
}

module.exports = {
    createMeta
};
