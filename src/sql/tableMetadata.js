const kv = require('./../db/kv');
const keyEncoding = require('./../db/keyEncoding');

function createMeta(remote, table_name, pk_field, schema) {
    const meta_key = keyEncoding.table(table_name);
    const meta_content = {
        infks: [],
        outfks: [],
        indices: [],
        uindices: [],
        schema: schema,
        current_pk_value: 0,
        primary_key_field: pk_field
    };

    // TODO: Run in a single tx
    // The problem is that the kset hasn't been updated with
    // the summary by the time we get to the `put` part.
    // Could update manuall the tx.kset in this case
    return kv
        .runT(remote, function(tx) {
            return updateSummary(tx, table_name);
        })
        .then(_ => kv.runT(remote, function(tx) {
            return kv.put(tx, meta_key, meta_content);
        }));
}

function updateSummary(remote, tableName) {
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
