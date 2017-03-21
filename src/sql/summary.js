const kv = require('./../db/kv');
const keyEncoding = require('./../db/keyEncoding');

function addTable(remote, tableName) {
    return kv.runT(remote, function(tx) {
        return updateSummary(tx, tableName);
    });
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
    addTable
};
