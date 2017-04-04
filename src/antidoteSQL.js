const kv = require('./db/kv');
const fks = require('./sql/meta/fks');
const indices = require('./sql/meta/indices');
const table = require('./sql/table');

module.exports = {
    connect: kv.createRemote,
    reset: table.reset,
    close: kv.closeRemote,
    createTable: table.create,
    createFK: fks.addFK_T,
    createIndex: indices.addIndex,
    createUniqueIndex: indices.addUniqueIndex,
    insert: table.insert,
    select: table.select,
    update: table.update,
    runTransaction: kv.runT
};
