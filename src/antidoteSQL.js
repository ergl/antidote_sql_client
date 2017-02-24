const kv = require('./db/kv');
const fks = require('./sql/meta/fks');
const indices = require('./sql/meta/indices');
const table = require('./sql/table');

module.exports = {
    connect: kv.createRemote,
    close: kv.closeRemote,
    createTable: table.create,
    createFK: fks.addFK_T,
    createIndex: indices.addIndex,
    createUniqueIndex: indices.addUniqueIndex,
    insert: table.insertInto_T,
    select: table.select_T
};
