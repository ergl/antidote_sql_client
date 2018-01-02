const kv = require('./db/kv');
const fks = require('./sql/meta/fks');
const indices = require('./sql/meta/indices');
const table = require('./sql/table');
const select = require('./sql/select');
const update = require('./sql/update');
const insert = require('./sql/insert');

module.exports = {
    connect: kv.createRemote,
    reset: table.reset,
    close: kv.closeRemote,
    createTable: table.create,
    createFK: fks.createFK,
    createIndex: indices.addIndex,
    createUniqueIndex: indices.addUniqueIndex,
    insert: insert.insert,
    select: select.select,
    update: update.update,
    runTransaction: kv.runT
};
