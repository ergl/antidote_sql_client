const keyEncoding = require('./../../db/keyEncoding')

function metaRef(remote, table_name) {
    return remote.map(keyEncoding.encodeTableName(table_name))
}

module.exports = {
    metaRef
}
