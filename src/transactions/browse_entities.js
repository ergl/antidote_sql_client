const db = require('../db')

function build(browsable_module) {
    return tx => db.getKeys(tx, browsable_module.keys())
}

module.exports = {
    build
}
