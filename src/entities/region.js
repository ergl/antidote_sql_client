const entity = require('./entity')

function create_id(id) {
    return `region_${id}`
}

function create_idx_id(id) {
    return `region_name_${id}`
}

function build(init = 0, idx_module = buildNameIndex()) {
    let region_id = init
    return {
        make(name) {
            const id = create_id(region_id++)
            const indices = [{
                field: 'name',
                module: idx_module
            }]
            return entity.makeEntity('regions',id, { name: name }, { indices: indices })
        },

        keys() {
            return [...new Array(region_id).keys()].map(create_id)
        }
    }
}

function buildNameIndex(init = 0) {
    let region_name_id = init
    return {
        make(fk_id, name) {
            const id = create_idx_id(region_name_id++)
            const contents = {name: name, region_id: fk_id}
            return entity.makeEntity('regions_name', id, contents, {fks: ['region_id']})
        },

        keys() {
            return [...new Array(region_name_id).keys()].map(create_idx_id)
        }
    }
}

module.exports = {
    build,
    buildNameIndex
}
