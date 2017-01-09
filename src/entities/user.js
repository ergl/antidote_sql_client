const entity = require('./entity')

function create_id(id) {
    return `user_${id}`
}

function create_idx_id(id) {
    return `user_name_${id}`
}

// We don't want to keep the index as state, too much burden
// Could expose it, but then wrap it with something on a higher level
// Drop `keys()`, too much burden, maybe do the same as jessy and store an array somewhere
function build(init = 0, idx_module = buildNameIndex()) {
    let user_id = init
    return {
        // Reconsider this - we _do_ want the id in the database,
        // but we _don't_ want the entity to create an user,
        // we should just provide the name of the region and lookup that
        // (region has a name index)
        make(name, rating, region_entity) {
            const id = create_id(user_id++)
            const contents = {
                name: name,
                rating: rating,
                region_id: region_entity.id
            }
            const indices = [{
                field: 'name',
                module: idx_module
            }]
            return entity.makeEntity('users', id, contents, {fks: ['region_id'], indices: indices})
        },

        keys() {
            return [...new Array(user_id).keys()].map(create_id)
        }

    }
}

function buildNameIndex(init = 0) {
    let user_name_id = init
    return {
        make(fk_id, name) {
            const id = create_idx_id(user_name_id++)
            const contents = {name: name, user_id: fk_id}
            return entity.makeEntity('users_name', id, contents, {fks: ['user_id']})
        },

        keys() {
            return [...new Array(user_name_id).keys()].map(create_idx_id)
        }
    }
}


module.exports = {
    build,
    buildNameIndex
}
