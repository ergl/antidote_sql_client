const entity = require('./entity')

function create_id(id) {
    return `category_${id}`
}

function build(init = 0) {
    let category_id = init
    return {
        make(name) {
            const id = create_id(category_id++)
            return entity.makeEntity("categories", id, { name: name })
        },

        keys() {
            return [...new Array(category_id).keys()].map(create_id)
        }
    }
}

module.exports = {
    build
}
