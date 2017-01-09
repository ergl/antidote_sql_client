function makeEntity(table_name, key, values, props = {fks: [], indices: []}) {
    const raw_ent = { id: key, content: values, table_name: table_name, fks: [], indices: [] }

    const fks = props.fks || [],
          indices = props.indices || []

    const with_fk = (Array.isArray(fks) && fks.length !== 0),
          with_idx = (Array.isArray(indices) && indices.length !== 0)

    let res = raw_ent
    if (with_idx) {
        res = addIdx(raw_ent, props.indices)
    }

    if (with_fk) {
        res = addFK(raw_ent, props.fks)
    }

    return res
}

function addFK(entity, fields) {
    let field_content = Array.isArray(fields) ? fields : [fields]
    let fks = field_content.map(f => entity.content[f])
    return Object.assign(entity, {
        fks: entity.fks.concat(fks)
    })
}

function addIdx(entity, fields) {
    return Object.assign(entity, {
        indices: entity.indices.concat(fields)
    })
}

function readField(values, fields) {
    if (fields === undefined) {
        return values
    }

    if (Array.isArray(fields)) {
        return fields.map(field => values[field])
    }

    return values[fields]
}

module.exports = {
    makeEntity,
    readField
}