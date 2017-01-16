const prefixSep = '/'
const metaSep = '#'
const keyPrefix = 'PK'

const metaIndexPrefix = 'indices'
const metaCounterPrefix = 'keyrange'
const metaSchemaPrefix = 'schema'
const metaPKFieldPrefix = 'primary'
const metaFKFieldsPrefix = 'fks'

function concat(...args) {
    return args.join(prefixSep)
}

function metaConcat(...args) {
    return args.join(metaSep)
}

function isBare(term) {
    return !term.includes(prefixSep)
}

function referencesMeta(term) {
    return term.includes(metaSep)
}

function encode(term) {
    const keys = Object.keys(term)
    if (keys.length !== 1) {
        throw "Can't encode term"
    }

    const type = keys[0]
    const content = term[type]
    switch (type) {
        case 'table': {
            return encodeTableName(content.table_name)
        }
        case 'meta_counter': {
            return encodeMetaCounter(content.table_name)
        }
        case 'meta_index': {
            return encodeMetaIndex(content.table_name)
        }
        case 'meta_schema': {
            return encodeMetaSchema(content.table_name)
        }
        case 'meta_pk': {
            return encodeMetaPK(content.table_name)
        }
        case 'meta_fk': {
            return encodeMetaFK(content.table_name)
        }
        case 'index': {
            return encodeIndex(content.table_name, content.index_name)
        }
        case 'index_pk': {
            return encodePrimary(encodeIndex(content.table_name, content.index_name), content.key_name)
        }
        case 'pk': {
            return encodePrimary(content.table_name, content.key_name)
        }
        case 'index_field': {
            return encodeField(encodeIndex(content.table_name, content.index_name), content.key_name, content.field_name)
        }
        case 'field': {
            return encodeField(content.table_name, content.key_name, content.field_name)
        }
        default: throw "Can't encode term"
    }
}

const encodeTableName = name => name
function decodeTableName(term) {
    return {table: {table_name: term}}
}

function encodeMetaCounter(table) {
    if (isBare(table)) {
        return metaConcat(encodeTableName(table), metaCounterPrefix)
    }

    throw "Can't encode meta information for non-tables"
}

function decodeMetaCounter(term) {
    return {
        meta_counter: {
            table_name: term
        }
    }
}

function encodeMetaIndex(table) {
    if (isBare(table)) {
        return metaConcat(encodeTableName(table), metaIndexPrefix)
    }

    throw "Can't encode meta information for non-tables"
}

function decodeMetaIndex(term) {
    return {
        meta_index: {
            table_name: term
        }
    }
}

function encodeMetaSchema(table) {
    if (isBare(table)) {
        return metaConcat(encodeTableName(table), metaSchemaPrefix)
    }

    throw "Can't encode meta information for non-tables"
}

function decodeMetaSchema(term) {
    return {
        meta_schema: {
            table_name: term
        }
    }
}

function encodeMetaPK(table) {
    if (isBare(table)) {
        return metaConcat(encodeTableName(table), metaPKFieldPrefix)
    }

    throw "Can't encode meta information for non-tables"
}

function decodeMetaPK(term) {
    return {
        meta_pk: {
            table_name: term
        }
    }
}

function encodeMetaFK(table) {
    if (isBare(table)) {
        return metaConcat(encodeTableName(table), metaFKFieldsPrefix)
    }

    throw "Can't encode meta information for non-tables"
}

function decodeMetaFK(term) {
    return {
        meta_fk: {
            table_name: term,
        }
    }
}

function encodePrimary(table, key) {
    const tableEncode = encodeTableName(table)

    if (referencesMeta(tableEncode)) {
        throw "Can't associate keys to table metada"
    }

    if (isBare(table)) {
        return concat(tableEncode, keyPrefix, key)
    }

    return concat(tableEncode, key)
}

function decodePrimary(terms) {
    // terms should be ['name', keyPrefix, 'key_name']
    if (terms.length !== 3) throw "Invalid key"
    return {
        pk: {
            table_name: terms[0],
            key_name: terms[2]
        }
    }
}

function encodeField(table, key, field) {
    return concat(encodePrimary(table, key), field)
}

function decodeField(terms) {
    // terms should be ['name', keyPrefix, 'key_name', 'field_name']
    if (terms.length !== 4) throw "Invalid key"
    return {
        field: {
            table_name: terms[0],
            key_name: terms[2],
            field_name: terms[3]
        }
    }
}

function encodeIndex(table, index_name) {
    return concat(encodeTableName(table), index_name)
}

function decodeIndex(terms) {
    // terms should be ['name', 'index_name']
    if (terms.length !== 2) throw "Invalid key"
    return {
        index: {
            table_name: terms[0],
            index_name: terms[1]
        }
    }
}

function decodeIndexPrimary(terms) {
    // terms should be ['name', 'index_name', 'key_name']
    if (terms.length !== 3) throw "Invalid key"
    return {
        index_pk: {
            table_name: terms[0],
            index_name: terms[1],
            key_name: terms[2]
        }
    }
}

function decodeIndexField(terms) {
    // terms should be ['name', 'index_name', 'key_name', 'field_name']
    if (terms.length !== 4) throw "Invalid key"
    return {
        index_field: {
            table_name: terms[0],
            index_name: terms[1],
            key_name: terms[2],
            field_name: terms[3]
        }
    }
}

function decode(key) {
    const parts = key.split(prefixSep)
    const length = parts.length

    if (length === 1) {
        const term = parts[0]
        if (referencesMeta(term)) {
            const meta_parts = term.split(metaSep)
            const meta_length = meta_parts.length

            if (meta_length === 2) {
                if (meta_parts[1] === metaCounterPrefix) {
                    return decodeMetaCounter(meta_parts[0])
                }

                if (meta_parts[1] === metaIndexPrefix) {
                    return decodeMetaIndex(meta_parts[0])
                }

                if (meta_parts[1] == metaSchemaPrefix) {
                    return decodeMetaSchema(meta_parts[0])
                }

                if (meta_parts[1] == metaPKFieldPrefix) {
                    return decodeMetaPK(meta_parts[0])
                }

                if (meta_parts[1] == metaFKFieldsPrefix) {
                    return decodeMetaFK(meta_parts[0])
                }

                throw "Invalid key"
            }

            throw "Invalid key"
        }

        return decodeTableName(term)
    }

    const is_index = parts[1] !== keyPrefix

    if (length === 2) {
        if (is_index) {
            return decodeIndex(parts)
        } else {
            throw "Invalid key"
        }
    }

    if (length === 3) {
        if (is_index) {
            return decodeIndexPrimary(parts)
        }

        return decodePrimary(parts)
    }

    if (length === 4) {
        if (is_index) {
            return decodeIndexField(parts)
        }

        return decodeField(parts)
    }

    throw "Invalid key"
}

module.exports = {
    decode,
    encode,

    encodeTableName,
    encodePrimary,
    encodeField,
    encodeIndex,

    encodeMetaPK,
    encodeMetaFK,
    encodeMetaIndex,
    encodeMetaSchema,
    encodeMetaCounter,
}