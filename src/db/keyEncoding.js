const prefixSep = '/';
const keyPrefix = 'PK';

function concat(...args) {
    return args.join(prefixSep);
}

function isBare(term) {
    return !term.includes(prefixSep);
}

function encode(term) {
    const keys = Object.keys(term);
    if (keys.length !== 1) {
        throw "Can't encode term";
    }

    const type = keys[0];
    const content = term[type];
    switch (type) {
        case 'table': {
            return encodeTableName(content.table_name);
        }
        case 'index': {
            return encodeIndex(content.table_name, content.index_name);
        }
        case 'index_pk': {
            return encodePrimary(
                encodeIndex(content.table_name, content.index_name),
                content.key_name
            );
        }
        case 'pk': {
            return encodePrimary(content.table_name, content.key_name);
        }
        case 'index_field': {
            return encodeField(
                encodeIndex(content.table_name, content.index_name),
                content.key_name,
                content.field_name
            );
        }
        case 'field': {
            return encodeField(content.table_name, content.key_name, content.field_name);
        }
        default:
            throw "Can't encode term";
    }
}

const encodeTableName = name => name;
function decodeTableName(term) {
    return { table: { table_name: term } };
}

function encodePrimary(table, key) {
    const tableEncode = encodeTableName(table);

    if (isBare(table)) {
        return concat(tableEncode, keyPrefix, key);
    }

    return concat(tableEncode, key);
}

function decodePrimary(terms) {
    // terms should be ['name', keyPrefix, 'key_name']
    if (terms.length !== 3) throw invalid(terms);
    return {
        pk: {
            table_name: terms[0],
            key_name: terms[2]
        }
    };
}

function encodeField(table, key, field) {
    return concat(encodePrimary(table, key), field);
}

function decodeField(terms) {
    // terms should be ['name', keyPrefix, 'key_name', 'field_name']
    if (terms.length !== 4) throw invalid(terms);
    return {
        field: {
            table_name: terms[0],
            key_name: terms[2],
            field_name: terms[3]
        }
    };
}

function encodeIndex(table, index_name) {
    return concat(encodeTableName(table), index_name);
}

function decodeIndex(terms) {
    // terms should be ['name', 'index_name']
    if (terms.length !== 2) throw invalid(terms);
    return {
        index: {
            table_name: terms[0],
            index_name: terms[1]
        }
    };
}

function encodeIndexPrimary(table, index, key) {
    return concat(encodeIndex(table, index), key);
}

function decodeIndexPrimary(terms) {
    // terms should be ['name', 'index_name', 'key_name']
    if (terms.length !== 3) throw invalid(terms);
    return {
        index_pk: {
            table_name: terms[0],
            index_name: terms[1],
            key_name: terms[2]
        }
    };
}

function encodeIndexField(table, index, key, field) {
    return concat(encodeIndexPrimary(table, index, key), field);
}

function decodeIndexField(terms) {
    // terms should be ['name', 'index_name', 'key_name', 'field_name']
    if (terms.length !== 4) throw invalid(terms);
    return {
        index_field: {
            table_name: terms[0],
            index_name: terms[1],
            key_name: terms[2],
            field_name: terms[3]
        }
    };
}

function decode(key) {
    if (typeof key !== 'string') throw invalid(key);

    const parts = key.split(prefixSep);
    const length = parts.length;

    if (length === 1) {
        const term = parts[0];
        return decodeTableName(term);
    }

    const is_index = parts[1] !== keyPrefix;

    if (length === 2) {
        if (is_index) {
            return decodeIndex(parts);
        } else {
            throw invalid(key);
        }
    }

    if (length === 3) {
        if (is_index) {
            return decodeIndexPrimary(parts);
        }

        return decodePrimary(parts);
    }

    if (length === 4) {
        if (is_index) {
            return decodeIndexField(parts);
        }

        return decodeField(parts);
    }

    throw invalid(key);
}

function invalid(key) {
    return `Invalid key ${key}`;
}

module.exports = {
    decode,
    encode,
    encodeTableName,
    encodePrimary,
    encodeField,
    encodeIndex,
    encodeIndexPrimary,
    encodeIndexField
};
