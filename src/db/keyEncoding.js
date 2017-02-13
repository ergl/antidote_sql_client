const prefixSep = '/';

function concat(...args) {
    return args.join(prefixSep);
}

const encodeTableName = name => name;

function encodeIndex(table, index_name) {
    return concat(encodeTableName(table), index_name);
}

function encodeIndexPrimary(table, index, key) {
    return concat(encodeIndex(table, index), key);
}

function encodeIndexField(table, index, key, field) {
    return concat(encodeIndexPrimary(table, index, key), field);
}

module.exports = {
    encodeIndex,
    encodeIndexPrimary,
    encodeIndexField
};
