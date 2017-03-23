const kset = require('kset');

const SET_KEY = '$$__ML_KSET__$$';

function set_key() {
    return SET_KEY;
}

function wrap_table(t) {
    return { key: kset.table(t) };
}

function wrap_spk(t, any) {
    return { key: kset.spk(t, kset.int(any)) };
}

function wrap_field(t, n, fl) {
    return { key: kset.field(t, kset.int(n), fl) };
}

function wrap_index_key(t, i, f, v, k) {
    return { key: kset.index_key(t, i, f, kset.string(v), kset.int(k)) };
}

function wrap_raw_index_field_value(t, i, f, v) {
    return { key: kset.raw_index_field_value(t, i, f, kset.string(v)) };
}

function wrap_uindex_key(t, i, f, v) {
    return { key: kset.uindex_key(t, i, f, kset.string(v)) };
}

function isData({ key }) {
    return kset.is_data(key);
}

function isIndex({ key }) {
    return kset.is_index(key);
}

function isUniqueIndex({ key }) {
    return kset.is_uindex(key);
}

function toString({ key }) {
    return kset.repr(key);
}

function fieldFromKey({ key }) {
    return kset.field_from_key(key);
}

function getIndexData({ key }) {
    return kset.get_index_data(key);
}

module.exports = {
    set_key,
    table: wrap_table,
    spk: wrap_spk,
    field: wrap_field,
    index_key: wrap_index_key,
    uindex_key: wrap_uindex_key,
    isData,
    isIndex,
    isUniqueIndex,
    fieldFromKey,
    toString,
    raw_index_field_value: wrap_raw_index_field_value,
    getIndexData
};
