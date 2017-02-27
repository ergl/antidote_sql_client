const kset = require('kset');

const SET_KEY = '$$__ML_KSET__$$';

function set_key() {
    return SET_KEY;
}

function wrap_table(t) {
    return { key: kset.table(t) };
}

function wrap_spk(t, any) {
    return { key: kset.spk(t, kset.d_int(any)) };
}

function wrap_field(t, n, fl) {
    return { key: kset.field(t, kset.d_int(n), fl) };
}

function wrap_index_key(t, i, f, v, k) {
    return { key: kset.index_key(t, i, f, kset.d_string(v), kset.d_int(k)) };
}

function wrap_uindex_key(t, i, f, v) {
    return { key: kset.uindex_key(t, i, f, kset.d_string(v)) };
}

function wrap_string({ key }) {
    return kset.repr(key);
}

module.exports = {
    set_key,
    table: wrap_table,
    spk: wrap_spk,
    field: wrap_field,
    index_key: wrap_index_key,
    uindex_key: wrap_uindex_key,
    toString: wrap_string
};
