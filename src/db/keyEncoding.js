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

function wrap_raw_index_field_value(t, i, f, v) {
    return { key: kset.raw_index_field_value(t, i, f, kset.d_string(v)) };
}

function wrap_uindex_key(t, i, f, v) {
    return { key: kset.uindex_key(t, i, f, kset.d_string(v)) };
}

function wrap_is_data({ key }) {
    return kset.is_data(key);
}

function wrap_is_index({ key }) {
    return kset.is_index(key);
}

function wrap_is_uindex({ key }) {
    return kset.is_uindex(key);
}

function wrap_string({ key }) {
    return kset.repr(key);
}

function wrap_field_from_key({ key }) {
    return kset.field_from_key(key);
}

// TODO: Extend to allow other kinds of data
function unwrap_data(data) {
    switch (data.tag) {
        case 0:
            return data[0];
        default:
            throw new Error(`Can't unwrap ${data}`);
    }
}

function wrap_get_index_data({ key }) {
    return unwrap_data(kset.get_index_data(key));
}

module.exports = {
    set_key,
    table: wrap_table,
    spk: wrap_spk,
    field: wrap_field,
    index_key: wrap_index_key,
    uindex_key: wrap_uindex_key,
    isData: wrap_is_data,
    isIndex: wrap_is_index,
    isUIndex: wrap_is_uindex,
    fieldFromKey: wrap_field_from_key,
    toString: wrap_string,
    raw_index_field_value: wrap_raw_index_field_value,
    getIndexData: wrap_get_index_data
};
