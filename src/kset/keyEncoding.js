const kset = require('./internal/kset');

function wrap_table(t) {
    return kset.repr(kset.table(t));
}

function wrap_spk(t, any) {
    return kset.repr(kset.spk(t, any));
}

function wrap_field(t, n, fl) {
    return kset.repr(kset.field(t, n, fl));
}

module.exports = {
    d_int: kset.d_int,
    d_float: kset.d_float,
    d_string: kset.d_string,
    table: wrap_table,
    spk: wrap_spk,
    field: wrap_field,
    index_key: kset.index_key,
    uindex_key: kset.uindex_key
};
