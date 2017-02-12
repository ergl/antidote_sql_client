const kset = require('./internal/kset');

module.exports = {
    d_int: kset.d_int,
    d_float: kset.d_float,
    d_string: kset.d_string,
    table: kset.table,
    spk: kset.spk,
    field: kset.field,
    index_key: kset.index_key,
    uindex_key: kset.uindex_key,
    repr: kset.repr
};
