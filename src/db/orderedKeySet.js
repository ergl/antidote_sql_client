const kset = require('kset');

function add({ key }, t) {
    return kset.add(key, t);
}

function subkeys(key, t) {
    const strict_subkeys = strictSubkeys(key, t);
    return [key, ...strict_subkeys];
}

function strictSubkeys({ key }, t) {
    return kset.subkeys(key, t).map(key => {
        return { key };
    });
}

function remove({ key }, t) {
    return kset.remove(key, t);
}

function printContents(t) {
    return kset.contents(t).map(kset.repr);
}

function dumpKeys(t) {
    return kset.contents(t).map(k => ({ key: k }));
}

exports.empty = kset.empty;

exports.add = add;
exports.remove = remove;

exports.subkeys = subkeys;
exports.strictSubkeys = strictSubkeys;

exports.wasChanged = kset.changed;

exports.serialize = kset.toJson;
exports.deserialize = kset.fromJson;

exports.dumpKeys = dumpKeys;
exports.printContents = printContents;
