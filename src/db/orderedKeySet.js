const kset = require('kset');
const List = require('bs-platform/lib/js/list');

const ML_BOTTOM = '$$__ML_BOTTOM__$$';

function safe_hd(ml_l) {
    try {
        return List.hd(ml_l);
    } catch (e) {
        return ML_BOTTOM;
    }
}

function safe_tl(ml_l) {
    try {
        return List.tl(ml_l);
    } catch (e) {
        return [];
    }
}

function ml_to_list(ml_l) {
    let l = ml_l;
    let acc = [];
    let el = safe_hd(ml_l);
    while (el !== ML_BOTTOM) {
        acc.push(el);
        l = safe_tl(l);
        el = safe_hd(l);
    }
    return acc;
}

function unwrap_js_t_list(ml_js_t) {
    if (ml_js_t === undefined) return ml_js_t;
    return ml_to_list(ml_js_t);
}

function wrap_add({ key }, t) {
    return kset.add(key, t);
}

function wrap_find({ key }, t) {
    return kset.find(key, t);
}

function wrap_next({ key }, t) {
    return kset.next_key(key, t);
}

function wrap_prev({ key }, t) {
    return kset.prev_key(key, t);
}

function wrap_subkeys(key, t) {
    const strict_subkeys = wrap_strict_subkeys(key, t);
    return [key, ...strict_subkeys];
}

function wrap_strict_subkeys({ key }, t) {
    return unwrap_js_t_list(kset.subkeys(key, t)).map(key => {
        return { key };
    });
}

function wrap_batch(ini, fin, t) {
    return unwrap_js_t_list(kset.batch(ini.key, fin.key, t)).map(key => {
        return { key };
    });
}

function wrap_swap({ key: oldKey }, { key: newKey }, t) {
    return kset.swap(oldKey, newKey, t);
}

function wrap_remove({ key }, t) {
    return kset.remove(key, t);
}

function wrap_contents(t) {
    return unwrap_js_t_list(kset.contents(t)).map(kset.repr);
}

function raw_contents(t) {
    return unwrap_js_t_list(kset.contents(t));
}

function serialize(t) {
    return raw_contents(t).map(serializeKey);
}

function wrap_serialize_key({ key }) {
    return serializeKey(key);
}

function serializeKey(key) {
    return Object.keys(key).reduce(
        (acc, curr) => {
            const ns = key[curr];
            const nested = Array.isArray(ns) ? [serializeKey(ns)] : ns;
            return Object.assign(acc, {
                [curr]: nested
            });
        },
        {}
    );
}

function deserialize(ser) {
    let empt = kset.empty();
    ser.forEach(serkey => {
        kset.add(deserializeKey(serkey), empt);
    });
    return empt;
}

function wrap_deserialize_key(ser) {
    return { key: deserializeKey(ser) };
}

function deserializeKey(key) {
    return Object.keys(key).reduce(
        (acc, curr) => {
            const ns = key[curr];
            acc[curr] = Array.isArray(ns) ? deserializeKey(ns[0]) : ns;
            return acc;
        },
        []
    );
}

module.exports = {
    empty: () => kset.empty(),
    add: wrap_add,
    find: wrap_find,
    next_key: wrap_next,
    prev_key: wrap_prev,
    subkeys: wrap_subkeys,
    strictSubKeys: wrap_strict_subkeys,
    batch: wrap_batch,
    swap: wrap_swap,
    remove: wrap_remove,
    contents: wrap_contents,
    serialize,
    deserialize,
    serializeKey: wrap_serialize_key,
    deserializeKey: wrap_deserialize_key
};
