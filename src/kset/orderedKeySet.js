const kset = require('./internal/kset');
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

function wrap_find(x, t) {
    return unwrap_js_t_list(kset.find(x, t));
}

function wrap_subkeys(ini, t) {
    return unwrap_js_t_list(kset.subkeys(ini, t));
}

function wrap_batch(ini, fin) {
    return unwrap_js_t_list(kset.batch(ini, fin));
}

module.exports = {
    empty: kset.empty,
    add: kset.add,
    find: wrap_find,
    next_key: kset.next_key,
    prev_key: kset.prev_key,
    subkeys: wrap_subkeys,
    batch: wrap_batch
};
