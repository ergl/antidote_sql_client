// Poor man's list monad return
function arreturn(v) {
    return Array.isArray(v) ? v : [v];
}

// Because, apparently, JS doesn't have Array.flatten ???
function flatten(arr) {
    return arr.reduce(
        (a, b) => {
            return a.concat(Array.isArray(b) ? flatten(b) : b);
        },
        []
    );
}

// Remove duplicates from an array
function squash(arr) {
    return [...new Set(arr)];
}

// mapOKeys({ k: v, ... }, fn) will return { fn(k): v, ... }
function mapOKeys(obj, fn) {
    const old_keys = Object.keys(obj);
    return old_keys.reduce(
        (acc, curr_key) => {
            return Object.assign(acc, {
                [fn(curr_key)]: obj[curr_key]
            });
        },
        {}
    );
}

// mapOValues({ k: v, ... }, fn) will return { k: fn(v), ... }
function mapOValues(obj, fn) {
    return Object.keys(obj).reduce(
        (acc, curr_key) => {
            return Object.assign(acc, {
                [curr_key]: fn(obj[curr_key])
            });
        },
        {}
    );
}

// mapOValues({ k: v, ... }, fn) will return { fn(k, v).key: fn(k, v).value, ... }
// Where fn should return a pair { key: value }
function mapO(obj, fn) {
    return Object.keys(obj).reduce(
        (acc, curr_key) => {
            return Object.assign(acc, fn(curr_key, obj[curr_key]));
        },
        {}
    );
}

// filterOKeys({ k: v, ... }, fn) will return { k: v } such that fn(k) = true
function filterOKeys(obj, fn) {
    return Object.keys(obj).reduce(
        (acc, curr_key) => {
            let res;
            if (fn(curr_key)) {
                res = Object.assign(acc, { [curr_key]: obj[curr_key] });
            } else {
                res = acc;
            }
            return res;
        },
        {}
    );
}

// filterOKeys({ k: v, ... }, fn) will return { k: v } such that fn(k, v) = true
function filterO(obj, fn) {
    return Object.keys(obj).reduce(
        (acc, curr_key) => {
            let res;
            const curr_value = obj[curr_key];
            if (fn(curr_key, curr_value)) {
                res = Object.assign(acc, { [curr_key]: curr_value });
            } else {
                res = acc;
            }
            return res;
        },
        {}
    );
}

function isFunction(fn) {
    return typeof fn === 'function';
}

module.exports = {
    squash,
    flatten,
    arreturn,
    mapO,
    mapOKeys,
    mapOValues,
    filterO,
    filterOKeys,
    isFunction
};
