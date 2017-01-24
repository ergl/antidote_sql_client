// Poor man's list monad return
function arreturn(v) {
    return Array.isArray(v) ? v : [v]
}

// Because, apparently, JS doesn't have Array.flatten ???
function flatten(arr) {
    return arr.reduce((a, b) => {
        return a.concat(Array.isArray(b) ? flatten(b) : b)
    }, [])
}

function mapOKeys(obj, fn) {
    const old_keys = Object.keys(obj)
    return old_keys.reduce((acc, curr_key) => {
        return Object.assign(acc, {
            [fn(curr_key)]: obj[curr_key]
        })
    }, {})
}

function mapOValues(obj, fn) {
    return Object.keys(obj).reduce((acc, curr_key) => {
        return Object.assign(acc, {
            [curr_key]: fn(obj[curr_key])
        })
    }, {})
}

function mapO(obj, fn) {
    return Object.keys(obj).reduce((acc, curr_key) => {
        const [new_k, new_v] = fn(curr_key, obj[curr_key])
        return Object.assign(acc, {
            [new_k]: new_v
        })
    }, {})
}

module.exports = {
    flatten,
    arreturn,

    mapO,
    mapOKeys,
    mapOValues
}
