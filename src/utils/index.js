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

module.exports = {
    arreturn,
    flatten
}
