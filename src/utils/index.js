// Poor man's list monad return
function arreturn(v) {
    return Array.isArray(v) ? v : [v]
}

module.exports = {
    arreturn
}
