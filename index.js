let _ = require('underscore')
_.mixin(require('underscore.deep'))
let genericPool = require('generic-pool')
let debug = require('debug')('thrift-pool')
let TIMEOUT_MESSAGE = 'Thrift-pool: Connection timeout'
let CLOSE_MESSAGE = 'Thrift-pool: Connection closed'

let __slice = [].slice

let create_cb = function(thrift, pool_options, thrift_options, cb) {
    let connection
    cb = _.once(cb)
    if (pool_options.ssl == null) {
        pool_options.ssl = false
    }
    if (pool_options.ssl) {
        connection = thrift.createSSLConnection(pool_options.host, pool_options.port, thrift_options)
    } else {
        connection = thrift.createConnection(pool_options.host, pool_options.port, thrift_options)
    }
    connection.__ended = false
    if (pool_options.ttl != null) {
        connection.__reap_time = Date.now() + _.random(pool_options.ttl / 2, pool_options.ttl * 1.5)
    }
    connection.on('connect', function() {
        debug('in connect callback')
        connection.connection.setKeepAlive(true)
        return cb(null, connection)
    })
    connection.on('error', function(err) {
        debug('in error callback')
        connection.__ended = true
        return cb(err)
    })
    connection.on('close', function() {
        debug('in close callback')
        connection.__ended = true
        return cb(new Error(CLOSE_MESSAGE))
    })
    if (thrift_options.timeout != null) {
        debug('adding timeout listener')
        return connection.on('timeout', function() {
            debug('in timeout callback')
            connection.__ended = true
            return cb(new Error(TIMEOUT_MESSAGE))
        })
    }
}

function create_pool (thrift, pool_options, thrift_options) {
    if (pool_options == null) {
        pool_options = {}
    }
    if (thrift_options == null) {
        thrift_options = {}
    }
    return genericPool.Pool({
        name: 'thrift',
        create (cb) {
            return create_cb(thrift, pool_options, thrift_options, cb)
        },
        destroy (connection) {
            debug('in destroy')
            return connection.end()
        },
        validate (connection) {
            debug('in validate')
            if (connection.__ended) {
                return false
            }
            if (pool_options.ttl == null) {
                return true
            }
            return connection.__reap_time > Date.now()
        },
        log: pool_options.log,
        max: pool_options.max_connections,
        min: pool_options.min_connections,
        idleTimeoutMillis: pool_options.idle_timeout
    })
}

module.exports = function(thrift, service, pool_options, thrift_options) {
    let key, pool
    if (pool_options == null) {
        pool_options = {}
    }
    if (thrift_options == null) {
        thrift_options = {}
    }
    let _ref = ['host', 'port']
    for (let _i = 0, _len = _ref.length; _i < _len; _i++) {
        key = _ref[_i]
        if (!pool_options[key]) {
            throw new Error('Thrift-pool: You must specify ' + key)
        }
    }
    pool_options = _(pool_options).defaults({
        log: false,
        max_connections: 1,
        min_connections: 0,
        idle_timeout: 30000
    })
    pool = create_pool(thrift, pool_options, thrift_options)
    function add_listeners (connection, cb_error, cb_timeout, cb_close) {
        connection.on('error', cb_error)
        connection.on('close', cb_close)
        if (thrift_options.timeout != null) {
            return connection.on('timeout', cb_timeout)
        }
    }
    function remove_listeners (connection, cb_error, cb_timeout, cb_close) {
        connection.removeListener('error', cb_error)
        connection.removeListener('close', cb_close)
        if (thrift_options.timeout != null) {
            return connection.removeListener('timeout', cb_timeout)
        }
    }
    function wrap_thrift_fn (fn) {
        // using ...args to replace 'arguments' object
        return function(...args) {
            let cb, _j
            // if there is one or more parameters
            if (args.length >= 1) {
                debug('typeof args[args.length - 1]', typeof args[args.length - 1])
                // if the last parameter is not a function,
                // then pull the last parameter as the callback function
                // if not, assuming it's using Promise(await/async)
                if (typeof args[args.length - 1] === 'function') {
                    // then need to pop out the cb
                    // and pop could also remove the last element
                    // from the argument array
                    cb = args.pop()
                }
            }

            function acquireAndExec(cb) {
                cb = _.once(cb)
                return pool.acquire(function(err, connection) {
                    debug('Connection acquired')
                    debug({
                        err: err
                    })
                    debug({
                        connection: connection
                    })
                    if (err != null) {
                        return cb(err)
                    }
                    let cb_error = function(err) {
                        debug('in error callback, post-acquire listener')
                        return cb(err)
                    }
                    let cb_timeout = function() {
                        debug('in timeout callback, post-acquire listener')
                        return cb(new Error(TIMEOUT_MESSAGE))
                    }
                    let cb_close = function() {
                        debug('in close callback, post-acquire listener')
                        return cb(new Error(CLOSE_MESSAGE))
                    }
                    add_listeners(connection, cb_error, cb_timeout, cb_close)
                    let client = thrift.createClient(service, connection)
                    debug('Client created')
                    debug({
                        client
                    })
                    try {
                        return client[fn](...args, (err, ...results) => {
                            debug('In client callback')
                            remove_listeners(connection, cb_error, cb_timeout, cb_close)
                            pool.release(connection)
                            return cb(err, ...results)
                        })
                    } catch (err) {
                        return cb(err)
                    }
                })
            }

            if (cb) {
                // provided callback func
                return acquireAndExec(cb)
            } else {
                // did not provide callback func,
                // assuming using promise
                return new Promise((resolve, reject) => {
                    acquireAndExec((err, result) => {
                        if (err) {
                            return reject(err)
                        } else {
                            return resolve(result)
                        }
                    })
                })
            }
        }
    }
    return _.mapValues(_.clone(service.Client.prototype), function(fn, name) {
        return wrap_thrift_fn(name)
    })
}

_.extend(module.exports, {
    _private: {
        create_pool: create_pool,
        TIMEOUT_MESSAGE: TIMEOUT_MESSAGE,
        CLOSE_MESSAGE: CLOSE_MESSAGE
    }
})
