// Generated by CoffeeScript 1.8.0
(function() {
  var CLOSE_MESSAGE, TIMEOUT_MESSAGE, async, create_cb, create_pool, debug, genericPool, _,
    __slice = [].slice;

  _ = require("underscore");

  _.mixin(require("underscore.deep"));

  async = require("async");

  genericPool = require("generic-pool");

  debug = require("debug")("thrift-pool");

  TIMEOUT_MESSAGE = "Thrift-pool: Connection timeout";

  CLOSE_MESSAGE = "Thrift-pool: Connection closed";

  create_cb = function(thrift, pool_options, thrift_options, cb) {
    var connection;
    cb = _.once(cb);
    if (pool_options.ssl == null) {
      pool_options.ssl = false;
    }
    if (pool_options.ssl) {
      connection = thrift.createSSLConnection(pool_options.host, pool_options.port, thrift_options);
    } else {
      connection = thrift.createConnection(pool_options.host, pool_options.port, thrift_options);
    }
    connection.__ended = false;
    if (pool_options.ttl != null) {
      connection.__reap_time = Date.now() + _.random(pool_options.ttl / 2, pool_options.ttl * 1.5);
    }
    connection.on("connect", function() {
      debug("in connect callback");
      connection.connection.setKeepAlive(true);
      return cb(null, connection);
    });
    connection.on("error", function(err) {
      debug("in error callback");
      connection.__ended = true;
      return cb(err);
    });
    connection.on("close", function() {
      debug("in close callback");
      connection.__ended = true;
      return cb(new Error(CLOSE_MESSAGE));
    });
    if (thrift_options.timeout != null) {
      debug("adding timeout listener");
      return connection.on("timeout", function() {
        debug("in timeout callback");
        connection.__ended = true;
        return cb(new Error(TIMEOUT_MESSAGE));
      });
    }
  };

  create_pool = function(thrift, pool_options, thrift_options) {
    var pool;
    if (pool_options == null) {
      pool_options = {};
    }
    if (thrift_options == null) {
      thrift_options = {};
    }
    return pool = genericPool.Pool({
      name: "thrift",
      create: function(cb) {
        return create_cb(thrift, pool_options, thrift_options, cb);
      },
      destroy: function(connection) {
        debug("in destroy");
        return connection.end();
      },
      validate: function(connection) {
        debug("in validate");
        if (connection.__ended) {
          return false;
        }
        if (pool_options.ttl == null) {
          return true;
        }
        return connection.__reap_time > Date.now();
      },
      log: pool_options.log,
      max: pool_options.max_connections,
      min: pool_options.min_connections,
      idleTimeoutMillis: pool_options.idle_timeout
    });
  };

  module.exports = function(thrift, service, pool_options, thrift_options) {
    var add_listeners, key, pool, remove_listeners, wrap_thrift_fn, _i, _len, _ref;
    if (pool_options == null) {
      pool_options = {};
    }
    if (thrift_options == null) {
      thrift_options = {};
    }
    _ref = ["host", "port"];
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      key = _ref[_i];
      if (!pool_options[key]) {
        throw new Error("Thrift-pool: You must specify " + key);
      }
    }
    pool_options = _(pool_options).defaults({
      log: false,
      max_connections: 1,
      min_connections: 0,
      idle_timeout: 30000
    });
    pool = create_pool(thrift, pool_options, thrift_options);
    add_listeners = function(connection, cb_error, cb_timeout, cb_close) {
      connection.on("error", cb_error);
      connection.on("close", cb_close);
      if (thrift_options.timeout != null) {
        return connection.on("timeout", cb_timeout);
      }
    };
    remove_listeners = function(connection, cb_error, cb_timeout, cb_close) {
      connection.removeListener("error", cb_error);
      connection.removeListener("close", cb_close);
      if (thrift_options.timeout != null) {
        return connection.removeListener("timeout", cb_timeout);
      }
    };
    wrap_thrift_fn = function(fn) {
      return function() {
        var args, cb, _j;
        args = 2 <= arguments.length ? __slice.call(arguments, 0, _j = arguments.length - 1) : (_j = 0, []), cb = arguments[_j++];
        return pool.acquire(function(err, connection) {
          var cb_close, cb_error, cb_timeout, client;
          debug("Connection acquired");
          debug({
            err: err
          });
          debug({
            connection: connection
          });
          if (err != null) {
            return cb(err);
          }
          cb = _.once(cb);
          cb_error = function(err) {
            debug("in error callback, post-acquire listener");
            return cb(err);
          };
          cb_timeout = function() {
            debug("in timeout callback, post-acquire listener");
            return cb(new Error(TIMEOUT_MESSAGE));
          };
          cb_close = function() {
            debug("in close callback, post-acquire listener");
            return cb(new Error(CLOSE_MESSAGE));
          };
          add_listeners(connection, cb_error, cb_timeout, cb_close);
          client = thrift.createClient(service, connection);
          debug("Client created");
          debug({
            client: client
          });
          try {
            return client[fn].apply(client, __slice.call(args).concat([function() {
              var err, results;
              err = arguments[0], results = 2 <= arguments.length ? __slice.call(arguments, 1) : [];
              debug("In client callback");
              remove_listeners(connection, cb_error, cb_timeout, cb_close);
              pool.release(connection);
              return cb.apply(null, [err].concat(__slice.call(results)));
            }]));
          } catch (_error) {
            err = _error;
            return cb(err);
          }
        });
      };
    };
    return _.mapValues(_.clone(service.Client.prototype), function(fn, name) {
      return wrap_thrift_fn(name);
    });
  };

  _.extend(module.exports, {
    _private: {
      create_pool: create_pool,
      TIMEOUT_MESSAGE: TIMEOUT_MESSAGE,
      CLOSE_MESSAGE: CLOSE_MESSAGE
    }
  });

}).call(this);