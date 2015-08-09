_ = require 'lodash'
promise = require 'bluebird'
ms = require 'ms'
redis = require 'redis'

toMs = (val) ->
  if typeof val is 'string' then ms(val) / 1e3 else val

class Lock
  @config: {
    ttl: '10s'
    pollingTimeout: '50ms'
    attempts: 10
    ns: 'lock'
  }

  constructor: (config = {}) ->
    @config = _.extend({}, config, Lock.config)
    @client = redis.createClient(@config.redis)

  acquire: (key, [options]..., resolver) ->
    options ||= {}
    options = _.extend({}, @config, options)
    key = options.ns + ':' + key
    ttl = toMs(options.ttl)
    pollingTimeout = toMs(options.pollingTimeout)
    attempts = options.attempts || Infinity

    acquireLockAndResolve = =>
      @client.setAsync(key, String(new Date), 'PX', ttl, 'NX').then (acquired) ->
        if attempts-- > 0
          unless acquired
            promise.delay(pollingTimeout).then(acquireLockAndResolve)
          else
            promise[typeof resolver is 'function' and 'try' or 'resolve'](resolver)
        else
          promise.reject('can\'t acquire lock')

    releaseLock = =>
      @client.delAsync(key)

    acquireLockAndResolve().then (result) ->
      releaseLock().then -> result


module.exports = (config) ->
  new Lock(config)

# задаем конфигурацию глобально
module.exports.config = (config) ->
  _.extend(Lock.config, config)
