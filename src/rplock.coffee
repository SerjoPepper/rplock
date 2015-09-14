_ = require 'lodash'
promise = require 'bluebird'
ms = require 'ms'
redis = require 'redis'

promise.promisifyAll(redis)

toMs = (val) ->
  if typeof val is 'string' then ms(val) else val

class Lock
  @config: {
    ttl: '10s'
    pollingTimeout: '1000ms'
    attempts: Infinity
    ns: 'rplock'
  }

  constructor: (config = {}) ->
    @config = _.extend({}, Lock.config, config)
    @client = redis.createClient(@config.redis)

  acquire: (key, [options]..., resolver) ->
    options ||= {}
    options = _.extend({}, @config, options)
    key = options.ns + ':lock:' + key
    ttl = toMs(options.ttl)
    pollingTimeout = toMs(options.pollingTimeout)
    attempts = options.attempts || Infinity
    _promise = null

    onMessage = (channel, message) =>
      if channel is key and message is 'release'
        _promise?.resolve()

    subscribe = =>
      @client.subscribe(key)
      @client.on 'message', onMessage

    unsubscribe = =>
      @client.unsubscribe(key)
      @client.off 'message', onMessage


    acquireLockAndResolve = =>
      unsubscribe()
      @client.setAsync(key, String(new Date), 'PX', ttl, 'NX').then (acquired) =>
        _promise = promise.defer()
        if attempts-- > 0
          unless acquired
            subscribe()
            promise.delay(ttl).then(-> _promise.resolve())
            p.promise.then(acquireLockAndResolve)
          else
            # 1. unsubscribe
            promise[typeof resolver is 'function' and 'try' or 'resolve'](resolver)
        else
          promise.reject('can\'t acquire lock')

    releaseLock = =>
      @client.delAsync(key)

    acquireLockAndResolve().then (result) =>
      releaseLock().then =>
        @client.publish(key, 'release')
        result



module.exports = (config) ->
  new Lock(config)

# задаем конфигурацию глобально
module.exports.config = (config) ->
  _.extend(Lock.config, config)
