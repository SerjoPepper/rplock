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
    pollingTimeout: '2s'
    attempts: Infinity
    ns: 'rplock'
  }

  constructor: (config = {}) ->
    @config = _.extend({}, Lock.config, config)
    @client = redis.createClient(@config.redis)
    @subClient = redis.createClient(@config.redis)
    @subClient.psubscribe(@config.ns + '*')

  acquire: (key, [options]..., resolver) ->
    options ||= {}
    options = _.extend({}, @config, options)
    key = @config.ns + ':' + key
    ttl = toMs(options.ttl)
    pollingTimeout = toMs(options.pollingTimeout)
    attempts = options.attempts || Infinity
    _promise = null

    onMessage = (pattern, channel, message) ->
      if channel is key and message is 'release'
        _promise?.resolve()

    subscribe = =>
      @subClient.on 'pmessage', onMessage

    unsubscribe = =>
      @subClient.removeListener 'pmessage', onMessage

    acquireLockAndResolve = =>
      @client.setAsync(key, String(new Date), 'PX', ttl, 'NX').then (acquired) ->
        unsubscribe()
        _promise = promise.defer()
        if attempts-- > 0
          unless acquired
            subscribe()
            promise.delay(pollingTimeout).then -> _promise.resolve()
            _promise.promise.then(acquireLockAndResolve)
          else
            # 1. unsubscribe
            promise[typeof resolver is 'function' and 'try' or 'resolve'](resolver)
        else
          promise.reject('can\'t acquire lock')

    releaseLock = =>
      @client.publish(key, 'release')
      @client.delAsync(key)

    acquireLockAndResolve().finally ->
      releaseLock()


module.exports = (config) ->
  new Lock(config)

# задаем конфигурацию глобально
module.exports.config = (config) ->
  _.extend(Lock.config, config)
