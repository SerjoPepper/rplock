_ = require 'lodash'
promise = require 'bluebird'
ms = require 'ms'
redis = require 'redis'
EventEmitter = require 'events'
promise.promisifyAll(redis)

toMs = (val) ->
  if typeof val is 'string' then ms(val) else val

class Lock
  @config: {
    ttl: '10s'
    timeout: '30s'
    pollingTimeout: '2s'
    attempts: Infinity
    ns: 'rplock'
  }

  @localKeys: {}

  @events: do ->
    events = new EventEmitter
    events.setMaxListeners(0)
    events

  constructor: (config = {}) ->
    @config = _.extend({}, Lock.config, config)
    @client = redis.createClient(@config.redis)
    @subClient = redis.createClient(@config.redis)
    if @config.redis.db
      @client.select(@config.redis.db)
      @client.subClient(@config.redis.db)
    @subClient.psubscribe(@config.ns + '*')

  _acquireLocal: (key, options, resolver) ->
    key = options.ns + ':' + key
    timeout = toMs(options.timeout)
    event = "release:#{key}"
    defer = promise.defer()
    releaseTimeout = null

    tryAcquire = ->
      unless Lock.localKeys[key]
        if releaseTimeout
          clearTimeout(releaseTimeout)
          releaseTimeout = null
        Lock.events.removeListener(event, tryAcquire)
        Lock.localKeys[key] = true
        p = if typeof resolver is 'function'
          promise.resolve co resolver
        else
          promise.resolve resolver
        p.then(defer.resolve.bind(defer), defer.reject.bind(defer)).finally(release)
      else
        unless releaseTimeout
          releaseTimeout = setTimeout(onTimeout, timeout)
          Lock.events.on(event, tryAcquire)

    onTimeout = ->
      Lock.events.removeListener(event, tryAcquire)
      defer.reject('Acquire timeout')

    release = ->
      Lock.localKeys[key] = false
      Lock.events.emit(event)

    tryAcquire()

    defer.promise


  _acquireRedis: (key, options, resolver) ->
    key = options.ns + ':' + key
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
            if typeof resolver is 'function'
              promise.resolve co resolver
            else
              promise.resolve resolver
        else
          promise.reject('can\'t acquire lock')

    releaseLock = =>
      @client.publish(key, 'release')
      @client.delAsync(key)

    acquireLockAndResolve().finally ->
      releaseLock()

  acquire: (key, [options]..., resolver) ->
    options ||= {}
    options = _.extend({}, @config, options)
    if options.local
      @_acquireLocal(key, options, resolver)
    else
      @_acquireRedis(key, options, resolver)



module.exports = (config) ->
  new Lock(config)

# задаем конфигурацию глобально
module.exports.config = (config) ->
  _.extend(Lock.config, config)
