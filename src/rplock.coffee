_ = require 'lodash'
promise = require 'bluebird'
ms = require 'ms'
redis = require 'redis'
EventEmitter = require 'events'
co = require 'co'
hat = require 'hat'
promise.promisifyAll(redis)

unlockScript = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end'

toMs = (val) ->
  if typeof val is 'string' then ms(val) else val

class Lock
  @config: {
    ttl: '10s'
    # timeout: '30s'
    pollingTimeout: '5s'
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

  _acquireMemory: (key, options, resolver) ->

    key = options.ns + ':' + key
    timeout = toMs(options.ttl)
    event = "release:#{key}"
    defer = promise.defer()
    releaseTimeout = null

    tryAcquire = ->
      unless Lock.localKeys[key]
        clear()
        Lock.localKeys[key] = true
        p = promise.resolve co resolver
        p.finally(release)
        p.then(defer.resolve.bind(defer), defer.reject.bind(defer))
      else
        unless releaseTimeout
          releaseTimeout = setTimeout(onTimeout, timeout)
          Lock.events.on(event, tryAcquire)

    clear = ->
      if releaseTimeout
        clearTimeout(releaseTimeout)
        releaseTimeout = null
        Lock.events.removeListener(event, tryAcquire)


    onTimeout = ->
      clear()
      defer.reject('Acquire timeout: ' + key)

    release = ->
      delete Lock.localKeys[key]
      Lock.events.emit(event)

    tryAcquire()

    defer.promise


  _acquireRedis: (key, options, resolver) ->
    key = options.ns + ':' + key
    ttl = toMs(options.ttl)
    pollingTimeout = toMs(options.pollingTimeout)
    attempts = options.attempts || Infinity
    _promise = null
    value = hat()

    onMessage = (pattern, channel, message) ->
      if channel is key and message is 'release'
        _promise?.resolve()

    subscribe = =>
      @subClient.on 'pmessage', onMessage

    unsubscribe = =>
      @subClient.removeListener 'pmessage', onMessage

    acquireLockAndResolve = =>
      @client.setAsync(key, value, 'PX', ttl, 'NX').then (acquired) ->
        unsubscribe()
        _promise = promise.defer()
        if attempts-- > 0
          unless acquired
            subscribe()
            promise.delay(pollingTimeout).then -> _promise.resolve()
            _promise.promise.then(acquireLockAndResolve)
          else
            promise.resolve co resolver
        else
          promise.reject('can\'t acquire lock')

    releaseLock = =>
      @client.publish(key, 'release')
      @client.evalAsync(unlockScript, 1, key, value)

    acquireLockAndResolve()
    .finally ->
      releaseLock()

  acquire: (key, [options]..., resolver) ->
    options ||= {}
    options = _.extend({}, @config, options)
    if options.memory
      @_acquireMemory(key, options, resolver)
    else
      @_acquireRedis(key, options, resolver)



module.exports = (config) ->
  new Lock(config)

# задаем конфигурацию глобально
module.exports.config = (config) ->
  _.extend(Lock.config, config)
