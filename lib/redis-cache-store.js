// @ts-check
'use strict'

const { EventEmitter, setMaxListeners } = require('node:events')
const { createHash } = require('node:crypto')
const { Writable } = require('node:stream')
const { setTimeout: sleep } = require('node:timers/promises')
const { Redis, Cluster } = require('iovalkey')
const TrackingCache = require('./tracking-cache.js')

const INDEX_SCHEMA_PREFIX = 'cache:v2'
const NO_VARY_FIELD = 'no-vary'
const LOOKUP_FIELD_PREFIX = '$lookup:'
const NO_HEADERS_LOOKUP_FIELD = `${LOOKUP_FIELD_PREFIX}no-headers`
const DEFAULT_MISS_CACHE_TTL = 250
const DEFAULT_MISS_CACHE_MAX_COUNT = 10000
const FIND_CACHE_ENTRY_SCRIPT = `
local lookup_field = ARGV[3]
if lookup_field ~= '' then
  local lookup_ref_string = redis.call('HGET', KEYS[1], lookup_field)
  if lookup_ref_string then
    local ok, lookup_ref = pcall(cjson.decode, lookup_ref_string)
    if ok and type(lookup_ref) == 'table' and lookup_ref.schemaVersion == 2 and type(lookup_ref.valueKey) == 'string' then
      if tonumber(lookup_ref.deleteAt) and tonumber(lookup_ref.deleteAt) <= tonumber(ARGV[2]) then
        redis.call('HDEL', KEYS[1], lookup_field)
      else
        local lookup_value = redis.call('GET', lookup_ref.valueKey)
        if lookup_value then
          return { lookup_ref_string, lookup_value }
        end
        redis.call('HDEL', KEYS[1], lookup_field)
      end
    else
      redis.call('HDEL', KEYS[1], lookup_field)
    end
  end
end

local refs = redis.call('HGETALL', KEYS[1])
if #refs == 0 then
  return nil
end

local headers = cjson.decode(ARGV[1])
local now = tonumber(ARGV[2])
local best_ref
local best_ref_string
local best_field
local best_vary_count = -1
local stale_fields = {}

local function values_equal(actual, expected)
  if expected == cjson.null then
    return actual == nil
  end
  if actual == nil then
    return false
  end
  if type(actual) == 'table' or type(expected) == 'table' then
    return cjson.encode(actual) == cjson.encode(expected)
  end
  return actual == expected
end

for i = 1, #refs, 2 do
  local field = refs[i]
  local ref_string = refs[i + 1]
  if string.sub(field, 1, 8) ~= '$lookup:' then
    local ok, ref = pcall(cjson.decode, ref_string)

    if not ok or type(ref) ~= 'table' or ref.schemaVersion ~= 2 or type(ref.valueKey) ~= 'string' then
      stale_fields[#stale_fields + 1] = field
    elseif tonumber(ref.deleteAt) and tonumber(ref.deleteAt) <= now then
      stale_fields[#stale_fields + 1] = field
    else
      local matches = true
      local vary_count = 0

      if type(ref.vary) == 'table' then
        for header, expected in pairs(ref.vary) do
          vary_count = vary_count + 1
          if not values_equal(headers[header], expected) then
            matches = false
            break
          end
        end
      end

      if matches and vary_count > best_vary_count then
        best_ref = ref
        best_ref_string = ref_string
        best_field = field
        best_vary_count = vary_count
      end
    end
  end
end

if #stale_fields > 0 then
  redis.call('HDEL', KEYS[1], unpack(stale_fields))
end

if not best_ref then
  return nil
end

local value = redis.call('GET', best_ref.valueKey)
if not value then
  redis.call('HDEL', KEYS[1], best_field)
  return { best_ref_string, false }
end

return { best_ref_string, value }
`

/**
 * @typedef {{
 *  idKey: string
 *  valueKey: string
 *  tagsKey?: string
 *  vary?: Record<string, string | string[]> | string
 * }} RedisMetadataValue
 *
 * @typedef {{
 *  key: string
 *  idKey: string
 *  valueKey: string
 *  tagsKey?: string
 *  vary?: Record<string, string | string[]>
 *  indexKey?: string
 *  indexField?: string
 * }} ParsedRedisMetadataValue
 *
 * @typedef {{
 *  statusCode: number;
 *  statusMessage: string;
 *  headers: Record<string, string | string[]>;
 *  cachedAt: number;
 *  staleAt: number;
 *  deleteAt: number;
 *  body: string[]
 *  cacheControlDirectives: Record<string, string | string[]>;
 * }} RedisValue
 *
 * @typedef {{
 * redis: import('iovalkey').Redis;
 * trackingCache?: TrackingCache | undefined;
 * abortController: AbortController;
 * keyPrefix: string;
* }} Context
*
 * @typedef {import('./internal-types.d.ts').CacheStore} CacheStore
 * @implements {CacheStore}
 */
class RedisCacheStore extends EventEmitter {
  #maxEntrySize = Infinity

  /**
   * @type {((err: Error) => void)}
   */
  #errorCallback

  /**
   * @type {string | undefined}
   */
  #cacheTagsHeader

  /**
   * The prefix for each key in Redis. Redis usually handles this for us, but
   *  `keys` is an exception in both its input and output (we need to pass in
   *  the full key and we get the full keys back out)
   * @type {string}
   */
  #keyPrefix

  /**
   * @type {import('iovalkey').Redis}
   */
  #redis

  /**
   * @type {import('iovalkey').Redis | undefined}
   */
  #redisSubscribe

  /**
   * @type {TrackingCache | undefined}
   */
  #trackingCache

  /**
   * @type {boolean}
   */
  #closed = false

  /**
   * @type {boolean}
   */
  #ownsRedis = true

  /**
   * @type {boolean}
   */
  #isCluster = false

  /**
   * @type {boolean}
   */
  #valkey9Optimizations = true

  /**
   * @type {Promise<boolean> | undefined}
   */
  #hashFieldExpirationSupport

  /**
   * @type {Promise<boolean> | undefined}
   */
  #valkeyReadOptimizationSupport

  /**
   * @type {Promise<string> | undefined}
   */
  #findCacheEntryScriptSha

  /**
   * @type {Map<string, number>}
   */
  #missCache = new Map()

  /**
   * @type {number}
   */
  #missCacheTtl = DEFAULT_MISS_CACHE_TTL

  /**
   * @type {number}
   */
  #missCacheMaxCount = DEFAULT_MISS_CACHE_MAX_COUNT

  /**
    * @type {import('iovalkey').RedisOptions}
    */
  #redisClientOpts

  /**
   * @type {AbortController}
   */
  #abortController

  /**
   * @type {Context}
   */
  #context

  /**
   * @param {import('../index.d.ts').RedisCacheStoreOpts | undefined} opts
   */
  constructor (opts) {
    super()

    if (opts) {
      if (typeof opts !== 'object') {
        throw new TypeError('expected opts to be an object')
      }

      if (opts.maxEntrySize) {
        if (typeof opts.maxEntrySize !== 'number') {
          throw new TypeError('expected opts.maxEntrySize to be a number')
        }
        this.#maxEntrySize = opts.maxEntrySize
      }

      if (opts.errorCallback) {
        if (typeof opts.errorCallback !== 'function') {
          throw new TypeError('expected opts.errorCallback to be a function')
        }
        this.#errorCallback = opts.errorCallback
      }

      if (typeof opts.cacheTagsHeader === 'string') {
        this.#cacheTagsHeader = opts.cacheTagsHeader.toLowerCase()
      }

      if (typeof opts.enableValkey9Optimizations === 'boolean') {
        this.#valkey9Optimizations = opts.enableValkey9Optimizations
      }

      if (typeof opts.missCacheTtl === 'number') {
        if (opts.missCacheTtl < 0) {
          throw new TypeError('expected opts.missCacheTtl to be >= 0')
        }
        this.#missCacheTtl = opts.missCacheTtl
      }

      if (typeof opts.missCacheMaxCount === 'number') {
        if (opts.missCacheMaxCount < 0) {
          throw new TypeError('expected opts.missCacheMaxCount to be >= 0')
        }
        this.#missCacheMaxCount = opts.missCacheMaxCount
      }
    }

    if (!this.#errorCallback) {
      this.#errorCallback = (err) => {
        console.error('Unhandled error in RedisCacheStore:', err)
      }
    }

    const { keyPrefix: clientKeyPrefix, ...clientOpts } = opts?.clientOpts ?? {}

    this.#redisClientOpts = clientOpts ?? {}
    this.#keyPrefix = opts?.keyPrefix ?? clientKeyPrefix ?? ''

    if (opts?.client) {
      this.#redis = opts.client
      this.#ownsRedis = false
      this.#isCluster = this.#redis instanceof Cluster || opts.mode === 'cluster'
    } else if (opts?.mode === 'cluster' || (opts?.mode === 'auto' && opts.startupNodes && opts.startupNodes.length > 1)) {
      this.#redis = new Cluster(opts.startupNodes ?? [{ host: '127.0.0.1', port: 6379 }], {
        redisOptions: { enableAutoPipelining: true, ...clientOpts },
        ...opts.clusterOptions
      })
      this.#isCluster = true
    } else {
      this.#redis = new Redis({ enableAutoPipelining: true, ...clientOpts })
    }

    if (this.#isCluster && opts?.missCacheTtl === undefined) {
      this.#missCacheTtl = 0
    }

    if (opts?.tracking !== false && !this.#isCluster) {
      this.#trackingCache = new TrackingCache({
        maxSize: opts?.maxSize,
        maxCount: opts?.maxCount
      })
      this.#subscribe()
    }

    this.#abortController = new AbortController()
    setMaxListeners(100, this.#abortController.signal)

    this.#context = {
      redis: this.#redis,
      trackingCache: this.#trackingCache,
      abortController: this.#abortController,
      keyPrefix: this.#keyPrefix
    }
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {Promise<import('./internal-types.d.ts').GetResult | undefined>}
   */
  async get (key) {
    if (typeof key !== 'object') {
      throw new TypeError(`expected key to be object, got ${typeof key}`)
    }

    if (this.#trackingCache) {
      const result = this.#trackingCache.get(key)
      if (result !== undefined) return result
    }

    const cacheEntry = await this.findCacheByKey(key)
    if (cacheEntry === undefined) return undefined

    const { metadata, value } = cacheEntry

    if (this.#trackingCache) {
      const parsedMetadataKey = parseMetadataKey(metadata.key)
      this.#trackingCache.set(parsedMetadataKey, metadata, value)
    }

    return value
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {Promise<{
   *   metadata: ParsedRedisMetadataValue,
   *   value: import('./internal-types.d.ts').GetResult
   * } | undefined>}
   */
  async findCacheByKey (key) {
    /**
     * @type {ParsedRedisMetadataValue | undefined}
     */
    let metadataValue

    /**
     * @type {string | null}
     */
    let valueString
    let valueObject

    try {
      if (await this.#supportsValkeyReadOptimization()) {
        const cacheEntry = await this.#findCacheEntryByKey(key)
        if (!cacheEntry) return undefined

        metadataValue = cacheEntry.metadata
        valueString = cacheEntry.value
        valueObject = cacheEntry.valueObject
      } else {
        metadataValue = await this.#findMetadataValue(key)
        if (!metadataValue) {
          // Request isn't cached
          return undefined
        }

        valueString = await this.#redis.get(metadataValue.valueKey)
      }

      if (!valueString && !valueObject) {
        // The value expired but the metadata stayed around. This shouldn't ever
        //  happen but is _technically_ possible
        this.#redis.del(this.#keyPrefix + metadataValue.key).catch(err => {
          this.#errorCallback(err)
        })

        return undefined
      }
    } catch (err) {
      this.#errorCallback(err)
      return undefined
    }

    /**
     * @type {RedisValue}
     */
    let value

    try {
      value = valueObject ?? JSON.parse(valueString)
    } catch (err) {
      deleteByMetadataKey(this.#context, metadataValue.key)
        .catch(err => { this.#errorCallback(err) })

      this.#errorCallback(err)

      return undefined
    }

    const result = createGetResult(value, metadataValue.vary)

    return { metadata: metadataValue, value: result }
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @param {import('./internal-types.d.ts').CachedResponse} value
   * @returns {Writable}
   */
  createWriteStream (key, value) {
    if (typeof key !== 'object') {
      throw new TypeError(`expected key to be object, got ${typeof key}`)
    }

    if (typeof value !== 'object') {
      throw new TypeError(`expected value to be object, got ${typeof value}`)
    }

    let currentSize = 0
    /**
     * @type {string[] | undefined}
     */
    let body = key.method !== 'HEAD' ? [] : undefined
    const maxSize = this.#maxEntrySize
    const writeValueToRedis = this.#writeValueToRedis.bind(this)
    const errorCallback = this.#errorCallback

    const writable = new Writable({
      write (chunk, _, callback) {
        if (typeof chunk === 'object') {
          // chunk is a buffer, we need it to be a string
          chunk = chunk.toString('base64')
        }

        currentSize += chunk.length

        if (body) {
          if (currentSize >= maxSize) {
            body = undefined
            this.end()
            return callback()
          }

          body.push(chunk)
        }

        callback()
      },
      final (callback) {
        if (body) {
          writeValueToRedis(
            key,
            {
              statusCode: value.statusCode,
              statusMessage: value.statusMessage,
              cachedAt: value.cachedAt,
              staleAt: value.staleAt,
              deleteAt: value.deleteAt,
              headers: value.headers,
              cacheControlDirectives: value.cacheControlDirectives,
              body
            },
            value.vary
          ).then(() => {
            callback()
          }).catch(err => {
            errorCallback(err)
          })
        } else {
          callback()
        }
      }
    })

    return writable
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   */
  async delete (key) {
    try {
      const methodSetKey = serializeMethodSetKey({
        keyPrefix: this.#keyPrefix,
        origin: key.origin,
        path: key.path
      })
      const methods = await this.#redis.smembers(methodSetKey)

      if (methods.length === 0 && key.method) {
        methods.push(key.method)
      }

      await Promise.all(methods.map(method => this.#deleteByKey({ ...key, method })))
      await this.#redis.del(methodSetKey)
    } catch (err) {
      this.#errorCallback(err)
    }
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey[]} keys
   */
  async deleteKeys (keys) {
    const promises = []

    for (const key of keys) {
      promises.push(this.#deleteByKey(key))
    }

    try {
      await Promise.all(promises)
    } catch (err) {
      this.#errorCallback(err)
    }
  }

  /**
   * @param {Array<string | string[]>} tags
   * @returns {Promise<void>}
   */
  async deleteTags (tags) {
    try {
      const promises = new Array(tags.length)

      for (let i = 0; i < tags.length; i++) {
        let entryTags = tags[i]
        if (!Array.isArray(entryTags)) {
          entryTags = [entryTags]
        }
        promises[i] = deleteTags(this.#context, entryTags)
      }

      await Promise.all(promises)
    } catch (err) {
      this.#errorCallback(err)
    }
  }

  /**
   * @returns {Promise<void>}
   */
  async close () {
    if (this.#closed) return
    this.#closed = true
    this.#abortController.abort()

    // Wait for all scan streams to abort
    await sleep(100)

    try {
      const promises = []
      if (this.#ownsRedis) {
        promises.push(this.#redis.quit())
      }
      if (this.#redisSubscribe) {
        promises.push(this.#redisSubscribe.quit())
      }
      await Promise.all(promises)
    } catch (err) {
      this.#errorCallback(err)
    }
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   */
  async #deleteByKey (key) {
    const indexKey = serializeIndexKey({
      keyPrefix: this.#keyPrefix,
      origin: key.origin,
      path: key.path,
      method: key.method
    })
    const methodSetKey = serializeMethodSetKey({
      keyPrefix: this.#keyPrefix,
      origin: key.origin,
      path: key.path
    })

    const refs = await this.#redis.hgetall(indexKey)
    const promises = []

    for (const [indexField, refString] of Object.entries(refs)) {
      if (indexField.startsWith(LOOKUP_FIELD_PREFIX)) continue

      const ref = parseIndexReference(refString)
      if (ref) {
        promises.push(deleteByIndexReference(this.#context, ref))
      }
    }

    await Promise.all(promises)
    await this.#redis.del(indexKey)
    await this.#redis.srem(methodSetKey, key.method)
    if (await this.#redis.scard(methodSetKey) === 0) {
      await this.#redis.del(methodSetKey)
    }
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {Promise<ParsedRedisMetadataValue | undefined>}
   */
  async #findMetadataValue (key) {
    const matchingMetadata = await this.#findMatchingMetadataByKey(key)
    if (matchingMetadata.length === 0) return undefined
    if (matchingMetadata.length === 1) return matchingMetadata[0]

    // Looking for the matching metadata with the most specific vary header
    let bestMatch = matchingMetadata[0]
    let bestMatchVaryCounter = Object.keys(bestMatch.vary ?? {}).length

    for (let i = 1; i < matchingMetadata.length; i++) {
      const matchVary = matchingMetadata[i].vary ?? {}
      const matchVaryCounter = Object.keys(matchVary).length
      if (matchVaryCounter > bestMatchVaryCounter) {
        bestMatch = matchingMetadata[i]
        bestMatchVaryCounter = matchVaryCounter
      }
    }

    return bestMatch
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {Promise<ParsedRedisMetadataValue[]>}
   */
  async #findMatchingMetadataByKey (key) {
    const indexKey = serializeIndexKey({
      keyPrefix: this.#keyPrefix,
      origin: key.origin,
      path: key.path,
      method: key.method
    })

    const metadata = []
    const refs = await this.#redis.hgetall(indexKey)
    const cleanupPromises = []
    const now = Date.now()

    for (const [indexField, refString] of Object.entries(refs)) {
      if (indexField.startsWith(LOOKUP_FIELD_PREFIX)) continue

      const ref = parseIndexReference(refString)
      if (!ref) {
        cleanupPromises.push(this.#redis.hdel(indexKey, indexField))
        continue
      }

      if (ref.deleteAt <= now) {
        cleanupPromises.push(deleteByIndexReference(this.#context, ref))
        continue
      }

      if (!varyMatchesRequest(ref.vary, key.headers)) continue

      metadata.push({
        key: ref.metadataKey,
        idKey: ref.idKey,
        valueKey: ref.valueKey,
        tagsKey: ref.tagsKey,
        vary: ref.vary,
        indexKey: ref.indexKey,
        indexField: ref.indexField
      })
    }

    if (cleanupPromises.length > 0) {
      Promise.all(cleanupPromises).catch(err => this.#errorCallback(err))
    }

    return metadata
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {Promise<{ metadata: ParsedRedisMetadataValue, value: string | null, valueObject?: RedisValue } | undefined>}
   */
  async #findCacheEntryByKey (key) {
    const indexKey = serializeIndexKey({
      keyPrefix: this.#keyPrefix,
      origin: key.origin,
      path: key.path,
      method: key.method
    })

    const normalizedHeaders = normalizeHeaders(key.headers)
    const lookupField = getLookupField(normalizedHeaders)
    if (lookupField) {
      if (this.#hasMissCacheEntry(indexKey, lookupField)) return undefined

      const lookupValue = await this.#redis.hget(indexKey, lookupField)
      if (!lookupValue && lookupField === NO_HEADERS_LOOKUP_FIELD) {
        this.#setMissCacheEntry(indexKey, lookupField)
        return undefined
      }
      if (!lookupValue) {
        const cacheEntry = this.#isCluster
          ? await this.#findCacheEntryByIndex(indexKey, normalizedHeaders)
          : await this.#findCacheEntryByScript(indexKey, normalizedHeaders, lookupField)
        if (!cacheEntry) this.#setMissCacheEntry(indexKey, lookupField)
        return cacheEntry
      }

      const parsedLookupValue = JSON.parse(lookupValue)
      const metadata = createParsedMetadataFromIndexReference(parsedLookupValue.metadata)
      if (!varyMatchesNormalizedHeaders(metadata.vary, normalizedHeaders)) {
        const cacheEntry = this.#isCluster
          ? await this.#findCacheEntryByIndex(indexKey, normalizedHeaders)
          : await this.#findCacheEntryByScript(indexKey, normalizedHeaders, lookupField)
        if (!cacheEntry) this.#setMissCacheEntry(indexKey, lookupField)
        return cacheEntry
      }

      if (parsedLookupValue.value.deleteAt <= Date.now()) {
        await this.#redis.hdel(indexKey, lookupField)
        this.#setMissCacheEntry(indexKey, lookupField)
        return undefined
      }

      return {
        metadata,
        value: null,
        valueObject: parsedLookupValue.value
      }
    }

    const cacheEntry = this.#isCluster
      ? await this.#findCacheEntryByIndex(indexKey, normalizedHeaders)
      : await this.#findCacheEntryByScript(indexKey, normalizedHeaders, lookupField)
    if (!cacheEntry) this.#setMissCacheEntry(indexKey, lookupField)
    return cacheEntry
  }

  /**
   * @param {string} indexKey
   * @param {Record<string, string | string[]>} normalizedHeaders
   * @returns {Promise<{ metadata: ParsedRedisMetadataValue, value: string | null } | undefined>}
   */
  async #findCacheEntryByIndex (indexKey, normalizedHeaders) {
    const refs = await this.#redis.hgetall(indexKey)
    const cleanupPromises = []
    const now = Date.now()
    let bestRef
    let bestVaryCount = -1

    for (const [indexField, refString] of Object.entries(refs)) {
      if (indexField.startsWith(LOOKUP_FIELD_PREFIX)) continue

      const ref = parseIndexReference(refString)
      if (!ref) {
        cleanupPromises.push(this.#redis.hdel(indexKey, indexField))
        continue
      }

      if (ref.deleteAt <= now) {
        cleanupPromises.push(deleteByIndexReference(this.#context, ref))
        continue
      }

      if (!varyMatchesNormalizedHeaders(ref.vary, normalizedHeaders)) continue

      const varyCount = Object.keys(ref.vary ?? {}).length
      if (varyCount > bestVaryCount) {
        bestRef = ref
        bestVaryCount = varyCount
      }
    }

    if (cleanupPromises.length > 0) {
      Promise.all(cleanupPromises).catch(err => this.#errorCallback(err))
    }

    if (!bestRef) return undefined

    return {
      metadata: createParsedMetadataFromIndexReference(bestRef),
      value: await this.#redis.get(bestRef.valueKey)
    }
  }

  /**
   * @param {string} indexKey
   * @param {Record<string, string | string[]>} normalizedHeaders
   * @param {string} lookupField
   * @returns {Promise<{ metadata: ParsedRedisMetadataValue, value: string | null } | undefined>}
   */
  async #findCacheEntryByScript (indexKey, normalizedHeaders, lookupField) {
    const scriptArgs = [
      1,
      indexKey,
      JSON.stringify(normalizedHeaders),
      Date.now(),
      lookupField
    ]

    let result
    try {
      result = await this.#redis.call('EVALSHA', await this.#loadFindCacheEntryScript(), ...scriptArgs)
    } catch (err) {
      if (!isNoScriptError(err)) throw err

      this.#findCacheEntryScriptSha = undefined
      result = await this.#redis.call('EVALSHA', await this.#loadFindCacheEntryScript(), ...scriptArgs)
    }

    if (!Array.isArray(result) || result.length < 2) return undefined

    const ref = parseIndexReference(result[0])
    if (!ref) return undefined

    return {
      metadata: createParsedMetadataFromIndexReference(ref),
      value: result[1]
    }
  }

  /**
   * @returns {Promise<string>}
   */
  async #loadFindCacheEntryScript () {
    if (!this.#findCacheEntryScriptSha) {
      this.#findCacheEntryScriptSha = this.#redis.call('SCRIPT', 'LOAD', FIND_CACHE_ENTRY_SCRIPT)
    }

    return this.#findCacheEntryScriptSha
  }

  /**
   * @param {string} indexKey
   * @param {string} lookupField
   * @returns {boolean}
   */
  #hasMissCacheEntry (indexKey, lookupField) {
    if (this.#missCacheTtl === 0 || this.#missCacheMaxCount === 0) return false

    const cacheKey = serializeMissCacheKey(indexKey, lookupField)
    const expiresAt = this.#missCache.get(cacheKey)
    if (expiresAt === undefined) return false

    if (expiresAt <= Date.now()) {
      this.#missCache.delete(cacheKey)
      return false
    }

    return true
  }

  /**
   * @param {string} indexKey
   * @param {string} lookupField
   * @returns {void}
   */
  #setMissCacheEntry (indexKey, lookupField) {
    if (this.#missCacheTtl === 0 || this.#missCacheMaxCount === 0) return

    if (this.#missCache.size >= this.#missCacheMaxCount) {
      const oldestKey = this.#missCache.keys().next().value
      if (oldestKey !== undefined) this.#missCache.delete(oldestKey)
    }

    this.#missCache.set(serializeMissCacheKey(indexKey, lookupField), Date.now() + this.#missCacheTtl)
  }

  /**
   * @param {string} indexKey
   * @param {string} lookupField
   * @returns {void}
   */
  #deleteMissCacheEntry (indexKey, lookupField) {
    if (this.#missCache.size === 0) return
    this.#missCache.delete(serializeMissCacheKey(indexKey, lookupField))
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @param {RedisValue} value
   * @param {Record<string, string | string[]> | undefined} vary
   */
  async #writeValueToRedis (key, value, vary) {
    const normalizedVary = normalizeVary(vary)
    if (normalizedVary && Object.hasOwn(normalizedVary, '*')) {
      return
    }

    const indexKey = serializeIndexKey({
      keyPrefix: this.#keyPrefix,
      origin: key.origin,
      path: key.path,
      method: key.method
    })
    const methodSetKey = serializeMethodSetKey({
      keyPrefix: this.#keyPrefix,
      origin: key.origin,
      path: key.path
    })
    const indexField = serializeVaryIndexField(normalizedVary)
    const hashTag = serializeUrlMethodHash({
      origin: key.origin,
      path: key.path,
      method: key.method
    })
    const entryId = key.id ?? serializeEntryId({
      origin: key.origin,
      path: key.path,
      method: key.method,
      indexField
    })

    const idKey = serializeIdKey({ keyPrefix: this.#keyPrefix, id: entryId, hashTag })
    const valueKey = serializeValueKey({ keyPrefix: this.#keyPrefix, id: entryId, hashTag })
    const metadataKey = serializeMetadataKey({
      keyPrefix: this.#keyPrefix,
      origin: key.origin,
      path: key.path,
      method: key.method,
      id: entryId,
      hashTag
    })
    const previousIndexReference = parseIndexReference(
      await this.#redis.hget(indexKey, indexField)
    )

    /**
     * @type {RedisMetadataValue}
     */
    const metadata = { idKey, valueKey }
    if (normalizedVary) {
      metadata.vary = JSON.stringify(normalizedVary)
    }
    metadata.indexKey = indexKey
    metadata.indexField = indexField

    const expireAt = Math.floor(value.deleteAt / 1000)

    const tags = this.#parseCacheTags(value.headers ?? {})
    const tagSetKeys = []
    let tagMember
    const writeCommands = []
    if (tags.length > 0) {
      const tagsKey = serializeTagsKey({ keyPrefix: this.#keyPrefix, tags, id: entryId, hashTag })
      writeCommands.push(
        ['hmset', tagsKey, { metadataKey }],
        ['expireat', tagsKey, expireAt]
      )
      metadata.tagsKey = tagsKey
    }

    const indexReference = {
      schemaVersion: 2,
      keyPrefix: this.#keyPrefix,
      metadataKey,
      idKey,
      valueKey,
      tagsKey: metadata.tagsKey,
      indexKey,
      indexField,
      vary: normalizedVary,
      tags,
      deleteAt: value.deleteAt
    }

    if (tags.length > 0) {
      tagMember = JSON.stringify(indexReference)
      metadata.tagMember = tagMember
      for (const tag of tags) {
        const tagSetKey = serializeTagIndexKey({ keyPrefix: this.#keyPrefix, tag })
        const globalTagSetKey = serializeGlobalTagIndexKey({ tag })
        tagSetKeys.push(tagSetKey, globalTagSetKey)
        writeCommands.push(
          ['sadd', tagSetKey, tagMember],
          ['sadd', globalTagSetKey, tagMember]
        )
      }
      metadata.tagSetKeys = JSON.stringify(tagSetKeys)
    }

    if (this.#trackingCache) {
      this.#trackingCache.set(
        parseMetadataKey(metadataKey),
        { vary: normalizedVary },
        createGetResult(value, normalizedVary)
      )
    }

    writeCommands.push(
      ['hmset', metadataKey, metadata],
      ['hmset', idKey, { metadataKey }],
      ['set', valueKey, JSON.stringify(value)],
      ['sadd', methodSetKey, key.method],
      ['expireat', metadataKey, expireAt],
      ['expireat', idKey, expireAt],
      ['expireat', valueKey, expireAt]
    )

    const lookupField = getVaryLookupField(normalizedVary)
    this.#deleteMissCacheEntry(indexKey, lookupField)

    const indexReferenceString = JSON.stringify(indexReference)
    writeCommands.push(['hset', indexKey, indexField, indexReferenceString])

    if (lookupField) {
      writeCommands.push(
        ['hset', indexKey, lookupField, JSON.stringify({
          metadata: indexReference,
          value
        })]
      )
    }

    if (await this.#supportsHashFieldExpiration()) {
      writeCommands.push(['call', 'HEXPIREAT', indexKey, expireAt, 'FIELDS', 1, indexField])
      if (lookupField) {
        writeCommands.push(['call', 'HEXPIREAT', indexKey, expireAt, 'FIELDS', 1, lookupField])
      }
    }

    await this.#execWriteCommands(writeCommands)

    this.emit('write', {
      id: entryId,
      origin: key.origin,
      path: key.path,
      method: key.method,
      statusCode: value.statusCode,
      headers: value.headers,
      cacheTags: tags,
      cachedAt: value.cachedAt,
      staleAt: value.staleAt,
      deleteAt: value.deleteAt
    })

    if (
      previousIndexReference &&
      previousIndexReference.metadataKey !== indexReference.metadataKey
    ) {
      deleteByIndexReference(this.#context, previousIndexReference)
        .catch(err => this.#errorCallback(err))
    }
  }

  #subscribe () {
    this.#redisSubscribe = new Redis(this.#redisClientOpts)
    this.#redisSubscribe.call('CLIENT', 'ID')
      .then(clientId => {
        return this.#redis.call('CLIENT', 'TRACKING', 'on', 'REDIRECT', clientId)
      })
      .then(() => this.#redisSubscribe.subscribe('__redis__:invalidate'))
      .then(() => this.#redisSubscribe.subscribe('__valkey__:invalidate'))
      .catch(err => this.#errorCallback(err))

    this.#redisSubscribe.on('message', (channel, message) => {
      if (isTrackingInvalidationChannel(channel)) {
        if (
          message.startsWith('metadata:') ||
          message.startsWith(addKeyPrefix('metadata:', this.#keyPrefix))
        ) {
          const parsedMetadataKey = parseMetadataKey(message)
          if (this.#trackingCache) {
            this.#trackingCache.delete(parsedMetadataKey)
          }
          return
        }

        if (
          this.#trackingCache &&
          (
            message.startsWith('values:') ||
            message.startsWith(addKeyPrefix('values:', this.#keyPrefix)) ||
            message.startsWith(`${INDEX_SCHEMA_PREFIX}:`) ||
            message.startsWith(addKeyPrefix(`${INDEX_SCHEMA_PREFIX}:`, this.#keyPrefix))
          )
        ) {
          this.#trackingCache.clear()
          this.#missCache.clear()
        }
      }
    })
  }

  /**
   * @param {Record<string, string | string[]>} headers
   * @returns {string[]}
   */
  #parseCacheTags (headers) {
    if (!this.#cacheTagsHeader) return []

    for (const headerName of Object.keys(headers)) {
      if (headerName.toLowerCase() !== this.#cacheTagsHeader) {
        continue
      }

      const headerValue = headers[headerName]
      return Array.isArray(headerValue) ? headerValue : headerValue.split(',')
    }

    return []
  }

  /**
   * @returns {Promise<boolean>}
   */
  async #supportsHashFieldExpiration () {
    if (!this.#valkey9Optimizations) return false

    if (!this.#hashFieldExpirationSupport) {
      this.#hashFieldExpirationSupport = this.#redis
        .call('COMMAND', 'INFO', 'HEXPIREAT')
        .then((result) => Array.isArray(result) && result[0] !== null)
        .catch(() => false)
    }

    return this.#hashFieldExpirationSupport
  }

  /**
   * @returns {Promise<boolean>}
   */
  async #supportsValkeyReadOptimization () {
    if (!this.#valkey9Optimizations) return false

    if (!this.#valkeyReadOptimizationSupport) {
      this.#valkeyReadOptimizationSupport = this.#supportsHashFieldExpiration()
    }

    return this.#valkeyReadOptimizationSupport
  }

  /**
   * @param {Array<[string, ...any[]]>} commands
   * @returns {Promise<void>}
   */
  async #execWriteCommands (commands) {
    if (this.#isCluster) {
      await Promise.all(commands.map(([command, ...args]) => {
        if (command === 'call') return this.#redis.call(...args)
        return this.#redis[command](...args)
      }))
      return
    }

    const pipeline = this.#redis.pipeline()
    for (const [command, ...args] of commands) {
      if (command === 'call') {
        pipeline.call(...args)
      } else {
        pipeline[command](...args)
      }
    }
    await pipeline.exec()
  }

  /**
   * @param {string} key
   * @param {string} field
   * @param {number} expireAt
   * @returns {Promise<void>}
   */
  async #expireHashField (key, field, expireAt) {
    if (!await this.#supportsHashFieldExpiration()) return

    try {
      await this.#redis.call('HEXPIREAT', key, expireAt, 'FIELDS', 1, field)
    } catch (err) {
      this.#errorCallback(err)
    }
  }
}

class RedisCacheManager extends EventEmitter {
  /**
   * @type {import('iovalkey').Redis}
   */
  #redis

  /**
   * @type {import('iovalkey').Redis}
   */
  #redisSubscribe

  /**
   * @type {boolean}
   */
  #subscribed = false

  /**
   * @type {boolean}
   */
  #closed = false

  /**
    * @type {import('iovalkey').RedisOptions}
    */
  #redisClientOpts

  /**
   * @type {AbortController}
   */
  #abortController

  /**
   * @type {Context}
   */
  #context

  /**
   * @type {boolean}
   */
  #clientConfigKeyspaceEventNotify

  /**
   * @param {import('../index.d.ts').RedisCacheManagerOpts | undefined} opts
   */
  constructor (opts) {
    super()

    if (opts) {
      if (typeof opts !== 'object') {
        throw new TypeError('expected opts to be an object')
      }

      this.#redisClientOpts = opts.clientOpts ?? {}
    }

    if (typeof opts?.clientConfigKeyspaceEventNotify === 'boolean') {
      this.#clientConfigKeyspaceEventNotify = opts.clientConfigKeyspaceEventNotify
    } else {
      this.#clientConfigKeyspaceEventNotify = true
    }

    if (!this.#redisClientOpts) this.#redisClientOpts = {}

    this.#redis = new Redis({
      enableAutoPipelining: true,
      ...this.#redisClientOpts
    })

    this.#abortController = new AbortController()

    this.#context = {
      redis: this.#redis,
      abortController: this.#abortController,
      keyPrefix: ''
    }
  }

  /**
   * @param {(entry: import('../index.d.ts').CacheEntry) => Promise<unknown> | unknown} callback
   * @param {string} keyPrefix
   * @returns {Promise<void>}
   */
  async streamEntries (callback, keyPrefix = '') {
    const context = { ...this.#context, keyPrefix }

    await scanByPattern(context, `${keyPrefix}ids:*`, async (keys) => {
      const promises = new Array(keys.length)

      for (let i = 0; i < keys.length; i++) {
        const { keyPrefix } = parseIdKey(keys[i])
        promises[i] = this.#getEntryByIdKey(keys[i], keyPrefix)
          .then(entry => { if (entry !== undefined) { callback(entry) } })
      }

      await Promise.all(promises)
    })
  }

  async subscribe () {
    if (this.#subscribed) return
    this.#subscribed = true

    try {
      if (this.#clientConfigKeyspaceEventNotify) {
        await this.#redis.send_command('CONFIG', [
          'SET', 'notify-keyspace-events', 'AKE'
        ])
      }

      this.#redisSubscribe = new Redis(this.#redisClientOpts)

      await this.#redisSubscribe.subscribe(
        '__keyevent@0__:hset',
        '__keyevent@0__:del',
        '__keyevent@0__:expired'
      )
    } catch (err) {
      this.subscribed = false
      await this.#redisSubscribe.quit()

      throw err
    }

    this.#redisSubscribe.on('message', async (channel, key) => {
      try {
        if (key.includes('ids:')) {
          const { keyPrefix, id } = parseIdKey(key)

          // A new cache entry was added
          if (channel === '__keyevent@0__:hset') {
            const cacheEntry = await this.#getEntryByIdKey(key, keyPrefix)
            if (cacheEntry !== undefined) {
              this.emit('add-entry', cacheEntry)
            }
            return
          }

          // A cache entry was deleted
          if (
            channel === '__keyevent@0__:del' ||
            channel === '__keyevent@0__:expired'
          ) {
            this.emit('delete-entry', { id, keyPrefix })
          }
          return
        }

        if (key.includes('cache-tags:')) {
          const { tags } = parseTagsKey(key)

          // A cache entry was deleted by tag
          if (
            channel === '__keyevent@0__:del' ||
            channel === '__keyevent@0__:expired'
          ) {
            await deleteTags(this.#context, tags, { global: true })
          }
        }
      } catch (err) {
        this.emit('error', err)
      }
    })
  }

  /**
   * @param {string} id
   * @param {string} keyPrefix
   * @returns {Promise<string | null>}
   */
  async getResponseById (id, keyPrefix = '') {
    let valueKey = serializeValueKey({ keyPrefix, id })
    const { metadataKey } = await this.#redis.hgetall(serializeIdKey({ keyPrefix, id }))
    if (metadataKey) {
      const metadata = await this.#redis.hgetall(addKeyPrefix(metadataKey, keyPrefix))
      if (metadata.valueKey) valueKey = addKeyPrefix(metadata.valueKey, keyPrefix)
    }

    let value = await this.#redis.get(valueKey)
    if (!value && valueKey !== `${keyPrefix}values:${id}`) {
      value = await this.#redis.get(`${keyPrefix}values:${id}`)
    }
    if (!value) return null

    const parsedValue = JSON.parse(value)
    const base64Body = parsedValue.body.join('')

    return Buffer.from(base64Body, 'base64').toString('utf8')
  }

  /**
   * @param {string} id
   * @param {string} keyPrefix
   * @returns {Promise<import('../index.d.ts').CacheEntry[]>}
   */
  async getDependentEntries (id, keyPrefix = '') {
    const { metadataKey } = await this.#redis.hgetall(serializeIdKey({ keyPrefix, id }))
    if (!metadataKey) return []

    const { tagsKey } = await this.#redis.hgetall(
      addKeyPrefix(metadataKey, keyPrefix)
    )
    if (!tagsKey) return []

    const { tags } = parseTagsKey(tagsKey)
    if (tags.length === 0) return []

    const entries = []
    const pattern = `*cache-tags:*${tags.sort().join('*:*')}:*`

    const fullTagsKey = addKeyPrefix(tagsKey, keyPrefix)

    await scanByPattern(this.#context, pattern, async (keys) => {
      const promises = new Array(keys.length)
      for (let i = 0; i < keys.length; i++) {
        if (keys[i] === fullTagsKey) continue

        const { keyPrefix } = parseTagsKey(keys[i])
        promises[i] = this.#getEntryByTagsKey(keys[i], keyPrefix)
          .then((entry) => { if (entry !== undefined) entries.push(entry) })
      }
      await Promise.all(promises)
    })

    return entries
  }

  /**
   * @param {string[]} ids
   * @param {string} keyPrefix
   * @returns {Promise<void>}
   */
  async deleteIds (ids, keyPrefix = '') {
    const promises = []
    for (const id of ids) {
      promises.push(this.#deleteById(id, keyPrefix))
    }
    await Promise.all(promises)
  }

  /**
   * @returns {Promise<void>}
   */
  async close () {
    if (this.#closed) return
    this.#closed = true
    this.#abortController.abort()

    // Wait for scan operations abortions
    await sleep(100)

    const promises = [this.#redis.quit()]
    if (this.#subscribed) {
      promises.push(this.#redisSubscribe.quit())
    }
    await Promise.all(promises)
  }

  /**
   * @param {string} idKey
   * @param {string} keyPrefix
   * @returns {Promise<import('../index.d.ts').CacheEntry | undefined>}
   */
  async #getEntryByIdKey (idKey, keyPrefix = '') {
    const { metadataKey } = await this.#redis.hgetall(
      addKeyPrefix(idKey, keyPrefix)
    )
    if (!metadataKey) return

    return this.#getEntryByMetadataKey(metadataKey, keyPrefix)
  }

  /**
   * @param {string} tagsKey
   * @param {string} keyPrefix
   * @returns {Promise<import('../index.d.ts').CacheEntry | undefined>}
   */
  async #getEntryByTagsKey (tagsKey, keyPrefix = '') {
    const { metadataKey } = await this.#redis.hgetall(
      addKeyPrefix(tagsKey, keyPrefix)
    )
    if (!metadataKey) return

    return this.#getEntryByMetadataKey(metadataKey, keyPrefix)
  }

  /**
   * @param {string} metadataKey
   * @param {string} keyPrefix
   * @returns {Promise<import('../index.d.ts').CacheEntry | undefined>}
   */
  async #getEntryByMetadataKey (metadataKey, keyPrefix = '') {
    const { id } = parseMetadataKey(metadataKey)

    const { valueKey, tagsKey } = await this.#redis.hgetall(
      addKeyPrefix(metadataKey, keyPrefix)
    )
    if (!valueKey) return

    const value = await this.#redis.get(
      addKeyPrefix(valueKey, keyPrefix)
    )
    if (!value) return

    const parsedMetaKey = parseMetadataKey(metadataKey)
    const parsedValue = JSON.parse(value)

    let cacheTags = []
    if (tagsKey) {
      const { tags } = parseTagsKey(tagsKey)
      cacheTags = tags
    }

    return {
      id,
      keyPrefix,
      origin: parsedMetaKey.origin,
      path: parsedMetaKey.path,
      method: parsedMetaKey.method,
      statusCode: parsedValue.statusCode,
      headers: parsedValue.headers,
      cacheTags,
      cachedAt: parsedValue.cachedAt,
      staleAt: parsedValue.staleAt,
      deleteAt: parsedValue.deleteAt
    }
  }

  /**
   * @param {string} id
   * @param {string} keyPrefix
   * @returns {Promise<void>}
   */
  async #deleteById (id, keyPrefix = '') {
    const { metadataKey } = await this.#redis.hgetall(serializeIdKey({ keyPrefix, id }))
    if (!metadataKey) return

    await deleteByMetadataKey(this.#context, metadataKey)
  }
}

/**
  * @param {Context} ctx
  * @param {string} metadataKey
  * @returns {Promise<void>}
  */
async function deleteByMetadataKey (ctx, metadataKey, opts = {}) {
  const { redis, keyPrefix } = ctx

  const metadata = await redis.hgetall(addKeyPrefix(metadataKey, keyPrefix))
  if (!metadata.valueKey) {
    await redis.del(addKeyPrefix(metadataKey, keyPrefix))
    return
  }

  const { idKey, valueKey, tagsKey, indexKey, indexField, tagMember } = metadata

  const promises = [
    redis.del(addKeyPrefix(metadataKey, keyPrefix)),
    redis.del(addKeyPrefix(idKey, keyPrefix)),
    redis.del(addKeyPrefix(valueKey, keyPrefix))
  ]

  if (indexKey && indexField) {
    promises.push(redis.hdel(addKeyPrefix(indexKey, keyPrefix), indexField))
    const vary = parseMetadataVary(metadata.vary)
    const lookupField = getVaryLookupField(vary)
    if (lookupField) {
      promises.push(redis.hdel(addKeyPrefix(indexKey, keyPrefix), lookupField))
    }
  }

  if (metadata.tagSetKeys && tagMember) {
    try {
      const tagSetKeys = JSON.parse(metadata.tagSetKeys)
      for (const tagSetKey of tagSetKeys) {
        promises.push(redis.srem(addKeyPrefix(tagSetKey, keyPrefix), tagMember))
      }
    } catch {
      // Ignore malformed cleanup metadata. The direct entry keys are still
      // deleted and stale tag references are cleaned lazily on invalidation.
    }
  }

  if (ctx.trackingCache) {
    ctx.trackingCache.delete(parseMetadataKey(metadataKey))
  }

  if (tagsKey) {
    promises.push(redis.del(addKeyPrefix(tagsKey, keyPrefix)))
    if (opts.cascadeTags !== false) {
      const { tags } = parseTagsKey(tagsKey)
      promises.push(deleteTags(ctx, tags))
    }
  }

  await Promise.all(promises)

  if (indexKey) {
    const { origin, path, method } = parseMetadataKey(metadataKey)
    const indexLength = await redis.hlen(addKeyPrefix(indexKey, keyPrefix))
    if (indexLength === 0) {
      const methodSetKey = serializeMethodSetKey({ keyPrefix, origin, path })
      await redis.srem(methodSetKey, method)
      if (await redis.scard(methodSetKey) === 0) {
        await redis.del(methodSetKey)
      }
    }
  }
}

/**
  * @param {Context} ctx
  * @param {string[]} tags
  * @param {{ global?: boolean }} [opts]
  * @returns {Promise<void>}
  */
async function deleteTags (ctx, tags, opts = {}) {
  tags = tags.filter(tag => tag.length > 0)
  if (tags.length === 0) return

  if (opts.global) {
    const prefix = '*'
    const pattern = `${prefix}cache-tags:*${tags.sort().join('*:*')}:*`

    await scanByPattern(ctx, pattern, async (keys) => {
      const promises = new Array(keys.length)
      for (let i = 0; i < keys.length; i++) {
        const { keyPrefix } = parseTagsKey(keys[i])
        const context = { ...ctx, keyPrefix }
        promises[i] = deleteByTagKey(context, keys[i])
      }
      await Promise.all(promises)
    })
    return
  }

  const primaryTag = tags[0]
  const tagIndexKey = serializeGlobalTagIndexKey({ tag: primaryTag })
  const members = await ctx.redis.smembers(tagIndexKey)
  const promises = []

  for (const member of members) {
    const ref = parseIndexReference(member)
    if (!ref) {
      promises.push(ctx.redis.srem(tagIndexKey, member))
      continue
    }

    const hasAllTags = tags.every(tag => ref.tags.includes(tag))
    if (!hasAllTags) continue

    promises.push(deleteByIndexReference(
      { ...ctx, keyPrefix: ref.keyPrefix ?? ctx.keyPrefix },
      ref,
      { cascadeTags: false }
    ))
  }

  await Promise.all(promises)
}

/**
  * @param {Context} ctx
  * @param {string} tagKey
  * @returns {Promise<void>}
  */
async function deleteByTagKey (ctx, tagKey) {
  const { redis, keyPrefix } = ctx

  const metadata = await redis.hgetall(addKeyPrefix(tagKey, keyPrefix))
  if (!metadata.metadataKey) return

  await redis.del(addKeyPrefix(tagKey, keyPrefix))
  await deleteByMetadataKey(ctx, metadata.metadataKey)
}

/**
 * @param {Context} ctx
 * @param {Record<string, any>} ref
 * @returns {Promise<void>}
 */
async function deleteByIndexReference (ctx, ref, opts = {}) {
  const promises = []

  if (ref.indexKey && ref.indexField) {
    promises.push(ctx.redis.hdel(addKeyPrefix(ref.indexKey, ctx.keyPrefix), ref.indexField))
    const lookupField = getVaryLookupField(ref.vary)
    if (lookupField) {
      promises.push(ctx.redis.hdel(addKeyPrefix(ref.indexKey, ctx.keyPrefix), lookupField))
    }
  }

  if (Array.isArray(ref.tags)) {
    for (const tag of ref.tags) {
      promises.push(ctx.redis.srem(
        serializeTagIndexKey({ keyPrefix: ctx.keyPrefix, tag }),
        JSON.stringify(ref)
      ))
    }
  }

  await Promise.all(promises)

  if (ref.metadataKey) {
    await deleteByMetadataKey(ctx, ref.metadataKey, opts)
  }
}

/**
 * @param {Context} ctx
 * @param {string} pattern
 * @param {(keys: string[]) => Promise<void>} callback
 * @returns {Promise<void>}
 */
async function scanByPattern (ctx, pattern, callback) {
  const { redis, keyPrefix, abortController } = ctx

  /**
   * @type {Promise<void|Error>[]}
   */
  const promises = []
  let cursor = '0'

  try {
    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', addKeyPrefix(pattern, keyPrefix), 'COUNT', '1000')
      if (keys.length > 0) promises.push(callback(keys).catch(err => err))
      cursor = nextCursor
    } while (cursor !== '0' && !abortController.signal.aborted)
  } finally {
    await Promise.allSettled(promises).then((results) => {
      const errors = results.filter(({ value }) => value instanceof Error)
      if (errors.length > 0) {
        throw new Error('Error(s) occurred during scanByPattern operation', { cause: errors })
      }
    })
  }
}

/**
 * @param {string} key
 * @param {string | undefined} prefix
 * @returns {string}
 */
function addKeyPrefix (key, prefix) {
  return prefix && !key.startsWith(prefix) ? prefix + key : key
}

/**
 * @param {string} channel
 * @returns {boolean}
 */
function isTrackingInvalidationChannel (channel) {
  return channel === '__redis__:invalidate' || channel === '__valkey__:invalidate'
}

/**
 * @param {{
 *   keyPrefix: string,
 *   origin: string,
 *   path: string,
 *   method: string,
 *   id: string,
 *   hashTag?: string
 * }} parsedKey
 * @returns {string}
 */
function serializeMetadataKey (parsedKey) {
  const { keyPrefix, origin, path, method, id, hashTag } = parsedKey

  const encodedOrigin = encodeURIComponent(origin)
  const encodedPath = encodeURIComponent(path)
  return `${keyPrefix}metadata:${formatHashTag(hashTag)}${encodedOrigin}:${encodedPath}:${method}:${id}`
}

/**
 * @param {{
 *   keyPrefix: string,
 *   origin: string,
 *   path: string,
 *   method: string
 * }} parsedKey
 * @returns {string}
 */
function serializeIndexKey (parsedKey) {
  const { keyPrefix, origin, path, method } = parsedKey
  const urlMethodHash = serializeUrlMethodHash({ origin, path, method })
  return `${keyPrefix}${INDEX_SCHEMA_PREFIX}:{${urlMethodHash}}:index`
}

/**
 * @param {{
 *   keyPrefix: string,
 *   origin: string,
 *   path: string
 * }} parsedKey
 * @returns {string}
 */
function serializeMethodSetKey (parsedKey) {
  const { keyPrefix, origin, path } = parsedKey
  const urlHash = hash(`${origin}\n${path}`)
  return `${keyPrefix}${INDEX_SCHEMA_PREFIX}:{${urlHash}}:methods`
}

/**
 * @param {{ origin: string, path: string, method: string }} parsedKey
 * @returns {string}
 */
function serializeUrlMethodHash (parsedKey) {
  const { origin, path, method } = parsedKey
  return hash(`${origin}\n${path}\n${method}`)
}

/**
 * @param {{
 *   origin: string,
 *   path: string,
 *   method: string,
 *   indexField: string
 * }} parsedKey
 * @returns {string}
 */
function serializeEntryId (parsedKey) {
  const { origin, path, method, indexField } = parsedKey
  return `${serializeUrlMethodHash({ origin, path, method })}-${indexField}`
}

/**
 * @param {string} key
 * @returns {{
 *   keyPrefix: string,
 *   origin: string,
 *   path: string,
 *   method: string,
 *   id: string
 * }}
 */
function parseMetadataKey (key) {
  const typePrefix = 'metadata:'
  const splitIndex = key.indexOf(typePrefix)

  if (splitIndex === -1) {
    throw new Error(`Invalid cache metadata key: "${key}"`)
  }

  const keyPrefix = key.slice(0, splitIndex)
  key = key.slice(splitIndex + typePrefix.length)

  const parts = key.split(':')
  if (isHashTagSegment(parts[0])) parts.shift()

  const origin = decodeURIComponent(parts[0])
  const path = decodeURIComponent(parts[1])
  const method = parts[2]
  const id = parts.slice(3).join(':')

  return { keyPrefix, origin, path, method, id }
}

/**
 * @param {{ keyPrefix: string, id: string, hashTag?: string }} parsedKey
 * @returns {string}
 */
function serializeIdKey (parsedKey) {
  const { keyPrefix, id, hashTag = getHashTagFromEntryId(id) } = parsedKey
  return `${keyPrefix}ids:${formatHashTag(hashTag)}${id}`
}

/**
  * @param {string} key
  * @returns {{ keyPrefix: string, id: string }}
  */
function parseIdKey (key) {
  const typePrefix = 'ids:'
  const splitIndex = key.indexOf(typePrefix)

  if (splitIndex === -1) {
    throw new Error(`Invalid cache id key: "${key}"`)
  }

  const keyPrefix = key.slice(0, splitIndex)
  let id = key.slice(splitIndex + typePrefix.length)
  if (id.startsWith('{')) {
    const hashTagEnd = id.indexOf('}:')
    if (hashTagEnd !== -1) {
      id = id.slice(hashTagEnd + 2)
    }
  }

  return { keyPrefix, id }
}

/**
 * @param {{ keyPrefix: string, id: string, hashTag?: string }} parsedKey
 * @returns {string}
 */
function serializeValueKey (parsedKey) {
  const { keyPrefix, id, hashTag = getHashTagFromEntryId(id) } = parsedKey
  return `${keyPrefix}values:${formatHashTag(hashTag)}${id}`
}

/**
 * @param {{ keyPrefix: string, tags: string[], id: string, hashTag?: string }} parsedKey
 * @returns {string}
 */
function serializeTagsKey (parsedKey) {
  const { keyPrefix, tags, id, hashTag = getHashTagFromEntryId(id) } = parsedKey
  return `${keyPrefix}cache-tags:${formatHashTag(hashTag)}${tags.sort().join(':')}:${id}`
}

/**
 * @param {string | undefined} hashTag
 * @returns {string}
 */
function formatHashTag (hashTag) {
  return hashTag ? `{${hashTag}}:` : ''
}

/**
 * @param {string} segment
 * @returns {boolean}
 */
function isHashTagSegment (segment) {
  return segment.startsWith('{') && segment.endsWith('}')
}

/**
 * @param {string} id
 * @returns {string | undefined}
 */
function getHashTagFromEntryId (id) {
  const [hashTag] = id.split('-', 1)
  return /^[a-f0-9]{32}$/.test(hashTag) ? hashTag : undefined
}

/**
 * @param {{ keyPrefix: string, tag: string }} parsedKey
 * @returns {string}
 */
function serializeTagIndexKey (parsedKey) {
  const { keyPrefix, tag } = parsedKey
  return `${keyPrefix}${INDEX_SCHEMA_PREFIX}:tag:{${hash(tag)}}`
}

/**
 * @param {{ tag: string }} parsedKey
 * @returns {string}
 */
function serializeGlobalTagIndexKey (parsedKey) {
  const { tag } = parsedKey
  return `${INDEX_SCHEMA_PREFIX}:global-tag:{${hash(tag)}}`
}

/**
 * @param {string} key
 * @returns {{ keyPrefix: string, tags: string[], id: string }}
 */
function parseTagsKey (key) {
  const typePrefix = 'cache-tags:'
  const splitIndex = key.indexOf(typePrefix)

  if (splitIndex === -1) {
    throw new Error(`Invalid cache tags key: "${key}"`)
  }

  const keyPrefix = key.slice(0, splitIndex)
  key = key.slice(splitIndex + typePrefix.length)

  const parts = key.split(':')
  if (isHashTagSegment(parts[0])) parts.shift()
  const tags = parts.slice(0, -1)
  const id = parts[parts.length - 1]

  return { keyPrefix, tags, id }
}

/**
 * @param {string[]} strings
 * @returns {Buffer[]}
 */
function parseBufferArray (strings) {
  const output = new Array(strings.length)

  for (let i = 0; i < strings.length; i++) {
    output[i] = Buffer.from(strings[i], 'base64')
  }

  return output
}

/**
 * @param {RedisValue} value
 * @param {Record<string, string | string[] | null> | undefined} vary
 * @returns {import('./internal-types.d.ts').GetResult}
 */
function createGetResult (value, vary) {
  const result = {
    statusCode: value.statusCode,
    statusMessage: value.statusMessage,
    cachedAt: value.cachedAt,
    staleAt: value.staleAt,
    deleteAt: value.deleteAt,
    headers: value.headers,
    body: parseBufferArray(value.body)
  }

  if (value.cacheControlDirectives) {
    result.cacheControlDirectives = value.cacheControlDirectives
  }

  if (value.headers?.etag) {
    result.etag = value.headers.etag
  }

  if (vary) {
    result.vary = vary
  }

  return result
}

/**
 * @param {string | null | undefined} refString
 * @returns {Record<string, any> | undefined}
 */
function parseIndexReference (refString) {
  if (!refString) return

  try {
    const ref = JSON.parse(refString)
    if (
      ref &&
      ref.schemaVersion === 2 &&
      typeof ref.metadataKey === 'string' &&
      typeof ref.idKey === 'string' &&
      typeof ref.valueKey === 'string' &&
      typeof ref.indexKey === 'string' &&
      typeof ref.indexField === 'string'
    ) {
      ref.tags = Array.isArray(ref.tags) ? ref.tags : []
      return ref
    }
  } catch {}
}

/**
 * @param {Record<string, any>} ref
 * @returns {ParsedRedisMetadataValue}
 */
function createParsedMetadataFromIndexReference (ref) {
  return {
    key: ref.metadataKey,
    idKey: ref.idKey,
    valueKey: ref.valueKey,
    tagsKey: ref.tagsKey,
    vary: ref.vary,
    indexKey: ref.indexKey,
    indexField: ref.indexField
  }
}

/**
 * @param {string | undefined}
 * @returns {Record<string, string | string[] | null> | undefined}
 */
function parseMetadataVary (vary) {
  if (!vary) return

  try {
    return JSON.parse(vary)
  } catch {}
}

/**
 * @param {unknown} err
 * @returns {boolean}
 */
function isNoScriptError (err) {
  return err instanceof Error && /NOSCRIPT/i.test(err.message)
}

/**
 * @param {Record<string, string | string[]> | undefined} vary
 * @returns {Record<string, string | string[] | null> | undefined}
 */
function normalizeVary (vary) {
  if (!vary) return

  const normalized = {}
  for (const [name, value] of Object.entries(vary)) {
    const normalizedName = name.toLowerCase()
    if (normalizedName === '*') {
      normalized['*'] = ''
      continue
    }

    normalized[normalizedName] = value === undefined ? null : value
  }

  if (Object.keys(normalized).length === 0) return
  return Object.fromEntries(Object.entries(normalized).sort(([a], [b]) => a.localeCompare(b)))
}

/**
 * @param {Record<string, string | string[] | null> | undefined} vary
 * @returns {string}
 */
function serializeVaryIndexField (vary) {
  if (!vary) return NO_VARY_FIELD
  return hash(JSON.stringify(vary))
}

/**
 * @param {Record<string, string | string[]>} headers
 * @returns {string}
 */
function getLookupField (headers) {
  const entries = Object.entries(headers)
  if (entries.length === 0) return NO_HEADERS_LOOKUP_FIELD
  entries.sort(([a], [b]) => a.localeCompare(b))
  return `${LOOKUP_FIELD_PREFIX}${hash(JSON.stringify(Object.fromEntries(entries)))}`
}

/**
 * @param {Record<string, string | string[] | null> | undefined} vary
 * @returns {string}
 */
function getVaryLookupField (vary) {
  if (!vary) return NO_HEADERS_LOOKUP_FIELD

  const lookupHeaders = {}
  for (const [header, value] of Object.entries(vary)) {
    if (value !== null) {
      lookupHeaders[header] = value
    }
  }

  return getLookupField(lookupHeaders)
}

/**
 * @param {string} indexKey
 * @param {string} lookupField
 * @returns {string}
 */
function serializeMissCacheKey (indexKey, lookupField) {
  return `${indexKey}\n${lookupField}`
}

/**
 * @param {Record<string, string | string[] | null> | undefined} vary
 * @param {Record<string, string | string[]> | undefined} requestHeaders
 * @returns {boolean}
 */
function varyMatchesRequest (vary, requestHeaders) {
  if (!vary) return true

  return varyMatchesNormalizedHeaders(vary, normalizeHeaders(requestHeaders))
}

/**
 * @param {Record<string, string | string[] | null> | undefined} vary
 * @param {Record<string, string | string[]>} headers
 * @returns {boolean}
 */
function varyMatchesNormalizedHeaders (vary, headers) {
  if (!vary) return true

  return Object.entries(vary).every(([header, value]) => {
    if (headers[header] === undefined && value === null) return true
    return headerValueEquals(headers[header], value)
  })
}

/**
 * @param {Record<string, string | string[]> | undefined} headers
 * @returns {Record<string, string | string[]>}
 */
function normalizeHeaders (headers) {
  const normalized = {}
  if (!headers) return normalized

  for (const [name, value] of Object.entries(headers)) {
    normalized[name.toLowerCase()] = value
  }

  return normalized
}

/**
 * @param {string | string[] | undefined} actual
 * @param {string | string[] | null} expected
 * @returns {boolean}
 */
function headerValueEquals (actual, expected) {
  if (actual === undefined || expected === null) return actual === undefined && expected === null
  if (Array.isArray(actual) || Array.isArray(expected)) {
    return JSON.stringify(actual) === JSON.stringify(expected)
  }
  return actual === expected
}

/**
 * @param {string} value
 * @returns {string}
 */
function hash (value) {
  return createHash('sha256').update(value).digest('hex').slice(0, 32)
}

module.exports = { RedisCacheStore, RedisCacheManager }

// exported for unittests only.
module.exports._scanByPattern = scanByPattern
