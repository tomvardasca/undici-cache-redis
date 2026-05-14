// @ts-check
'use strict'

const { describe, test } = require('node:test')
const { strictEqual, deepStrictEqual, notEqual, equal, fail, ok } = require('node:assert')
const { Readable } = require('node:stream')
const { once } = require('node:events')
const { Redis } = require('iovalkey')
const { RedisCacheStore } = require('../lib/redis-cache-store')
const { getAllKeys, cleanValkey } = require('./helper.js')
const { setTimeout: sleep } = require('node:timers/promises')

cacheStoreTests(RedisCacheStore)

function cacheStoreTests (CacheStore) {
  describe(CacheStore.prototype.constructor.name, () => {
    test('matches interface', async (t) => {
      const store = new CacheStore()

      t.after(async () => {
        await store.close()
      })

      equal(typeof store.get, 'function')
      equal(typeof store.createWriteStream, 'function')
      equal(typeof store.delete, 'function')
    })

    // Checks that it can store & fetch different responses
    test('basic functionality', async (t) => {
      await cleanValkey()

      const request = {
        origin: 'localhost',
        path: '/',
        method: 'GET',
        headers: {}
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: { foo: 'bar' },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }
      const requestBody = ['asd', '123']

      /**
       * @type {import('../lib/internal-types.d.ts').CacheStore}
       */
      const store = new CacheStore({
        clientOpts: {
          keyPrefix: `${crypto.randomUUID()}:`
        },
        errorCallback: (err) => {
          fail(err)
        }
      })

      t.after(async () => {
        await store.close()
      })

      // Sanity check
      equal(await store.get(request), undefined)

      // Write the response to the store
      let writeStream = store.createWriteStream(request, requestValue)
      notEqual(writeStream, undefined)
      writeResponse(writeStream, requestBody)

      await once(writeStream, 'close')

      // Now try fetching it with a deep copy of the original request
      let readStream = await store.get(structuredClone(request))
      notEqual(readStream, undefined)

      deepStrictEqual(await readResponse(readStream), {
        ...requestValue,
        body: requestBody
      })

      // Now let's write another request to the store
      const anotherRequest = {
        origin: 'localhost',
        path: '/asd',
        method: 'GET',
        headers: {}
      }
      const anotherValue = {
        statusCode: 200,
        statusMessage: '',
        headers: { foo: 'bar' },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }
      const anotherBody = ['asd2', '1234']

      // We haven't cached this one yet, make sure it doesn't confuse it with
      //  another request
      equal(await store.get(anotherRequest), undefined)

      // Now let's cache it
      writeStream = store.createWriteStream(anotherRequest, {
        ...anotherValue,
        body: []
      })
      notEqual(writeStream, undefined)
      writeResponse(writeStream, anotherBody)

      await once(writeStream, 'close')

      readStream = await store.get(anotherRequest)
      notEqual(readStream, undefined)
      deepStrictEqual(await readResponse(readStream), {
        ...anotherValue,
        body: anotherBody,
      })
    })

    test('returns stale response if possible', async (t) => {
      await cleanValkey()

      const request = {
        origin: 'localhost',
        path: '/',
        method: 'GET',
        headers: {}
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: { foo: 'bar' },
        cachedAt: Date.now() - 10000,
        staleAt: Date.now() - 1,
        deleteAt: Date.now() + 20000
      }
      const requestBody = ['part1', 'part2']

      /**
       * @type {import('../lib/internal-types.d.ts').CacheStore}
       */
      const store = new CacheStore({
        clientOpts: {
          keyPrefix: `${crypto.randomUUID()}:`
        },
        errorCallback: (err) => {
          fail(err)
        }
      })

      t.after(async () => {
        await store.close()
      })

      const writeStream = store.createWriteStream(request, requestValue)
      notEqual(writeStream, undefined)
      writeResponse(writeStream, requestBody)

      await once(writeStream, 'close')

      const readStream = await store.get(request)
      notEqual(readStream, undefined)
      deepStrictEqual(await readResponse(readStream), {
        ...requestValue,
        body: requestBody,
      })
    })

    test('a stale request is overwritten', async (t) => {
      /**
       * @type {import('../../types/cache-interceptor.d.ts').default.CacheKey}
       */
      const key = {
        origin: 'localhost',
        path: '/',
        method: 'GET',
        headers: {}
      }

      /**
       * @type {import('../../types/cache-interceptor.d.ts').default.CacheValue}
       */
      const value = {
        statusCode: 200,
        statusMessage: '',
        headers: { foo: 'bar' },
        cacheControlDirectives: {},
        cachedAt: Date.now(),
        staleAt: Date.now() + 1000,
        // deleteAt is different because stale-while-revalidate, stale-if-error, ...
        deleteAt: Date.now() + 5000
      }

      const body = [Buffer.from('asd'), Buffer.from('123')]

      const store = new CacheStore()

      t.after(async () => {
        await store.close()
      })

      // Sanity check
      equal(await store.get(key), undefined)

      {
        const writable = store.createWriteStream(key, value)
        notEqual(writable, undefined)
        writeResponse(writable, body)
      }

      await sleep(1500)

      {
        const result = await store.get(structuredClone(key))
        notEqual(result, undefined)
        deepStrictEqual(result, {
          ...value,
          body
        })
      }

      /**
       * @type {import('../../types/cache-interceptor.d.ts').default.CacheValue}
       */
      const value2 = {
        statusCode: 200,
        statusMessage: '',
        headers: { foo: 'baz' },
        cacheControlDirectives: {},
        cachedAt: Date.now(),
        staleAt: Date.now() + 1000,
        // deleteAt is different because stale-while-revalidate, stale-if-error, ...
        deleteAt: Date.now() + 5000
      }

      const body2 = [Buffer.from('foo'), Buffer.from('123')]

      {
        const writable = store.createWriteStream(key, value2)
        notEqual(writable, undefined)
        writeResponse(writable, body2)
      }

      {
        const result = await store.get(structuredClone(key))
        notEqual(result, undefined)
        deepStrictEqual(result, {
          ...value,
          body
        })
      }
    })

    test('doesn\'t return response past deletedAt', async (t) => {
      await cleanValkey()

      const request = {
        origin: 'localhost',
        path: '/',
        method: 'GET',
        headers: {}
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        cachedAt: Date.now() - 20000,
        staleAt: Date.now() - 10000,
        deleteAt: Date.now() - 5000
      }
      const requestBody = ['part1', 'part2']

      /**
       * @type {import('../lib/internal-types.d.ts').CacheStore}
       */
      const store = new CacheStore({
        clientOpts: {
          keyPrefix: `${crypto.randomUUID()}:`
        },
        errorCallback: (err) => {
          fail(err)
        }
      })

      t.after(async () => {
        await store.close()
      })

      const writeStream = store.createWriteStream(request, requestValue)
      notEqual(writeStream, undefined)
      writeResponse(writeStream, requestBody)

      await once(writeStream, 'close')

      equal(await store.get(request), undefined)
    })

    test('respects vary directives', async (t) => {
      await cleanValkey()

      const request = {
        origin: 'localhost',
        path: '/',
        method: 'GET',
        headers: {
          'some-header': 'hello world'
        }
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: { foo: 'bar' },
        vary: {
          'some-header': 'hello world'
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }
      const requestBody = ['part1', 'part2']

      /**
       * @type {import('../lib/internal-types.d.ts').CacheStore}
       */
      const store = new CacheStore({
        clientOpts: {
          keyPrefix: `${crypto.randomUUID()}:`
        },
        errorCallback: (err) => {
          fail(err)
        }
      })

      t.after(async () => {
        await store.close()
      })

      // Sanity check
      equal(await store.get(request), undefined)

      const writeStream = store.createWriteStream(request, requestValue)
      notEqual(writeStream, undefined)
      writeResponse(writeStream, requestBody)

      await once(writeStream, 'close')

      const readStream = await store.get(structuredClone(request))
      notEqual(readStream, undefined)
      deepStrictEqual(await readResponse(readStream), {
        ...requestValue,
        body: requestBody,
      })

      const nonMatchingRequest = {
        origin: 'localhost',
        path: '/',
        method: 'GET',
        headers: {
          'some-header': 'another-value'
        }
      }
      equal(await store.get(nonMatchingRequest), undefined)
    })

    test('respects empty vary directives', async (t) => {
      await cleanValkey()

      const request = {
        origin: 'localhost',
        path: '/',
        method: 'GET'
      }

      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: { foo: 'bar' },
        vary: {
          'header-1': null
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }
      const requestBody = ['part1', 'part2']

      /**
       * @type {import('../lib/internal-types.d.ts').CacheStore}
       */
      const store = new CacheStore({
        clientOpts: {
          keyPrefix: `${crypto.randomUUID()}:`
        },
        errorCallback: (err) => {
          fail(err)
        }
      })

      t.after(async () => {
        await store.close()
      })

      // Sanity check
      equal(await store.get(request), undefined)

      const writeStream = store.createWriteStream(request, requestValue)
      notEqual(writeStream, undefined)
      writeResponse(writeStream, requestBody)

      await once(writeStream, 'close')

      const readStream = await store.get(structuredClone(request))
      notEqual(readStream, undefined)
      deepStrictEqual(await readResponse(readStream), {
        ...requestValue,
        body: requestBody,
      })
    })
  })

  test('returns cached values', async (t) => {
    await cleanValkey()

    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      clientOpts: {
        keyPrefix: `${crypto.randomUUID()}:`
      },
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    // Write the response to the store
    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream)

    // Wait for redis to be written too
    await once(writeStream, 'close')
  })

  test('does not use SCAN or KEYS during normal lookup', async (t) => {
    await cleanValkey()

    const keyPrefix = `${crypto.randomUUID()}:`
    const redis = new Redis()
    const forbiddenCommands = []

    redis.scan = async () => {
      forbiddenCommands.push('scan')
      throw new Error('SCAN must not be used during lookup')
    }
    redis.keys = async () => {
      forbiddenCommands.push('keys')
      throw new Error('KEYS must not be used during lookup')
    }

    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {
        accept: 'application/json'
      }
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      vary: {
        accept: 'application/json'
      },
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      client: redis,
      clientOpts: { keyPrefix },
      tracking: false,
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
      await redis.quit()
    })

    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream, ['indexed'])
    await once(writeStream, 'close')

    const readStream = await store.get(structuredClone(request))
    notEqual(readStream, undefined)
    deepStrictEqual(await readResponse(readStream), {
      ...requestValue,
      body: ['indexed']
    })
    deepStrictEqual(forbiddenCommands, [])
  })

  test('uses optimized lookup field for matching vary headers', async (t) => {
    await cleanValkey()

    const keyPrefix = `${crypto.randomUUID()}:`
    const redis = new Redis()
    t.after(async () => {
      await store.close()
      await redis.quit()
    })

    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {
        accept: 'application/json'
      }
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      vary: {
        accept: 'application/json'
      },
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }
    const store = new CacheStore({
      clientOpts: { keyPrefix },
      tracking: false,
      errorCallback: (err) => {
        fail(err)
      }
    })

    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream, ['indexed'])
    await once(writeStream, 'close')

    const keys = await getAllKeys()
    const indexKey = keys.find(key => key.startsWith(`${keyPrefix}cache:v2:`) && key.endsWith(':index'))
    ok(indexKey)

    const indexFields = await redis.hkeys(indexKey)
    ok(indexFields.some(field => field.startsWith('$lookup:') && field !== '$lookup:no-headers'))

    const readStream = await store.get(structuredClone(request))
    notEqual(readStream, undefined)
    deepStrictEqual(await readResponse(readStream), {
      ...requestValue,
      body: ['indexed']
    })
  })

  test('co-locates entry keys with the URL/method index for cluster slots', async (t) => {
    await cleanValkey()

    const keyPrefix = `${crypto.randomUUID()}:`
    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {
        'cache-tag': 'cluster'
      },
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      clientOpts: { keyPrefix },
      cacheTagsHeader: 'cache-tag',
      tracking: false,
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream, ['cluster-ready'])
    await once(writeStream, 'close')

    const keys = await getAllKeys()
    const indexKey = keys.find(key => key.startsWith(`${keyPrefix}cache:v2:`) && key.endsWith(':index'))
    ok(indexKey)

    const hashTag = indexKey.match(/\{([^}]+)\}/)?.[1]
    ok(hashTag)

    ok(keys.some(key => key.startsWith(`${keyPrefix}metadata:{${hashTag}}:`)))
    ok(keys.some(key => key.startsWith(`${keyPrefix}ids:{${hashTag}}:`)))
    ok(keys.some(key => key.startsWith(`${keyPrefix}values:{${hashTag}}:`)))
    ok(keys.some(key => key.startsWith(`${keyPrefix}cache-tags:{${hashTag}}:`)))
  })

  test('accepts top-level keyPrefix without passing it to the client', async (t) => {
    await cleanValkey()

    const keyPrefix = `${crypto.randomUUID()}:`
    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      keyPrefix,
      tracking: false,
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream, ['prefixed'])
    await once(writeStream, 'close')

    const keys = await getAllKeys()
    ok(keys.every(key => key.startsWith(keyPrefix) || key.startsWith('cache:v2:global-tag:')))

    const readStream = await store.get(structuredClone(request))
    notEqual(readStream, undefined)
    deepStrictEqual(await readResponse(readStream), {
      ...requestValue,
      body: ['prefixed']
    })
  })

  test('caches repeated optimized misses until a local write clears the miss', async (t) => {
    await cleanValkey()

    const redis = new Redis()
    let lookupReads = 0
    const originalHget = redis.hget.bind(redis)
    redis.hget = async (key, field) => {
      if (key.includes('cache:v2:') && String(field).startsWith('$lookup:')) {
        lookupReads++
      }
      return originalHget(key, field)
    }

    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }
    const store = new CacheStore({
      client: redis,
      tracking: false,
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
      await redis.quit()
    })

    strictEqual(await store.get(request), undefined)
    strictEqual(await store.get(request), undefined)
    strictEqual(lookupReads, 1)

    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream, ['indexed'])
    await once(writeStream, 'close')

    const readStream = await store.get(request)
    notEqual(readStream, undefined)
    strictEqual(lookupReads, 2)
  })

  test('uses hash field expiration for indexed entries when supported', async (t) => {
    await cleanValkey()

    const redis = new Redis()
    t.after(async () => {
      await redis.quit()
    })

    if (!await supportsHashFieldExpiration(redis)) {
      t.skip('HEXPIREAT is not supported by this Redis/Valkey server')
      return
    }

    const keyPrefix = `${crypto.randomUUID()}:`
    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      clientOpts: { keyPrefix },
      tracking: false,
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream, ['indexed'])
    await once(writeStream, 'close')

    const keys = await getAllKeys()
    const indexKey = keys.find(key => key.startsWith(`${keyPrefix}cache:v2:`) && key.endsWith(':index'))
    ok(indexKey)

    const indexFields = await redis.hkeys(indexKey)
    ok(indexFields.includes('no-vary'))

    const [ttl] = await redis.call('HTTL', indexKey, 'FIELDS', 1, 'no-vary')
    ok(ttl > 0)
    ok(ttl <= 20)
  })

  test('can disable hash field expiration optimization', async (t) => {
    await cleanValkey()

    const redis = new Redis()
    t.after(async () => {
      await redis.quit()
    })

    if (!await supportsHashFieldExpiration(redis)) {
      t.skip('HEXPIREAT is not supported by this Redis/Valkey server')
      return
    }

    const keyPrefix = `${crypto.randomUUID()}:`
    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      clientOpts: { keyPrefix },
      tracking: false,
      enableValkey9Optimizations: false,
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream, ['indexed'])
    await once(writeStream, 'close')

    const keys = await getAllKeys()
    const indexKey = keys.find(key => key.startsWith(`${keyPrefix}cache:v2:`) && key.endsWith(':index'))
    ok(indexKey)

    const indexFields = await redis.hkeys(indexKey)
    ok(indexFields.includes('no-vary'))

    const [ttl] = await redis.call('HTTL', indexKey, 'FIELDS', 1, 'no-vary')
    strictEqual(ttl, -1)
  })

  test('invalidates cache by cache keys', async (t) => {
    await cleanValkey()

    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      clientOpts: {
        keyPrefix: `${crypto.randomUUID()}:`
      },
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    // Write the response to the store
    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream)

    // Wait for redis to be written too
    await once(writeStream, 'close')

    {
      const keys = await getAllKeys()
      strictEqual(keys.length, 5)
    }

    await store.deleteKeys([
      { method: 'GET', origin: 'http://test-origin-1', path: '/foo?bar=baz' }
    ])

    {
      const keys = await getAllKeys()
      strictEqual(countEntryKeys(keys), 0)
    }
  })

  test('invalidates cache by ids', async (t) => {
    await cleanValkey()

    const request = {
      origin: 'http://test-origin-1',
      path: '/foo?bar=baz',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: {},
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }

    const store = new CacheStore({
      clientOpts: {
        keyPrefix: `${crypto.randomUUID()}:`
      },
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    // Write the response to the store
    const writeStream = store.createWriteStream(request, requestValue)
    writeResponse(writeStream)

    // Wait for redis to be written too
    await once(writeStream, 'close')

    {
      const keys = await getAllKeys()
      strictEqual(keys.length, 5)
    }

    await store.deleteKeys([
      { method: 'GET', origin: 'http://test-origin-1', path: '/foo?bar=baz' }
    ])

    {
      const keys = await getAllKeys()
      strictEqual(countEntryKeys(keys), 0)
    }
  })

  test('invalidates cache by combined cache tag', async (t) => {
    await cleanValkey()

    const store = new CacheStore({
      cacheTagsHeader: 'cache-tag',
      clientOpts: {
        keyPrefix: `${crypto.randomUUID()}:`
      },
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    {
      const request = {
        origin: 'http://test-origin-1',
        path: '/foo-1?bar=baz',
        method: 'GET',
        headers: {
          'cache-tag': 'tag1,tag2'
        }
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: {
          'cache-tag': 'tag1,tag2'
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }

      // Write the response to the store
      const writeStream = store.createWriteStream(request, requestValue)
      writeResponse(writeStream)

      // Wait for redis to be written too
      await once(writeStream, 'close')
    }

    {
      const request = {
        origin: 'http://test-origin-1',
        path: '/foo-2?bar=baz',
        method: 'GET',
        headers: {
          'cache-tag': 'tag1,tag2,tag3'
        }
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: {
          'cache-tag': 'tag1,tag2,tag3'
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }

      // Write the response to the store
      const writeStream = store.createWriteStream(request, requestValue)
      writeResponse(writeStream)

      // Wait for redis to be written too
      await once(writeStream, 'close')
    }

    {
      const request = {
        origin: 'http://test-origin-1',
        path: '/foo-3?bar=baz',
        method: 'GET',
        headers: {
          'cache-tag': 'tag1,tag3'
        }
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: {
          'cache-tag': 'tag1,tag3'
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }

      // Write the response to the store
      const writeStream = store.createWriteStream(request, requestValue)
      writeResponse(writeStream)

      // Wait for redis to be written too
      await once(writeStream, 'close')
    }

    {
      const keys = await getAllKeys()
      strictEqual(countEntryKeys(keys), 12)
    }

    await store.deleteTags([['tag1', 'tag2']])

    {
      const keys = await getAllKeys()
      strictEqual(countEntryKeys(keys), 4)

      const tagsKeys = keys.filter(key => key.includes('cache-tags'))
      strictEqual(tagsKeys.length, 1)

      ok(tagsKeys[0].includes('tag1:tag3'))
    }
  })

  test('invalidates cache by cache tag', async (t) => {
    await cleanValkey()

    const store = new CacheStore({
      cacheTagsHeader: 'cache-tag',
      clientOpts: {
        keyPrefix: `${crypto.randomUUID()}:`
      },
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    {
      const request = {
        origin: 'http://test-origin-1',
        path: '/foo-1?bar=baz',
        method: 'GET',
        headers: {
          'cache-tag': 'tag1,tag2'
        }
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: {
          'cache-tag': 'tag1,tag2'
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }

      // Write the response to the store
      const writeStream = store.createWriteStream(request, requestValue)
      writeResponse(writeStream)

      // Wait for redis to be written too
      await once(writeStream, 'close')
    }

    {
      const request = {
        origin: 'http://test-origin-1',
        path: '/foo-2?bar=baz',
        method: 'GET',
        headers: {
          'cache-tag': 'tag1,tag3'
        }
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: {
          'cache-tag': 'tag1,tag3'
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }

      // Write the response to the store
      const writeStream = store.createWriteStream(request, requestValue)
      writeResponse(writeStream)

      // Wait for redis to be written too
      await once(writeStream, 'close')
    }

    {
      const request = {
        origin: 'http://test-origin-1',
        path: '/foo-3?bar=baz',
        method: 'GET',
        headers: {
          'cache-tag': 'tag3,tag4'
        }
      }
      const requestValue = {
        statusCode: 200,
        statusMessage: '',
        headers: {
          'cache-tag': 'tag3,tag4'
        },
        cachedAt: Date.now(),
        staleAt: Date.now() + 10000,
        deleteAt: Date.now() + 20000
      }

      // Write the response to the store
      const writeStream = store.createWriteStream(request, requestValue)
      writeResponse(writeStream)

      // Wait for redis to be written too
      await once(writeStream, 'close')
    }

    {
      const keys = await getAllKeys()
      strictEqual(countEntryKeys(keys), 12)
    }

    await store.deleteTags(['tag1', 'tag4'])

    {
      const keys = await getAllKeys()
      strictEqual(countEntryKeys(keys), 0)
    }
  })

  test('saves entry with a custom id', async (t) => {
    await cleanValkey()

    const request = {
      id: 'custom-id',
      origin: 'localhost',
      path: '/',
      method: 'GET',
      headers: {}
    }
    const requestValue = {
      statusCode: 200,
      statusMessage: '',
      headers: { foo: 'bar' },
      cachedAt: Date.now(),
      staleAt: Date.now() + 10000,
      deleteAt: Date.now() + 20000
    }
    const requestBody = ['asd', '123']

    /**
     * @type {import('../lib/internal-types.d.ts').CacheStore}
     */
    const store = new CacheStore({
      clientOpts: {
        keyPrefix: `${crypto.randomUUID()}:`
      },
      errorCallback: (err) => {
        fail(err)
      }
    })

    t.after(async () => {
      await store.close()
    })

    // Sanity check
    equal(await store.get(request), undefined)

    // Write the response to the store
    const writeStream = store.createWriteStream(request, requestValue)
    notEqual(writeStream, undefined)
    writeResponse(writeStream, requestBody)

    const [entry] = await once(store, 'write')
    strictEqual(entry.id, 'custom-id')
  })
}

/**
 * @param {import('node:stream').Writable} stream
 * @param {string[]} body
 */
function writeResponse (stream, body = []) {
  for (const chunk of body) {
    stream.write(Buffer.from(chunk))
  }

  stream.end()
}

function countEntryKeys (keys) {
  return keys.filter(key =>
    key.includes('metadata:') ||
    key.includes('values:') ||
    key.includes('ids:') ||
    key.includes('cache-tags:')
  ).length
}

async function supportsHashFieldExpiration (redis) {
  const commandInfo = await redis.call('COMMAND', 'INFO', 'HEXPIREAT')
  return Array.isArray(commandInfo) && commandInfo[0] !== null
}

/**
 * @param {import('../lib/internal-types.d.ts').GetResult} result
 * @returns {Promise<import('../lib/internal-types.d.ts').GetResult | { body: Buffer[] }>}
 */
async function readResponse ({ body: src, ...response }) {
  notEqual(response, undefined)
  notEqual(src, undefined)

  const stream = Readable.from(src ?? [])

  /**
   * @type {Buffer[]}
   */
  const body = []
  stream.on('data', chunk => {
    body.push(chunk.toString())
  })

  await once(stream, 'end')

  return {
    ...response,
    body
  }
}
