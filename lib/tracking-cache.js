'use strict'

const { LRUMap } = require('lru_map')

class TrackingCache {
  /**
   * @type {LRUMap}
   */
  #data

  /**
   * @type {number}
   */
  #maxCount

  /**
   * @type {number}
   */
  #maxSize

  /**
   * @type {number}
   */
  #count = 0

  /**
   * @type {number}
   */
  #size = 0

  constructor (opts = {}) {
    this.#maxCount = opts.maxCount ?? Infinity
    this.#maxSize = opts.maxSize ?? Infinity
    this.#data = new LRUMap(this.#maxCount + 1)
  }

  get count () {
    return this.#count
  }

  get size () {
    return this.#size
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {import('./internal-types.d.ts').GetResult | undefined}
   */
  get (key) {
    const entry = this.#findMatchingEntry(key)
    return entry?.result
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @param {object} metadata
   * @param {import('./internal-types.d.ts').GetResult} result
   * @returns {void}
   */
  set (key, metadata, result) {
    const entry = this.#findMatchingEntry(key)
    if (entry !== undefined) {
      this.delete(entry.key)
    }

    const trackingMetadataKey = serializeTackingMetadataKey(key)

    let entries = this.#data.get(trackingMetadataKey)
    if (entries === undefined) {
      entries = new Map()
      this.#data.set(trackingMetadataKey, entries)
    }
    const size = this.#countResultSize(result)
    entries.set(key.id, { metadata, result, size })

    this.#count++
    this.#size += size

    if (this.#count > this.#maxCount || this.#size > this.#maxSize) {
      this.#clean()
    }
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {void}
   */
  delete (key) {
    const trackingMetadataKey = serializeTackingMetadataKey(key)
    const entries = this.#data.get(trackingMetadataKey)
    if (entries === undefined) return

    const entry = entries.get(key.id)
    if (entry === undefined) return

    entries.delete(key.id)

    this.#count--
    this.#size -= entry.size

    if (entries.size === 0) {
      this.#data.delete(trackingMetadataKey)
    }
  }

  clear () {
    this.#data.clear()
    this.#count = 0
    this.#size = 0
  }

  /**
   * @param {import('./internal-types.d.ts').CacheKey} key
   * @returns {{ metadata: object, result: object } | undefined}
   */
  #findMatchingEntry (key) {
    const trackingMetadataKey = serializeTackingMetadataKey(key)
    const entries = this.#data.get(trackingMetadataKey)
    if (entries === undefined) return undefined

    let bestMatch
    let bestMatchVaryCounter = -1

    for (const [id, entry] of entries.entries()) {
      if (entry.result.deleteAt <= Date.now()) {
        entries.delete(id)
        this.#count--
        this.#size -= entry.size
        continue
      }

      let matches = true
      const vary = entry.metadata.vary
      const varyCounter = vary ? Object.keys(vary).length : 0

      if (vary) {
        const headers = normalizeHeaders(key.headers)

        for (const header in vary) {
          const normalizedHeader = header.toLowerCase()

          if (headers[normalizedHeader] === undefined && (vary[header] === null || vary[header] === undefined)) {
            continue
          }

          if (headers[normalizedHeader] !== vary[header]) {
            matches = false
            break
          }
        }
      }

      if (matches) {
        if (varyCounter > bestMatchVaryCounter) {
          bestMatch = { ...entry, key: { ...key, id } }
          bestMatchVaryCounter = varyCounter
        }
      }
    }

    if (entries.size === 0) {
      this.#data.delete(trackingMetadataKey)
    }

    return bestMatch
  }

  #countResultSize (result) {
    let size = 0
    for (const buffer of result.body) {
      size += buffer.length
    }
    return size
  }

  #clean () {
    while (this.#count > this.#maxCount || this.#size > this.#maxSize) {
      const entries = this.#data.shift()[1]
      for (const entry of entries.values()) {
        this.#count--
        this.#size -= entry.size
      }
    }
  }
}

function serializeTackingMetadataKey (key) {
  const { origin, path, method } = key

  const encodedOrigin = encodeURIComponent(origin)
  const encodedPath = encodeURIComponent(path)
  return `${encodedOrigin}:${encodedPath}:${method}`
}

function normalizeHeaders (headers) {
  const normalized = {}
  if (!headers) return normalized

  for (const [name, value] of Object.entries(headers)) {
    normalized[name.toLowerCase()] = value
  }

  return normalized
}

module.exports = TrackingCache
