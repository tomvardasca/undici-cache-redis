# Indexed lookup architecture

This fork removes `SCAN` from the normal cache lookup path by adding a v2 URL/method index alongside the legacy metadata, value, and ID keys.

## Old bottleneck

The upstream store wrote one metadata key per cached response:

- `metadata:{origin}:{path}:{method}:{id}`
- `values:{id}`
- `ids:{id}`
- `cache-tags:{tag1}:{tag2}:{id}`

To satisfy `get(key)`, the store scanned `metadata:{origin}:{path}:{method}:*`, fetched every matching metadata key, parsed `vary`, and selected the most specific match. That made lookup cost depend on Redis/Valkey database size because a complete `SCAN` iteration is O(N).

## New lookup model

Writes now maintain a versioned index hash per URL/method:

- `{prefix}cache:v2:{urlMethodHash}:index`

The hash field is a deterministic Vary signature:

- `no-vary` for responses without Vary metadata
- `sha256(canonicalVary).slice(0, 32)` for Vary responses

The hash value is compact JSON containing:

- legacy `metadataKey`, `idKey`, and `valueKey`
- `indexKey` and `indexField`
- normalized `vary`
- cache tags
- `deleteAt`
- schema version

Normal lookup performs one `HGETALL` for the URL/method index, filters only variants for that URL/method, and then reads the selected value key. It never calls `SCAN`, `KEYS`, broad pattern matching, or per-shard scans.

Lookup complexity is O(V), where V is the number of variants for that URL/method. It is independent of the total number of unrelated keys in Valkey.

## Vary signature

Vary metadata is normalized before indexing:

- header names are lower-cased
- entries are sorted by header name
- `undefined` is stored as `null`
- request header matching is case-insensitive
- `Vary: *` is treated as uncacheable and is not written

When multiple variants match a request, the store preserves the existing behavior and returns the most specific match, measured by number of Vary headers.

## TTL and stale behavior

HTTP freshness data remains stored in the cached value:

- `cachedAt`
- `staleAt`
- `deleteAt`
- `cacheControlDirectives`
- response headers such as `Date`, `Age`, and `Expires` when supplied by Undici

Native Valkey expiration is still used as garbage collection, but lookup also checks `deleteAt`. Expired or malformed index references are lazily cleaned during lookup.

## Tag invalidation

The legacy combined `cache-tags:*` keys are still written for manager compatibility. In addition, writes maintain direct tag indexes:

- `{prefix}cache:v2:tag:{tagHash}`
- `cache:v2:global-tag:{tagHash}`

Tag index members are the same v2 entry references used by the URL/method index. Public tag invalidation reads the first tag index directly, filters members for all requested tags, and deletes matching entries. It does not scan the keyspace.

## Shard behavior

The URL/method index uses a Cluster hash tag derived from the URL/method hash:

- `cache:v2:{urlMethodHash}:index`

Entry metadata, ID, value, and compatibility tag keys use the same `{urlMethodHash}` hash tag. This colocates a URL/method group in one Cluster slot and lets normal reads stay shard-local. When the optimized lookup field is present, the hot path is a single hash read from the URL/method index; otherwise the store reads the index hash and selected value key on the same shard.

Tag indexes use tag-derived hash tags. Invalidation can touch entries across multiple URL/method groups, so each entry reference contains enough key information to delete the affected entry directly without a global scan.

## Migration behavior

The fork writes v2 indexes for new entries while preserving legacy keys. Existing legacy-only entries are not discovered by normal lookup because doing so would require the old SCAN hot path. Operators should either flush cache data on upgrade or run an explicit migration tool. SCAN remains available only in manager/maintenance paths and tests.

## Current tradeoffs

- Normal lookup has been moved to the indexed path and is covered by a command-spy test that fails on `SCAN` or `KEYS`.
- Existing manager APIs still use legacy SCAN-based maintenance routines.
- Cluster construction is additive, shard-local entry key layout is covered by unit tests, and Valkey 7/8/9 Cluster matrix automation remains to be completed.
- OpenTelemetry metrics are not implemented in this slice.
