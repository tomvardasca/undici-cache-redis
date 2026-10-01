import { expectAssignable } from "tsd";
import { RedisOptions } from "iovalkey";
import { RedisCacheStoreOpts } from "..";

const clientOpts: RedisOptions = {}

expectAssignable<RedisCacheStoreOpts>({})

expectAssignable<RedisCacheStoreOpts>({
    clientOpts: {},
    maxSize: 0,
    tracking: true
})

expectAssignable<RedisCacheStoreOpts>({
    clientOpts: clientOpts,
    cacheErrorResponses: false,
    maxSize: 0,
    tracking: false
})
