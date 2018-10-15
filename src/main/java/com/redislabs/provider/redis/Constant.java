package com.redislabs.provider.redis;

public class Constant {

    public static final String JEDIS_SERVERS = "redis.servers";
    public static final String JEDIS_QUERY_BATCHSIZE = "redis.query.batchSize";
    public static final String JEDIS_SERVER_SECURITY_ENABLED = "redis.server.security.enabled";
    public static final String JEDIS_SERVER_PASSWORD = "redis.server.password";
    public static final String JEDIS_SERVER_CONNECTIONTIMEOUT = "redis.server.connectionTimeoutMillis";
    public static final String JEDIS_SERVER_RESPONSETIMEOUT = "redis.server.responseTimeoutMillis";
    public static final String JEDIS_SERVER_MAXATTEMPTS = "redis.server.maxAttempts";

    public static final String JEDIS_MAXIDLE = "redis.maxIdle";
    public static final String JEDIS_MAXTOTAL = "redis.maxTotal";
    public static final String JEDIS_MINIDLE = "redis.minIdle";
    public static final String JEDIS_TESTONBORROW = "redis.TestOnBorrow";
    public static final String JEDIS_MINEVICTABLEIDLETIMEMILLIS = "redis.MinEvictableIdleTimeMillis";
    public static final String JEDIS_MAXWAITMILLIS= "redis.maxWaitMillis";
    public static final String JEDIS_BLOCKWHENEXHAUSTED = "redis.blockWhenExhausted";
    public static final String JEDIS_TESTONRETURN = "redis.testOnReturn";
    public static final String JEDIS_NUMTESTSPEREVICTIONRUN = "redis.numTestsPerEvictionRun";
    public static final String JEDIS_TESTWHILEIDLE = "redis.testWhileIdle";
}
