package com.redislabs.provider.redis;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Map;

public class JedisClusterPlusFactory {

    private static Logger logger = LoggerFactory.getLogger(JedisClusterPlusFactory.class);

    public static HostAndPort getJedisInitHost(Map<String, String> props) {

        String jedisServers = props.get(Constant.JEDIS_SERVERS);
        String[] hostAndPort = jedisServers.split(",")[0].split(":");
        return new HostAndPort(hostAndPort[0], Integer.valueOf(hostAndPort[1]));
    }

    public static JedisClusterPlus getJedisClusterPlus(Map<String, String> props) {

        String jedisServers = props.get(Constant.JEDIS_SERVERS);

        logger.info("Init redis cluster from " + jedisServers);

        JedisPoolConfig jedisPoolConfig = getJedisPooConfig(props);

        logger.info("cacheProps= " + props);

        HashSet<HostAndPort> jedisClusterNodes = new java.util.HashSet<>();

        for (String host : jedisServers.split(",")) {
            String[] hostAndPort = host.split(":");
            jedisClusterNodes.add(new HostAndPort(hostAndPort[0], Integer.valueOf(hostAndPort[1])));
        }

        int connectionTimeout = Integer.valueOf(props.getOrDefault(Constant.JEDIS_SERVER_CONNECTIONTIMEOUT,
                "2000"));
        int responseTimeout = Integer.valueOf(props.getOrDefault(Constant.JEDIS_SERVER_RESPONSETIMEOUT,
                "2000"));
        String password = props.getOrDefault(Constant.JEDIS_SERVER_PASSWORD, "");
        int maxAttempts = Integer.valueOf(props.getOrDefault(Constant.JEDIS_SERVER_MAXATTEMPTS, "3"));

        boolean security = Boolean.valueOf(props.getOrDefault(Constant.JEDIS_SERVER_SECURITY_ENABLED, "false"));

        JedisClusterPlus jedisClusterPlus = null;

        logger.info("JedisCluster config, connectionTimeout:" + connectionTimeout + "responseTimeout: " + responseTimeout);
        if (security) {
            logger.info("Redis cluster security Mode");
            jedisClusterPlus = new JedisClusterPlus(jedisClusterNodes, connectionTimeout, responseTimeout, maxAttempts, password, jedisPoolConfig);
        } else {
            logger.info("Redis cluster unsecurity Mode");
            jedisClusterPlus = new JedisClusterPlus(jedisClusterNodes, connectionTimeout, responseTimeout, maxAttempts, jedisPoolConfig);
        }

        return jedisClusterPlus;
    }

    protected static JedisPoolConfig getJedisPooConfig(Map<String, String> properties) {

        JedisPoolConfig jedisConfig = new JedisPoolConfig();
        String maxIdle = properties.getOrDefault(Constant.JEDIS_MAXIDLE,
                "" + GenericObjectPoolConfig.DEFAULT_MAX_IDLE);
        String minIdle = properties.getOrDefault(Constant.JEDIS_MINIDLE,
                "" + GenericObjectPoolConfig.DEFAULT_MIN_IDLE);
        String maxTotal = properties.getOrDefault(Constant.JEDIS_MAXTOTAL,
                "" + GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);
        String testOnBorrow = properties.getOrDefault(Constant.JEDIS_TESTONBORROW,
                "" + BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW);
        String blockWhenExhausted = properties.getOrDefault(Constant.JEDIS_BLOCKWHENEXHAUSTED,
                "" + BaseObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED);
        String maxWaitMillis = properties.getOrDefault(Constant.JEDIS_MAXWAITMILLIS,
                "" + BaseObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS);
        String minEvictableIdleTimeMillis = properties.getOrDefault(Constant.JEDIS_MINEVICTABLEIDLETIMEMILLIS,
                "" + BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        String  testOnReturn = properties.getOrDefault(Constant.JEDIS_TESTONRETURN,
                "" + BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
        String numTestsPerEvictionRun = properties.getOrDefault(Constant.JEDIS_NUMTESTSPEREVICTIONRUN,
                "" + BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
        String testWhileIdle = properties.getOrDefault(Constant.JEDIS_TESTWHILEIDLE,
                 "" + BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);

        logger.info("JedisPoolConfig: maxTotal " + maxTotal + " maxIdle: " + maxIdle + " minIdle: " + minIdle);

        jedisConfig.setMaxIdle(Integer.valueOf(maxIdle));
        jedisConfig.setMinIdle(Integer.valueOf(minIdle));
        jedisConfig.setMaxTotal(Integer.valueOf(maxTotal));
        jedisConfig.setTestOnBorrow(Boolean.valueOf(testOnBorrow));
        jedisConfig.setBlockWhenExhausted(Boolean.valueOf(blockWhenExhausted));
        jedisConfig.setMaxWaitMillis(Integer.valueOf(maxWaitMillis));
        jedisConfig.setMinEvictableIdleTimeMillis(Integer.valueOf(minEvictableIdleTimeMillis));
        jedisConfig.setTestOnReturn(Boolean.valueOf(testOnReturn));
        jedisConfig.setNumTestsPerEvictionRun(Integer.valueOf(numTestsPerEvictionRun));
        jedisConfig.setTestWhileIdle(Boolean.valueOf(testWhileIdle));
        return jedisConfig;
    }

}
