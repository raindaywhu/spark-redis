package com.redislabs.provider.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;

import java.util.Map;
import java.util.Set;

public class JedisSingletonConnHandler {

    private static volatile JedisSlotAdvancedConnectionHandler instance = null;

    public static JedisSlotAdvancedConnectionHandler getInstance(Map<String, String> props) {
        if (null == instance) {
            synchronized (JedisSingletonConnHandler.class) {
                if (null == instance) {
                    instance = JedisClusterPlusFactory.getJedisClusterPlus(props).getConnectionHandler();
                }
            }
        }
        return instance;
    }
}
