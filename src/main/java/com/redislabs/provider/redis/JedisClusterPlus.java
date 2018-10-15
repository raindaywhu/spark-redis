package com.redislabs.provider.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.Serializable;
import java.util.Set;

public class JedisClusterPlus extends JedisCluster implements Serializable {

    public JedisClusterPlus(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout,
                           int maxAttempts, String password, final GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
        super.connectionHandler = new JedisSlotAdvancedConnectionHandler(jedisClusterNode, poolConfig,
                connectionTimeout, soTimeout, password);
    }

    public JedisClusterPlus(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout,
                           int maxAttempts, final GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
        super.connectionHandler = new JedisSlotAdvancedConnectionHandler(jedisClusterNode, poolConfig,
                connectionTimeout, soTimeout);
    }

    public JedisSlotAdvancedConnectionHandler getConnectionHandler() {
        return (JedisSlotAdvancedConnectionHandler)this.connectionHandler;
    }
}
