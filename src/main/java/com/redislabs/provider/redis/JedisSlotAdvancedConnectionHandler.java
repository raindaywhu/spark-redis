package com.redislabs.provider.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;

import java.io.Serializable;
import java.util.Random;
import java.util.Set;

public class JedisSlotAdvancedConnectionHandler extends JedisSlotBasedConnectionHandler implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(JedisSlotAdvancedConnectionHandler.class);

    public JedisSlotAdvancedConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout) {
        super(nodes, poolConfig, connectionTimeout, soTimeout);
        cache.getNodes().forEach((k,v) -> {
            logger.info("init hosts: " + k);
        });
    }

    public JedisSlotAdvancedConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password) {
        super(nodes, poolConfig, connectionTimeout, soTimeout, password);
        cache.getNodes().forEach((k,v) -> {
            logger.info("init hosts: " + k);
        });
    }

    public JedisPool getJedisPoolFromSlot(int slot) {
        JedisPool connectionPool = cache.getSlotPool(slot);
        if (connectionPool != null) {
            // It can't guaranteed to get valid connection because of node
            // assignment
            return connectionPool;
        } else {
            renewSlotCache(); //It's abnormal situation for cluster mode, that we have just nothing for slot, try to rediscover state
            connectionPool = cache.getSlotPool(slot);
            if (connectionPool != null) {
                return connectionPool;
            } else {
                throw new JedisNoReachableClusterNodeException("No reachable node in cluster for slot " + slot);
            }
        }
    }

    public JedisPool getJedisPoolFromNode(String host, int port) {
        String nodeKey = host + ":" + port;
        logger.info("try to find host: " + nodeKey);
        cache.getNodes().forEach((k,v) -> {
            logger.info("cache map, host: " + k);
        });
        //return cache.getNode(nodeKey);
        //return cache.setupNodeIfNotExist(new HostAndPort(host, port));
        JedisPool connPool = cache.getNode(nodeKey);
        if (null != connPool) {
            return connPool;
        } else {
            renewSlotCache();;
            if (connPool != null) {
                return connPool;
            } else {
                throw new JedisNoReachableClusterNodeException("No reachable node in cluster for node" + nodeKey);
            }
        }
    }

    public JedisPool getRandomJedisPool() {
        cache.getNodes().forEach((k,v) -> {
            logger.info("init cache map, host: " + k);
        });
        int randSlot = (int)(Math.random() * 16383);
        return cache.getSlotPool(randSlot);
    }

}
