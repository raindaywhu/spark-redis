package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis._
import org.scalatest.{FunSuite, ShouldMatchers}
import redis.clients.util.JedisClusterCRC16

class RedisConfigSuite extends FunSuite with ShouldMatchers {

    val props = new java.util.HashMap[String, String]() {
      //put("redis.servers", "127.0.0.1:7379")
      put("redis.servers", "host-10-1-236-129:7000")
    }

    val security_props = new java.util.HashMap[String, String]() {
      //put("redis.servers", "127.0.0.1:7379")
      //put("redis.server.password", "passwd")
      put("redis.servers", "host-10-1-236-129:7000")
    }

  val redisStandaloneConfig = new RedisConfig(security_props)
  val redisClusterConfig = new RedisConfig(props)

  test("getNodesBySlots") {
    assert(redisStandaloneConfig.getNodesBySlots(0, 16383).size == 1)
    assert(redisClusterConfig.getNodesBySlots(0, 16383).size == 7)
  }

//  test("connectionForKey") {
//    val key = "connectionForKey"
//    val slot = JedisClusterCRC16.getSlot(key)
//    val standaloneConn = redisStandaloneConfig.connectionForKey(key)
//    assert()
//    assert()
//  }

  test("getHost") {
    val key = "getHost"
    val slot = JedisClusterCRC16.getSlot(key)
    val standaloneHost = redisStandaloneConfig.getHost(key)
    assert(standaloneHost.startSlot <= slot && standaloneHost.endSlot >= slot)
    val clusterHost = redisClusterConfig.getHost(key)
    assert(clusterHost.startSlot <= slot && clusterHost.endSlot >= slot)
  }

  test("getNodes") {
    assert(redisStandaloneConfig.getNodes().size == 1)
    assert(redisClusterConfig.getNodes().size == 7)
  }
}
