package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.RedisConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import com.redislabs.provider.redis._

class TempTesterSuite extends FunSuite {

  test("test redis hashRDD") {

    val sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
//      .set("redis.servers", "host-10-1-236-129:7000")
    )

    val props = new java.util.HashMap[String, String]() {
      // put("redis.servers", "127.0.0.1:7379")
      put("redis.servers", "host-10-1-236-129:7002")
    }

    val redisConfig = new RedisConfig(props)

    val redisHashRDD = sc.fromRedisHashCSV( "dcache_*", props)
    val hashContents = redisHashRDD.collect.foreach(row => {
      System.out.println("data: " + row.toList)
    })

  }
}
