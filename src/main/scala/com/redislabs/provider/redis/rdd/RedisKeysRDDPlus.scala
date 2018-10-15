package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.{RedisConfig, RedisNode}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

class RedisKeysRDDPlus(sc: SparkContext, var prev: RDD[String], val redisConfig: RedisConfig)
  extends RDD[Seq[String]](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[String]] = {

    val keys = firstParent[String].iterator(split, context)

    getHASH(redisConfig, redisConfig.nodes, keys)

  }

  def getHASH(redisConfig: RedisConfig,
              nodes: Array[RedisNode],
              keys: Iterator[String]): Iterator[Seq[String]] = {
    //FIXME use JedisSlotAdvancedConnectionHandler.getJedisPoolFromSlot instead of groupKeysByNode
    groupKeysByNode(nodes, keys).flatMap {
      x =>
      {
        logInfo("partition data: ")
        val conn = redisConfig.connect(x._1)
        //FIXME this filter not NEEDED
        val hashKeys = filterKeysByType(conn, x._2, "hash")
        val res = hashKeys.map(key => {
          val item = key :: conn.hgetAll(key).values().toList
          logInfo("partition : " + item)
          item
        }).iterator
        conn.close
        res
      }
    }.iterator
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

}
