package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.{RedisConfig, RedisNode}
import com.redislabs.provider.redis.partitioner.RedisPartition
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

class RedisHashRDD(prev: RDD[String])
  extends RDD[Seq[String]](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[String]] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val keys = firstParent[String].iterator(split, context)
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    getHASH(partition.redisConfig, nodes, keys)
  }

  def getHASH(redisConfig: RedisConfig,
              nodes: Array[RedisNode],
              keys: Iterator[String]): Iterator[Seq[String]] = {
    //FIXME use JedisSlotAdvancedConnectionHandler.getJedisPoolFromSlot instead of groupKeysByNode
    groupKeysByNode(nodes, keys).flatMap {
      x =>
      {
        val conn = redisConfig.connect(x._1)
        val hashKeys = filterKeysByType(conn, x._2, "hash")
        val res = hashKeys.map(key => {
          key :: conn.hgetAll(key).values().toList
        }).iterator
        conn.close
        res
      }
    }.iterator
  }

  /*
  def GeneratorKeyMap(valueOfKey: String,values : java.util.Map[String,String]) : java.util.Map[String,String]= {
    values.put(keySchema,valueOfKey)
    values
  }

  def getHASH(nodes: Array[RedisNode], keys: Iterator[String]): Iterator[String] = {
    val service = new ExecutorCompletionService[Seq[(String, java.util.Map[String, String])]](Executors.newCachedThreadPool)
    //FIXME to be paralled
    groupKeysByNode(nodes, keys).flatMap {
      x =>
      {
        val conn = x._1.endpoint.connect()
        val hashKeys = filterKeysByType(conn, x._2, "hash")
        var values =  new ArrayBuffer[java.util.Map[String,String]]()
        val listKeys = hashKeys.toList.grouped(1000).toList

        listKeys.foreach{ item => service.submit(new HGetAllHandler(conn, item))}

        for (_ <- listKeys.indices){
          val result = service.take().get()
          if (result != null ){result.foreach(rs =>  values +=  GeneratorKeyMap(rs._1,rs._2)) }
        }

        val res = values.map{
          elem => {
            var MAPPER = JsonUtil.getMapper()
            MAPPER.writeValueAsString(elem)
          }
        }.iterator
        res
      }
    }.iterator
  }*/
}

