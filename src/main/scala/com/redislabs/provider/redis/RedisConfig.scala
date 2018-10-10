package com.redislabs.provider.redis

import java.net.URI
import java.util

import org.apache.spark.SparkConf
import redis.clients.jedis.{Jedis, Protocol}
import redis.clients.util.{JedisClusterCRC16, JedisURIHelper, SafeEncoder}

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


case class RedisNode(val host: String,
                     val port: Int,
                     val startSlot: Int,
                     val endSlot: Int,
                     val idx: Int,
                     val total: Int)



object RedisConfig {

  var connectionHandler: JedisSlotAdvancedConnectionHandler = null

  implicit def arrayAsJavaMap[A,B](m: Array[(A,B)]): java.util.Map[A,B] = {
    val propsMap = new util.HashMap[A, B]()
    m.foreach(conf => {
      propsMap.put(conf._1, conf._2)
    })
    propsMap
  }
}


/**
  * RedisConfig holds the state of the cluster nodes, and uses consistent hashing to map
  * keys to nodes
  * val initialHost: RedisEndpoint
  */
class RedisConfig(val props: java.util.Map[String, String]) extends  Serializable {

  def this(sparkConf: SparkConf) = {
    //this(RedisConfig.arrayAsJavaMap(sparkConf.getAllWithPrefix("jedis")))
    this(RedisConfig.arrayAsJavaMap(sparkConf.getAll))
  }

  val logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  @transient val initiaJedis = JedisSingletonConnHandler.getInstance(props).getRandomJedisPool

  val hosts = {
   val resData = getHosts()
    logger.info("all hosts: " + resData.toList)
    resData
  }
  val nodes = getNodes()

  def connect(re: RedisNode): Jedis = {
    logger.info(s"connect host ${re.host}:${re.port}")
    JedisSingletonConnHandler
      .getInstance(props)
      .getJedisPoolFromNode(re.host, re.port)
      .getResource
  }

  /**
    * @return initialHost's auth
    */
  /*
  def getAuth: String = {
    initialHost.auth
  }
  */

  /**
    * @return selected db number
    */
  /*
  def getDB :Int = {
    initialHost.dbNum
  }
  */

  def getRandomNode(): RedisNode = {
    val rnd = scala.util.Random.nextInt().abs % hosts.length
    hosts(rnd)
  }

  /**
    * @param sPos start slot number
    * @param ePos end slot number
    * @return a list of RedisNode whose slots union [sPos, ePos] is not null
    */
  def getNodesBySlots(sPos: Int, ePos: Int): Array[RedisNode] = {

    logger.info("try to find nodes by slots: " + sPos + " " + ePos)
    logger.info("nodes: ")
    nodes.foreach(redisNode => {
      logger.info("node: " + redisNode.host + ":" + redisNode.port)
    })
    /* This function judges if [sPos1, ePos1] union [sPos2, ePos2] is not null */
    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int) =
      if (sPos1 <= sPos2) ePos1 >= sPos2 else ePos2 >= sPos1

    nodes.filter(node => inter(sPos, ePos, node.startSlot, node.endSlot)).
      filter(_.idx == 0) //master only now
  }

  /**
    * @param key
    * *IMPORTANT* Please remember to close after using
    * @return jedis who is a connection for a given key
    */
  def connectionForKey(key: String): Jedis = {
    val slot = JedisClusterCRC16.getSlot(key)
    JedisSingletonConnHandler.getInstance(props).getJedisPoolFromSlot(slot).getResource
  }

  /**
    * @param initialHost any redis endpoint of a cluster or a single server
    * @return true if the target server is in cluster mode
    */
  //FIXME
  /*
  private def clusterEnabled(initialHost: RedisEndpoint): Boolean = {
    val conn = initialHost.connect()
    val info = conn.info.split("\n")
    val version = info.filter(_.contains("redis_version:"))(0)
    val clusterEnable = info.filter(_.contains("cluster_enabled:"))
    val mainVersion = version.substring(14, version.indexOf(".")).toInt
    val res = mainVersion>2 && clusterEnable.length>0 && clusterEnable(0).contains("1")
    conn.close
    res
  }
  */

  /**
    * @param key
    * @return host whose slots should involve key
    */
  def getHost(key: String): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    hosts.filter(host => {
      host.startSlot <= slot && host.endSlot >= slot
    })(0)
  }


  /**
    * @param initialHost any redis endpoint of a cluster or a single server
    * @return list of host nodes
    */
  private def getHosts(): Array[RedisNode] = {
    getNodes().filter(_.idx == 0)
  }

  /**
    * @param initialHost any redis endpoint of a single server
    * @return list of nodes
    */
  /*
  private def getNonClusterNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    val master = (initialHost.host, initialHost.port)
    val conn = initialHost.connect()

    val replinfo = conn.info("Replication").split("\n")
    conn.close

    // If  this node is a slave, we need to extract the slaves from its master
    if (replinfo.filter(_.contains("role:slave")).length != 0) {
      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt

      //simply re-enter this function witht he master host/port
      getNonClusterNodes(initialHost = new RedisEndpoint(host, port,
        initialHost.auth, initialHost.dbNum))

    } else {
      //this is a master - take its slaves

      val slaves = replinfo.filter(x => (x.contains("slave") && x.contains("online"))).map(rl => {
        val content = rl.substring(rl.indexOf(':') + 1).split(",")
        val ip = content(0)
        val port = content(1)
        (ip.substring(ip.indexOf('=') + 1), port.substring(port.indexOf('=') + 1).toInt)
      })

      val nodes = master +: slaves
      val range = nodes.size
      (0 until range).map(i =>
        //RedisNode(new RedisEndpoint(nodes(i)._1, nodes(i)._2, initialHost.auth, initialHost.dbNum,
        //            initialHost.timeout),
        //  0, 16383, i, range)).toArray

      RedisNode(nodes(i)._1, nodes(i)._2, 0, 16383, i, range)).toArray
    }
  }
  */

  /**
    * @param initialHost any redis endpoint of a cluster server
    * @return list of nodes
    */
  private def getClusterNodes(): Array[RedisNode] = {
    val conn = initiaJedis.getResource
    val res = conn.clusterSlots().flatMap {
      slotInfoObj => {
        val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
        val sPos = slotInfo.get(0).toString.toInt
        val ePos = slotInfo.get(1).toString.toInt
        /*
         * We will get all the nodes with the slots range [sPos, ePos],
         * and create RedisNode for each nodes, the total field of all
         * RedisNode are the number of the nodes whose slots range is
         * as above, and the idx field is just an index for each node
         * which will be used for adding support for slaves and so on.
         * And the idx of a master is always 0, we rely on this fact to
         * filter master.
         */
        (0 until (slotInfo.size - 2)).map(i => {
          val node = slotInfo(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
          val host = SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]])
          val port = node.get(1).toString.toInt
          RedisNode(host, port, sPos, ePos, i, slotInfo.size - 2)
        })
      }
    }.toArray
    conn.close()
    res
  }

  /**
    * @param initialHost any redis endpoint of a cluster or a single server
    * @return list of nodes
    */
  def getNodes(): Array[RedisNode] = {
    getClusterNodes()
    /*FIXME for non cluster
    if (clusterEnabled(initialHost)) {
      getClusterNodes()
    } else {
      getNonClusterNodes(initialHost)
    }
    */
  }
}
