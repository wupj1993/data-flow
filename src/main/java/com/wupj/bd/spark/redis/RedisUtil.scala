package com.wupj.bd.spark.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

class RedisUtil(host: String, port: Int, timeOut: Int, password: String) {

  def getRedis(): JedisPool = {
    val jedis = new JedisPool(new GenericObjectPoolConfig(), host, port, timeOut, password)
    jedis
  }

  def returnResource(pool: JedisPool): Unit = {
    pool.getResource.close();
  }
}
