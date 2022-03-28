package com.dh.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisUtil {

  // 获得连接
  private val jedisPool = new JedisPool(new GenericObjectPoolConfig,"127.0.0.1",6379,30000,null,7)

  def getJedis = jedisPool.getResource
}
