package com.dahua.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object JedisUtil {
   val pool = new JedisPool(new GenericObjectPoolConfig,"192.168.137.58",6379,30000,null,8)

  def  resource = pool.getResource

  def main(args: Array[String]): Unit = {
    println(resource)
  }

}
