package com.dh.tools

import org.apache.spark.sql.SparkSession

object RedisMapping {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkReadRedis")
      .master("local[*]")
      .config("spark.redis.host","127.0.0.1")
      .config("spark.redis.port", "6379")
      .config("spark.redis.auth","") //指定redis密码
      .config("spark.redis.db","7") //指定redis库
      .getOrCreate()


    val loadedDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .option("key.column", "com.you.ku")
      .load()
    loadedDf.show(false)
  }

}
