package com.dh.dim

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object ZoneDim {

/*
地狱报表
 */
  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }


    // RDD 序列化到磁盘 worker与worker之间的数据传输
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建sparksession对象
    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._


    //接受参数
    var Array(inputPath) = args

    val df:DataFrame = spark.read.parquet(inputPath)

    //创建表
    val dim:Unit = df.createTempView("dim")

    //sql语句
    val sql =
      """
     select
      |provincename,cityname,
      |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end )as ysqq,
      |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end )as yxqq,
      |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq,
      |sum(case when iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as jjx,
      |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs,
      |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
      |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
      |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
      |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
      |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
      |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
      |from dim
      |group by
      |provincename,cityname
      |
      |""".stripMargin
  val result =spark.sql(sql)

    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user",load.getString("jdbc.user"))
    props.setProperty("password",load.getString("jdbc.password"))
    //将结果存储成json格式
    result.write/*.mode(SaveMode.Append)*/.jdbc(load.getString("jdbc.url"), load.getString("jdbc.arearpt.table"), props)
    spark.stop()
    sc.stop()

  }
}
