package com.dh.mysql

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

object ProCityRptMysql {

  def main(args: Array[String]): Unit = {
      // 0 校验参数个数
      if (args.length != 1) {
        println(
          """
            |cn.dmp.report.ProCityRpt
            |参数：
            | logInputPath
        """.stripMargin)
        sys.exit()
      }

      // 1 接受程序参数
      val Array(logInputPath) = args

      // 2 创建sparkconf->sparkContext
      val sparkConf = new SparkConf()
      sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      sparkConf.setMaster("local[*]")

      // RDD 序列化到磁盘 worker与worker之间的数据传输
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(sparkConf)

      val sqlc = new SQLContext(sc)
      val df = sqlc.read.parquet(logInputPath)
      df.registerTempTable("log")
      // 按照省市进行分组聚合---》统计分组后的各省市的日志记录条数
      df.show()
      val result = sqlc.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

      /*
      加载配置文件 加载顺序 application.conf -> application.json --> application.properties
       */
      val load = ConfigFactory.load()
      val props = new Properties()
      props.setProperty("user",load.getString("jdbc.user"))
      props.setProperty("password",load.getString("jdbc.password"))
      //将结果存储成json格式
      result.write/*.mode(SaveMode.Append)*/.jdbc(load.getString("jdbc.url"), load.getString("jdbc.tableName"), props)
      sc.stop()
    }

  }

