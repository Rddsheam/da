package com.dh.dim

import com.dh.bean.LogBean
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ZoneDimForRDDV2 {

  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 3) {
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
    var Array(inputPath,appMapping,outputPath) = args

    //读取映射文件:appmappering
    val appMap = sc.textFile(appMapping).map(line => {
      val arr: Array[String] = line.split("[:]", -1)
      (arr(0), arr(1))
    }).collect().toMap

    //使用广播变量,进行广播
    val appBroadcast = sc.broadcast(appMap)

    val log:RDD[String] =sc.textFile(inputPath)
    val logRdd = log.map(_.split(",", 1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      !t.appid.isEmpty
    })

    logRdd.map(log=>{
      var appname = log.appname
      if(appname=="" || appname.isEmpty){
       appname = appBroadcast.value.getOrElse(log.appid,"不明确")
      }

      val ysqqs:List[Double] = DimZhiBiao.qqsRtp(log.requestmode,log.processnode)
      (appname,ysqqs)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })

log.saveAsTextFile(outputPath)





    sc.stop()
  }

}
