package com.dh.analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ProCityCount {


  def main(args: Array[String]): Unit = {
//    // 判断参数。
//    if (args.length != 1) {
//      println(
//        """
//          |com.dahua.analyse.ProCityCount
//          |缺少参数
//          |inputPath
//          """.stripMargin)
//      sys.exit()
//    }

    // 接收参数
//    val Array(inputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).getOrCreate()

    val sc: SparkContext = spark.sparkContext
    // 需求1： 统计各个省份分布情况，并排序。

    // 需求2： 使用RDD方式，完成按照省分区，省内有序。

    // 需求3： 将项目打包，上传到linux。使用yarn_cluster 模式进行提交，并查看UI。

    // 需求4： 使用azkaban ，对两个脚本进行调度。
    // 读取数据源
//    val df: DataFrame = spark.read.parquet(inputPath)
    val rdds:RDD[String] = sc.textFile("hdfs://master:8020/2016-10-01_06_p1_invalid.1475274123982.log")
    // 创建临时视图
    //df.createTempView("log")
    // 编写sql语句
    //val sql = "select provincename,count(*) as pccount from log group by provincename Order By pccount DESC";
    //spark.sql(sql).show(50)



//    rdd_string.foreach(println)

//    ((江苏省,无锡市),1)
//    ((湖南省,长沙市),1)
//    ((广东省,广州市),1)
val citys = rdds.map(x => {
  val lines = x.split(",")
  ((lines(24), lines(25)), 1)
})
//203274

//355
//    val  citys1 = citys.distinct()


    val rdd = citys.map(x=>{
      ((x._1._1),(x._1._2,x._2))
    }).sortBy(_._2._2,false)

    //.reduceByKey(_+_).sortBy(_._2).groupByKey().collect().foreach(println)
    //24省  25市
    rdd.partitionBy(new MyPatition(30)).saveAsTextFile("hdfs://192.168.186.81:8020/sparkrdd")
sc.stop()
   // spark.stop()

  }



}

class MyPatition(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    if (key == "江苏省") 0
    else if (key == "浙江省") 1
    else if (key == "山东省") 2
    else if (key == "广东省") 3
    else if (key == "安徽省") 4
    else if (key == "四川省") 5
    else if (key == "广西壮族自治区") 6
    else if (key == "河北省") 7
    else if (key == "江西省") 8
    else if (key == "陕西省") 9
    else if (key == "湖南省") 10
    else if (key == "湖北省") 11
    else if (key == "福建省") 12
    else if (key == "重庆市") 13
    else if (key == "北京市") 14
    else if (key == "河南省") 15
    else if (key == "辽宁省") 16
    else if (key == "内蒙古自治区") 17
    else if (key == "吉林省") 18
    else if (key == "黑龙江省") 19
    else if (key == "上海市") 20
    else if (key == "山西省") 21
    else if (key == "贵州省") 22
    else if (key == "云南省") 23
    else if (key == "新疆维吾尔自治区") 24
    else if (key == "宁夏回族自治区") 25
    else if (key == "甘肃省") 26
    else if (key == "青海省") 27
    else if (key == "海南省") 28
    else 29
  }
}