package com.dahua.analye

import com.dahua.bean.log
import com.dahua.util.TerritoryTool
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MeitiAna {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println(
        """
          |com.dahua.bz2parquet
          |缺少参数
          |inputpath
          |apppath
          |outputpath
          |""".stripMargin)
      sys.exit()
    }
    val Array(inputPath,apppath,outputpath) = args

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    // 读取数据源

    // 需求3： 使用RDD方式，完成按照省分区，省内有序。

    // 需求4： 将项目打包，上传到linux。使用yarn_cluster 模式进行提交，并查看UI。

    // 需求5： 使用azkaban ，对两个脚本进行调度。

    val value1: RDD[String] = sc.textFile(apppath)
    val map1: Map[String, String] = value1.map(line => {
      val strings: Array[String] = line.split("[:]")

      (strings(0), strings(1))
    }).collect().toMap
    val broadcase: Broadcast[Map[String, String]] = sc.broadcast(map1)


    val value: RDD[String] = sc.textFile(inputPath)
    val value2: RDD[(String, List[Double])] = value.map(_.split(",", -1)).filter(_.length >= 85).map(res => {
      log(res)
    }).map(log => {
      val qqR: List[Double] = TerritoryTool.qqRtp(log.requestmode, log.processnode)
      var appname: String = log.appname
      if (appname.isEmpty) {
        appname = broadcase.value.getOrElse(log.appid, "不明确")
      }
      (appname, qqR)

    })
    val value3: RDD[(String, List[Double])] = value2.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
      value3.saveAsTextFile(outputpath)

    spark.stop()




  }
}
