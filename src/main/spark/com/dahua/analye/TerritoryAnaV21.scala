package com.dahua.analye

import java.util.Properties

import com.dahua.util.TerritoryTool
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object TerritoryAnaV21 {



  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 2) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
          |outputPath
        """.stripMargin)
      sys.exit()
    }

    // 接收参数
    val Array(inputPath,outputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    // 读取app_mapping.txt 文件。 .collec(). 使用广播变量进行广播。应该是个Map对象。

    val value: RDD[String] = sc.textFile(inputPath)
    val value1: RDD[(String, String)] = value.map(_.split(",", -1)).map((x) => {
      (x(0), x(1))
    })
    
  }

}
