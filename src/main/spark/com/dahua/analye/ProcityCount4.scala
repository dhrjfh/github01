package com.dahua.analye

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProcityCount4 {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println(
        """
          |com.dahua.bz2parquet
          |缺少参数
          |inputpath
          |outputpath
          |""".stripMargin)
      sys.exit()
    }
    val Array(inputPath) = args

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("yarn").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    // 读取数据源
    val df: DataFrame = spark.read.parquet(inputPath)
    // 创建临时视图
    df.createTempView("log")
    // 编写sql语句
    val sql = "select provincename ,cityname, row_number() over(partition by provincename order by cityname) as pcount from log ";
    spark.sql(sql).show(50)



    spark.stop()




  }
}
