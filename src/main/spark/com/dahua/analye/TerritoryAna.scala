package com.dahua.analye

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object TerritoryAna {
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

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    // 读取数据源
    val df: DataFrame = spark.read.parquet(inputPath)
    // 创建临时视图
    df.createTempView("log")

    val sql=
      """
        |select
        |   provincename,
        |   cityname,
        |   sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as yxqq,
        |   sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as yxdqq,
        |   sum(case when requestmode=1 and processnode>=3 then 1 else 0 end) as ggqq
        |from log
        |group by provincename ,cityname
        |order by provincename,yxqq
        |""".stripMargin

    spark.sql(sql).show()
  }
}
