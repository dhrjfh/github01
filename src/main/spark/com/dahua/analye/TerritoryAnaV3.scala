package com.dahua.analye

import com.dahua.bean.log
import com.dahua.util.TerritoryTool
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TerritoryAnaV3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
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

    import spark.implicits._


    val value: RDD[log] = sc.textFile(inputPath).map(_.split(",", -1)).map(log(_))

    value.map(log=>{
      val qqs: List[Double] = TerritoryTool.qqRtp(log.requestmode, log.processnode)

    })




  }







}
