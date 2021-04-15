package com.dahua.analye




import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProcityCount2 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println(
        """
          |com.dahua.bz2parquet
          |缺少参数
          |inputpath
          |outputpath
          |""".stripMargin)
      sys.exit()
    }
    val Array(inputPath, outputPath) = args

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext


    val df: DataFrame = spark.read.parquet(inputPath)
    df.createTempView("log")

    val frame: DataFrame = spark.sql("select provincename,cityname,count(*) as pcsum from log  group by provincename,cityname")
    val load= ConfigFactory.load()

    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("password",load.getString("jdbc.password"))
    frame.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

    spark.stop()




  }
}
