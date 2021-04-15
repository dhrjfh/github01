package com.dahua.analye



import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProcityCount {
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

    val frame: DataFrame = spark.sql("select provincename,cityname,count(*) from log group by provincename,cityname")

    val configuration: Configuration = sc.hadoopConfiguration
    val system: FileSystem = FileSystem.get(configuration)
    val path = new Path(outputPath)
    if(system.exists(path)){
      system.delete(path,true)
    }
    frame.write.partitionBy("provincename","cityname").json(outputPath)
    spark.stop()




  }
}
