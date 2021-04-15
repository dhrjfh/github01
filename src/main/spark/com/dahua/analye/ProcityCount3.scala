package com.dahua.analye

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProcityCount3 {
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


    val value: RDD[String] = sc.textFile(inputPath)
    val value1: RDD[((String, String), Int)] = value.map(_.split(",", -1)).filter(_.length >= 85).map(fileid => {
      ((fileid(24), fileid(25)), 1)
    })
    val value2: RDD[((String, String), Int)] = value1.reduceByKey(_ + _)
    value2.map(lien=>{
      lien._1._1+"\t"+lien._1._2+"\t"+lien._2
    }).saveAsTextFile(outputPath)


    spark.stop()




  }
}
