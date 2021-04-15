package com.dahua.bz2parquet

import com.dahua.bean.log
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Bz2parquet2 {
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

    var Array(inputpath,outputpath)=args

    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[log]))
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("yarn").getOrCreate()

    var sc = spark.sparkContext

    val value: RDD[String] = sc.textFile(inputpath)

    val value1: RDD[log] = value.map(_.split(",", -1)).filter(_.length >= 85).map(log(_))
    val frame: DataFrame = spark.createDataFrame(value1)
    frame.write.parquet(outputpath)

    spark.stop()
    sc.stop()



  }
}
