package com.dahua.analye

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProcityCount5 {
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

    // 需求3： 使用RDD方式，完成按照省分区，省内有序。

    // 需求4： 将项目打包，上传到linux。使用yarn_cluster 模式进行提交，并查看UI。

    // 需求5： 使用azkaban ，对两个脚本进行调度。

    val value: RDD[String] = sc.textFile(inputPath)
    val value1: RDD[((String, String), Int)] = value.map(_.split(",", -1)).filter(_.length >= 85).map(res => {
      ((res(24), res(25)), 1)
    })
    val value2: RDD[((String, String), Int)] = value1.reduceByKey(_ + _)
//    val value3: RDD[((String, String), Int)] = value2.sortBy(_._2)
 val value3 = value2
    val value4: RDD[(String, (String, Int))] = value3.map(x => {
      (x._1._1, (x._1._2, x._2))
    })
    val strings: Array[String] = value4.map(_._1).collect()
    val value5: RDD[(String, (String, Int))] = value4.partitionBy(new Udfpartiton(strings))
    value5.foreach(x=>{
      println(x._1+"\t"+x._2._1+"\t"+x._2._2)
    })


    spark.stop()




  }
}
