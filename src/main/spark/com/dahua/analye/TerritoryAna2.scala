package com.dahua.analye

import com.dahua.util.NumFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TerritoryAna2 {
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
    val value: RDD[String] = sc.textFile(inputPath)
    val value1: RDD[(String, String, String, String)] = value.map(_.split(",", -1)).filter(_.length >= 85).map(res => {
      (res(8), res(35), res(24), res(25))
    })

    val value2: RDD[((String, String), List[Int])] = value1.map(x => {
      if (NumFormat.toInt(x._1) == 1 && NumFormat.toInt(x._2) == 1) {
        ((x._3, x._4), List(1, 0,0))
      }else if(NumFormat.toInt(x._1) == 1 && NumFormat.toInt(x._2) == 2){
        ((x._3, x._4), List(1, 1,0))
      }
      else {
        ((x._3, x._4), List(1,1,1))
      }
    })
    value2.reduceByKey((list1,list2)=>{
      val tuples: List[(Int, Int)] = list1.zip(list2)
      tuples.map(s=>{s._1+s._2})
    }).foreach(println)


  }
}
