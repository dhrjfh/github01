package com.dahua.analye

import com.dahua.bean.log
import com.dahua.util.{JedisUtil, TerritoryTool}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object MeitiAna2 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println(
        """
          |com.dahua.bz2parquet
          |缺少参数
          |inputpath
          |apppath
          |outputpath
          |""".stripMargin)
      sys.exit()
    }
    val Array(inputPath,outputpath) = args

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    // 读取数据源




    val value: RDD[String] = sc.textFile(inputPath)

    val value1: RDD[(String, List[Double])] = value.map(_.split(",", -1)).filter(_.length >= 85).map(log(_)).mapPartitions(iterat => {
      val tuples = new ListBuffer[(String, List[Double])]
      val resource: Jedis = JedisUtil.resource

      iterat.foreach(line => {
        val doubles: List[Double] = TerritoryTool.qqRtp(line.requestmode, line.processnode)
        var appname = line.appname
        if (appname.isEmpty) {
          appname = resource.get(line.appid)
        }
        tuples += ((appname, doubles))
      })
      resource.close()
      tuples.iterator
    })
    value1.reduceByKey((list1,list2)=>{
      val tuples: List[(Double, Double)] = list1.zip(list2)
      tuples.map(t=>t._2+t._1)
    }).map(t=>t._1+"\t"+t._2.mkString(",")).saveAsTextFile(outputpath)




  }
}
