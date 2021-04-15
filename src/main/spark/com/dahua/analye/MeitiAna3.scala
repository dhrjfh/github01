package com.dahua.analye

import com.dahua.bean.log
import com.dahua.util.{JedisUtil, TerritoryTool}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object MeitiAna3 {
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
    val Array(inputPath1,inputPath2) = args

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    // 读取数据源
    val df1: DataFrame = spark.read.parquet(inputPath1)
    df1.createTempView("log")
    val value: RDD[(String, String)] = sc.textFile(inputPath2).map(t => {
      val strings: Array[String] = t.split("[:]")
      (strings(0), strings(1))
    })
    value.toDF("appid","appname").createTempView("zip")

//    spark.sql(
//      """
//        | select
//        |		a.appid,
//        |		a.newname,
//        |        sum(case when requestmode =1 and processnode >=1 then 1 else 0 end )as ysqq,
//        |        sum(case when requestmode =1 and processnode >=2 then 1 else 0 end )as yxqq,
//        |        sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq,
//        |        sum(case when iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as jjx,
//        |        sum(case when iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs,
//        |        sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
//        |        sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
//        |        sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
//        |        sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
//        |        sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
//        |        sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
//        |        from
//        |		(select
//        |			l.*,
//        |			case when l.appid = z.appid then z.appname else l.appname end as newname
//        |		from log l left join zip z
//        |		on l.appid =z.appid)a
//        |		group by
//        |			a.appid,
//        |			a.newname
//        |""".stripMargin).show(2000)



  }
}
