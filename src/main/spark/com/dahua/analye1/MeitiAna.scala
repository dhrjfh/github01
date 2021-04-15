package com.dahua.analye1

import com.dahua.analye1
import com.dahua.bean.log
import com.dahua.util.{JedisUtil, TerritoryTool}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object MeitiAna {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
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
    val Array(inputPath) = args

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    // 读取数据源

    val value: RDD[String] = sc.textFile(inputPath)

    val value1: RDD[log] = value.map(_.split(",", -1)).filter(_.length >= 85).map(log(_))


    value1.map(line => {
      val str: String = MEUtil.getid(line)
//      val tuple: (String, String) = MEUtil.getggwlx(line)
//      val str1: String = MEUtil.getappname(line)
//      val str2: String = MEUtil.getqd(line)
//      val str3: String = MEUtil.getczxt(line)
//      val str4: String = MEUtil.getlwf(line)
//      val str5: String = MEUtil.getyys(line)
//      (str, ((tuple), str1, str2, str3, str4, str5))

      ((str,line.adspacetype,line.adspacetypename,line.appname,line.client,line.adplatformproviderid,line.ispid,line.ispname,line.networkmannerid,line.networkmannername),1)


    }).reduceByKey(_+_).map(line=>{


      line._1._1+"\t"+MEUtil.getggwlx(line._1._2,line._1._3,line._2)+"\t"+MEUtil.getappname(line._1._4,line._2)+"\t"+
      MEUtil.getqd(line._1._6,line._2)+"\t"+line._1._5+"\t"+MEUtil.getyys(line._1._7,line._1._8,line._2)+"\t"+MEUtil.getlwf(line._1._9,line._1._10,line._2)
    }).foreach(println)
  }
}
