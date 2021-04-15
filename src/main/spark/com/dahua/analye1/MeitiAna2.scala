package com.dahua.analye1

import com.dahua.bean.log
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MeitiAna2 {
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
    import spark.implicits._
    val value: RDD[String] = sc.textFile(inputPath)

    val value1: RDD[log] = value.map(_.split(",", -1)).filter(_.length >= 85).map(log(_))
    val df: DataFrame = value1.toDF()


    df.createTempView("log")

    spark.sql(
      """
        |select
        |			a.newid,
        |			concat('LC',case when adspacetype<10 then concat(0,adspacetype) else adspacetype end ,'->',count(adspacetype))as gglx,
        |			concat('LN',case when adspacetype<10 then concat(0,adspacetype) else adspacetype end,adspacetypename,'->',count(adspacetypename))as ggname,
        |			concat('APP名称',appname,'->',count(appname))as appname,
        |			concat('CN',adplatformproviderid,'->',count(adplatformproviderid))as qudao,
        |			concat('操作系统',client)as czxt,
        |			concat('运营商',ispid,ispname,count(ispid))as yys,
        |			concat('联网方式',networkmannerid,networkmannername)as lwfs
        |
        |		from
        |			(select
        |				case when imei !='' then imei
        |					when mac  !='' then mac
        |					when idfa !='' then idfa
        |					when openudid  !='' then openudid
        |					when androidid !='' then androidid
        |					else   'king' end as newid,
        |				adspacetype,
        |				adspacetypename,
        |				appname,
        |				client,
        |				adplatformproviderid,
        |				ispid,
        |				networkmannerid,
        |				networkmannername,
        |				ispname
        |
        |			from log)a
        |			group by 	newid,
        |						adspacetype,
        |						adspacetypename,
        |						appname,
        |						client,
        |						adplatformproviderid,
        |						ispid,
        |						networkmannerid,
        |						networkmannername,
        |						ispname
        |""".stripMargin).show(2000)



  }
}
