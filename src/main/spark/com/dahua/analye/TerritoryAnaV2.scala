package com.dahua.analye


import com.dahua.util.TerritoryTool
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object TerritoryAnaV2 {
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
    val df: DataFrame = spark.read.parquet(inputPath)
    import spark.implicits._
    val rdd: Dataset[((String, String), List[Double])] = df.map(row => {
      // 获取列。
      val requestMode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val appname: String = row.getAs[String]("appname")

      // 获取各个指标。
      val qqs: List[Double] = TerritoryTool.qqRtp(requestMode, processNode)
      val jingjia: List[Double] = TerritoryTool.jingjiaRtp(iseffective, isbilling, isbid, iswin, adorderid)
      val ggz: List[Double] = TerritoryTool.ggzjRtp(requestMode, iseffective)
      val mj: List[Double] = TerritoryTool.mjjRtp(requestMode, iseffective, isbilling)
      val ggc: List[Double] = TerritoryTool.ggcbRtp(iseffective, isbilling, iswin, winprice, adpayment)

      // 从广播变量中获取值。如果为空。替换成新的appname
      ((province, cityname), qqs ++ jingjia ++ ggz ++ mj ++ ggc)
    })
    rdd.rdd.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreach(println(_))
  }







}
