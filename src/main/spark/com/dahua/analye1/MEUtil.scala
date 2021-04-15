package com.dahua.analye1

import com.dahua.bean.log

object MEUtil {
  def getid(log:log)={
    if(!log.imei.isEmpty){
      log.imei
    }else if(!log.mac.isEmpty){
      log.mac
    }else if (!log.idfa.isEmpty){
      log.idfa
    }else if (! log.openudid.isEmpty){
      log.openudid
    }else if (! log.androidid.isEmpty){
      log.androidid
    }else{
      "king"
    }
  }

  def getggwlx(log:log)={
    val ggwlx =log.adspacetype
    val ggwname = log.adspacetypename
    var ggwlx1=""
    var ggwname1=""
    if(ggwlx.toInt<10){
      ggwlx1="LC"+0+ggwlx
      ggwname1= "LN"+0+ggwlx+ggwname
    }else{
      ggwlx1="LC"+ggwlx
      ggwname1= "LN"+ggwlx+ggwname
    }
    ((ggwlx1+"->"+1),(ggwname1+"->"+1))

  }
  def getggwlx(Str:Int,string:String,i:Int)={
    val ggwlx =Str
    val ggwname = string
    var ggwlx1=""
    var ggwname1=""
    if(ggwlx.toInt<10){
      ggwlx1="LC"+0+ggwlx
      ggwname1= "LN"+0+ggwlx+ggwname
    }else{
      ggwlx1="LC"+ggwlx
      ggwname1= "LN"+ggwlx+ggwname
    }
    ((ggwlx1+"->"+1),(ggwname1+"->"+1))


  }

  def getappname (log:log)={
    "App"+log.appname+"->"+1
  }
  def getappname (str:String,i:Int)={
    "App"+str+"->"+i
  }

  def getqd (log:log)={
    "CN"+log.adplatformproviderid+"->"+1
  }
  def getqd (str:Int,i:Int)={
    "CN"+str+"->"+i
  }
  def getczxt (log:log)={
    "操作系统:"+log.osversion+"->"+1
  }
  def getczxt (str:String,i:Int)={
    "操作系统"+str+"->"+i
  }
  def getlwf (log:log)={
    "联网方式:"+log.networkmannerid+"\t"+log.networkmannername+"->"+1
  }
  def getlwf (str:Int,string: String,i:Int)={
    "联网方式:"+str+"\t"+string+"->"+i
  }
  def getyys(log:log)={
    "运营商:"+log.ispid+"\t"+log.ispname+"->"+1
  }
  def getyys(str:Int,string: String,i:Int)={
    "运营商:"+str+"\t"+string+"->"+1
  }
}
