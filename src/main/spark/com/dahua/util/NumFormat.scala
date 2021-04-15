package com.dahua.util

object NumFormat {
  def toInt(string: String)={
    try{
      string.toInt
    }catch {
      case _: Exception =>0
    }
  }
  def toDouble(string: String): Double={
    try{
      string.toDouble
    }catch {
      case _: Exception =>0
    }
  }
}
