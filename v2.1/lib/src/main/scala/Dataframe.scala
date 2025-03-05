package com.snowflake.snowpark

import scala.language.experimental.macros

class Dataframe(m: Int) {
  def fillna(n: Int)(implicit src: SrcPosition): Int = {
    println(s"fillna src $src")
    m + n
  }

  def summary(stats: List[String])(implicit src: SrcPosition): String = {
    println(s"summary src $src")
    stats mkString ", "
  }
}
