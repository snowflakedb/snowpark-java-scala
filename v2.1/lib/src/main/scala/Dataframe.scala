package com.snowflake.snowpark

import scala.language.experimental.macros

class Dataframe(m: Int) {
  def fillna(n: Int): Int = macro DataframeMacro.fillna

  def fillnaImpl(n: Int, src: DataframeMacro.SrcPosition): Int = {
    println(s"src $src")
    m + n
  }
}
