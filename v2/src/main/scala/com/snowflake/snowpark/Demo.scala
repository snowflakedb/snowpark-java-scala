package com.snowflake.snowpark
object Demo {
  def main(args: Array[String]): Unit = {
    val df = new DataFrame
    df.select("col1")
  }
}
