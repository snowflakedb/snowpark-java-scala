package com.snowflake.snowpark.demo

import com.snowflake.snowpark.Dataframe

object Demo {
  def main(args: Array[String]): Int = {
    val df = new Dataframe(42)
    println(df.fillna(378))
    0
  }
}
