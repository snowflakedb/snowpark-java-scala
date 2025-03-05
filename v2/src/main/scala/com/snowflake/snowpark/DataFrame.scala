package com.snowflake.snowpark

import com.snowflake.snowpark.internal.SrcPosition

class DataFrame {
  def select(col: String)(implicit src: SrcPosition): Int = {
    println(s"exampleProperty src $src")
    42
  }
}
