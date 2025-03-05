package com.snowflake.snowpark

import com.snowflake.snowpark.proto.ast.SrcPosition

class DataFrame {
  def exampleProperty(implicit src: SrcPosition): Int = {
    println(s"exampleProperty src $src")
    42
  }
}
