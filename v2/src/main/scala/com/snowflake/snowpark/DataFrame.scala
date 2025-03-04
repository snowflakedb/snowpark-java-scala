package com.snowflake.snowpark

import com.snowflake.snowpark.internal.Utils.withSourcePosition

class DataFrame {
  def func: Int => Int = withSourcePosition (a => a + 1)
}
