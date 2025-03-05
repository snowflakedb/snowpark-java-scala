package com.snowflake.snowpark

import com.snowflake.snowpark.internal.SrcPosition

class DummySrcPositionImpl {

  def func(num: Int)(implicit src: SrcPosition): SrcPosition = src

}
