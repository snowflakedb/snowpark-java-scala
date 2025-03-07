package com.snowflake.snowpark

import com.snowflake.snowpark.internal.SrcPositionInfo

class DummySrcPositionImpl {

  def func(num: Int)(implicit src: SrcPositionInfo): SrcPositionInfo = src

}
