package com.snowflake.snowpark

class SrcPositionInfoSuite extends UnitTestBase {

  test("read src position") {
    val dummy = new DummySrcPositionImpl
    val src = dummy.func(1)
    assert(src.filename == "SrcPositionInfoSuite.scala")
  }
}
