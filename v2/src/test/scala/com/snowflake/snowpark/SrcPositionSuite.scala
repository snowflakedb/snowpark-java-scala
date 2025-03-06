package com.snowflake.snowpark

class SrcPositionSuite extends UnitTestBase {

  test("read src position") {
    val dummy = new DummySrcPositionImpl
    val src = dummy.func(1)
    assert(src.filename == "SrcPositionSuite.scala")
  }
}
