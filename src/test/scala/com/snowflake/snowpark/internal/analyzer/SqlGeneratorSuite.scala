package com.snowflake.snowpark.internal.analyzer

import org.scalatest.funsuite.AnyFunSuite

class SqlGeneratorSuite extends AnyFunSuite {

  test("windowFrameBoundary keeps Int.MinValue magnitude non-negative") {
    // Int.MinValue.abs is still negative; SQL must emit a positive PRECEDING offset.
    assert(SqlGenerator.windowFrameBoundary(Int.MinValue.toString) == "2147483648 PRECEDING ")
    assert(SqlGenerator.windowFrameBoundary((-1).toString) == "1 PRECEDING ")
    assert(SqlGenerator.windowFrameBoundary("2") == "2 FOLLOWING ")
    assert(SqlGenerator.windowFrameBoundary("UNBOUNDED PRECEDING") == "UNBOUNDED PRECEDING")
  }
}
