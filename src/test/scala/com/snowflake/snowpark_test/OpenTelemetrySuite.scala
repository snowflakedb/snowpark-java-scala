package com.snowflake.snowpark_test

import com.snowflake.snowpark.OpenTelemetryEnabled
import com.snowflake.snowpark.internal.OpenTelemetry

class OpenTelemetrySuite extends OpenTelemetryEnabled {
  // do not add test before line number tests
  // it verifies code line numbers
  test("line number - collect") {
    session.sql("select 1").collect()
    checkSpan("snow.snowpark.DataFrame", "collect", "OpenTelemetrySuite.scala", 10, "")
  }

  test("line number - randomSplit") {
    session.sql("select * from values(1),(2),(3) as t(num)").randomSplit(Array(0.5, 0.5))
    checkSpan("snow.snowpark.DataFrame", "randomSplit", "OpenTelemetrySuite.scala", 15, "")
  }

  test("line number - first") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.first()
    checkSpan("snow.snowpark.DataFrame", "first", "OpenTelemetrySuite.scala", 21, "")
    df.first(2)
    checkSpan("snow.snowpark.DataFrame", "first", "OpenTelemetrySuite.scala", 23, "")
  }

  test("line number - cacheResult") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.cacheResult()
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "OpenTelemetrySuite.scala", 29, "")
  }

  test("line number - toLocalIterator") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.toLocalIterator
    checkSpan("snow.snowpark.DataFrame", "toLocalIterator", "OpenTelemetrySuite.scala", 35, "")
  }

  test("line number - count") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.count()
    checkSpan("snow.snowpark.DataFrame", "count", "OpenTelemetrySuite.scala", 41, "")
  }

  test("OpenTelemetry.emit") {
    OpenTelemetry.emit("ClassA", "functionB", "fileC", 123, "chainD")
    checkSpan("snow.snowpark.ClassA", "functionB", "fileC", 123, "chainD")
  }

  test("report error") {
    val error = new Exception("test")
    OpenTelemetry.reportError("ClassA1", "functionB1", error)
    checkSpanError("snow.snowpark.ClassA1", "functionB1", error)
  }
}
