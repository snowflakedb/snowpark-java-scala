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
