package com.snowflake.snowpark_test

import com.snowflake.snowpark.OpenTelemetryEnabled
import com.snowflake.snowpark.internal.OpenTelemetry

class OpenTelemetrySuite extends OpenTelemetryEnabled {

  test("OpenTelemetry.emit") {
    OpenTelemetry.emit("ClassA", "functionB", "fileC", 123, "chainD")
    checkSpan("snow.snowpark.ClassA", "functionB", "fileC", 123, "chainD")
  }

}
