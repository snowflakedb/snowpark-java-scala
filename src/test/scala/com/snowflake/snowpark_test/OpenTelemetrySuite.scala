package com.snowflake.snowpark_test

import com.snowflake.snowpark.OpenTelemetryEnabled
import com.snowflake.snowpark.internal.OpenTelemetry

class OpenTelemetrySuite extends OpenTelemetryEnabled {

  test("a") {
    OpenTelemetry.emit("a1", "b2", "c3", 123, "d4")
    checkSpan("a1", "b2", "c3", 123, "d4")
  }

}
