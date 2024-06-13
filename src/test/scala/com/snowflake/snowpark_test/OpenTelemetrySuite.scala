package com.snowflake.snowpark_test

import com.snowflake.snowpark.SNTestBase
import com.snowflake.snowpark.internal.OpenTelemetry

class OpenTelemetrySuite extends SNTestBase {

  test("a") {
    OpenTelemetry.test()
  }

}
