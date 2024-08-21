package com.snowflake.snowpark_test

import com.snowflake.snowpark.{SnowparkClientException, UploadTimeoutSession}

class RequestTimeoutSuite extends UploadTimeoutSession {

  // Jar upload timeout is set to 0 second
  test("Test udf jar upload timeout") {
    assertThrows[SnowparkClientException](
      mockSession.udf.registerTemporary((a: Int, b: Int) => a == b))
  }
}
