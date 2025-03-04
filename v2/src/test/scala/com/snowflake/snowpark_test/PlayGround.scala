package com.snowflake.snowpark_test

import com.snowflake.snowpark.{DataFrame, UnitTestBase}

class PlayGround extends UnitTestBase {
  test("macro test") {
    new DataFrame().func
  }
}
