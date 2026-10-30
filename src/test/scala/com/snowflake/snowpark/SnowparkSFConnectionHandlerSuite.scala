package com.snowflake.snowpark

import com.snowflake.snowpark.internal.Utils
import org.scalatest.funsuite.AnyFunSuite

class SnowparkSFConnectionHandlerSuite extends AnyFunSuite {

  test("version") {
    assert(Utils.extractValidVersionNumber("0.1.0-snapshot").equals("0.1.0"))
    assert(Utils.extractValidVersionNumber("0.1.0").equals("0.1.0"))
    assert(Utils.extractValidVersionNumber("0.1.0.0").equals("0.1.0.0"))
  }

  test("version negative") {
    val err = intercept[SnowparkClientException](Utils.extractValidVersionNumber("0.1"))
    assert(err.message.contains("Invalid client version string"))
  }

}
