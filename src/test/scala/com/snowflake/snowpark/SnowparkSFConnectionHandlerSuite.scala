package com.snowflake.snowpark

import org.scalatest.FunSuite
import com.snowflake.snowpark.internal.SnowparkSFConnectionHandler

class SnowparkSFConnectionHandlerSuite extends FunSuite {

  test("version") {
    assert(SnowparkSFConnectionHandler.extractValidVersionNumber("0.1.0-snapshot").equals("0.1.0"))
    assert(SnowparkSFConnectionHandler.extractValidVersionNumber("0.1.0").equals("0.1.0"))
    assert(SnowparkSFConnectionHandler.extractValidVersionNumber("0.1.0.0").equals("0.1.0.0"))
  }

  test("version negative") {
    val err = intercept[SnowparkClientException](
      SnowparkSFConnectionHandler.extractValidVersionNumber("0.1")
    )
    assert(err.message.contains("Invalid client version string"))
  }

}
