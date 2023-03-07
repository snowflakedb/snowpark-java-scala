package com.snowflake.snowpark

import com.snowflake.snowpark.internal.Logging
import org.scalatest.FunSuite

class LoggingSuite extends FunSuite {

  test("log name") {
    val a = new LoggingTestA
    assert(a.getClass.getName.equals(a.name))
    assert(a.name.equals("com.snowflake.snowpark.LoggingSuite$LoggingTestA"))

    assert(LoggingTestB.name.equals("com.snowflake.snowpark.LoggingSuite$LoggingTestB"))
  }

  class LoggingTestA extends Logging {
    val name: String = logName
  }

  object LoggingTestB extends Logging {
    val name: String = logName
  }
}
