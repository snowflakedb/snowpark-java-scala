package com.snowflake.snowpark_test

import com.snowflake.snowpark.{OpenTelemetryEnabled, TestUtils, functions}

class UdxOpenTelemetrySuite extends OpenTelemetryEnabled {
  override def beforeAll: Unit = {
    super.beforeAll
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session)
    }
  }

  test("udf") {
    val className = "snow.snowpark.UDFRegistration"
    val func = () => 100
    session.udf.registerTemporary(func)
    checkUdfSpan(className, "registerTemporary", "", "")
    val udfName = randomName()
    session.udf.registerTemporary(udfName, func)
    checkUdfSpan(className, "registerTemporary", udfName, "")
    functions.udf(func)
    checkUdfSpan("snow.snowpark.functions", "udf", "", "")
    val stageName = randomName()
    val udfName2 = randomFunctionName()
    try {
      createStage(stageName, isTemporary = false)
      session.udf.registerPermanent(udfName2, func, stageName)
      checkUdfSpan(className, "registerPermanent", udfName2, stageName)
    } finally {
      runQuery(s"drop function $udfName2()", session)
      dropStage(stageName)
    }
  }

  def checkUdfSpan(
      className: String,
      funcName: String,
      execName: String,
      execFilePath: String): Unit = {
    val stack = Thread.currentThread().getStackTrace
    val file = stack(2) // this file
    checkSpan(
      className,
      funcName,
      "UdxOpenTelemetrySuite.scala",
      file.getLineNumber - 1,
      execName,
      "SnowUDF.compute",
      execFilePath)
  }
}
