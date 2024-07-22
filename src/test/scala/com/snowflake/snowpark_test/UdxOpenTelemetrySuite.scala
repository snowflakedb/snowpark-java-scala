package com.snowflake.snowpark_test

import com.snowflake.snowpark.types.{IntegerType, StructField, StructType}
import com.snowflake.snowpark.udtf.UDTF0
import com.snowflake.snowpark.{OpenTelemetryEnabled, Row, Session, TestUtils, functions}

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

  test("udtf") {
    val className = "snow.snowpark.UDTFRegistration"
    class MyUDTF0 extends UDTF0 {
      override def process(): Iterable[Row] = {
        Seq(Row(123), Row(123))
      }
      override def endPartition(): Iterable[Row] = Seq.empty
      override def outputSchema(): StructType = StructType(StructField("sum", IntegerType))
    }
    session.udtf.registerTemporary(new MyUDTF0())
    checkUdfSpan(className, "registerTemporary", "", "")
    val udtfName = randomFunctionName()
    session.udtf.registerTemporary(udtfName, new MyUDTF0)
    checkUdfSpan(className, "registerTemporary", udtfName, "")

    val stageName: String = randomName()
    val udtfName2 = randomFunctionName()
    try {
      createStage(stageName, isTemporary = false)
      session.udtf.registerPermanent(udtfName2, new MyUDTF0(), stageName)
      checkUdfSpan(className, "registerPermanent", udtfName2, stageName)
    } finally {
      runQuery(s"drop function $udtfName2()", session)
      dropStage(stageName)
    }
  }

  test("sproc") {
    val className: String = "snow.snowpark.SProcRegistration"
    val spName: String = randomName()
    val stageName: String = randomName()
    val spName1: String = randomName()
    val sproc = (_: Session) => s"SUCCESS"
    session.sproc.registerTemporary(sproc)
    checkUdfSpan(className, "registerTemporary", "", "")
    session.sproc.registerTemporary(spName1, sproc)
    checkUdfSpan(className, "registerTemporary", spName1, "")
    try {
      createStage(stageName, isTemporary = false)
      session.sproc.registerPermanent(spName, sproc, stageName, isCallerMode = true)
      checkUdfSpan(className, "registerPermanent", spName, stageName)
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName ()").show()
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
