package com.snowflake.snowpark

import com.snowflake.snowpark.functions.{col, udf}
import com.snowflake.snowpark.internal.{UDFClassPath, Utils}
import com.snowflake.snowpark_test.{UDFSuite, UDTFSuite}
import junit.framework.Assert.assertEquals
import junit.framework.TestCase.assertFalse
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, spy, times, verify, when}

import java.sql.SQLException

class UDFInternalSuite extends TestData {

  lazy private val stageName: String = randomName()
  lazy private val newSession = Session.builder.configFile(defaultProfile).create

  override def beforeAll: Unit = {
    super.beforeAll
    createStage(stageName, isTemporary = false)
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session, Some(stageName))
      TestUtils.addDepsToClassPath(newSession, Some(stageName))
    }
  }

  override def afterAll: Unit = {
    dropStage(stageName)
    super.afterAll
  }

  test("Test temp udf not failing back to upload jar", JavaStoredProcExclude) {
    val newSession = Session.builder.configFile(defaultProfile).create
    val mockSession = spy(newSession)
    TestUtils.addDepsToClassPath(mockSession, None, usePackages = true)
    val path = UDFClassPath.getPathForClass(classOf[com.snowflake.snowpark.Session]).get

    val doubleUDF = mockSession.udf.registerTemporary((x: Int) => x + x)
    checkAnswer(
      mockSession
        .createDataFrame(Seq(1, 2))
        .select(doubleUDF(col("value"))),
      Seq(Row(2), Row(4)))
    if (mockSession.isVersionSupportedByServerPackages) {
      verify(mockSession, times(0)).addDependency(path)
    } else {
      verify(mockSession, times(1)).addDependency(path)
    }
    verify(mockSession, times(1)).removeDependency(path)
    verify(mockSession, times(1)).addPackage("com.snowflake:snowpark:latest")
  }

  test("Test permanent udf not failing back to upload jar", JavaStoredProcExclude) {
    val newSession = Session.builder.configFile(defaultProfile).create
    val mockSession = spy(newSession)
    TestUtils.addDepsToClassPath(mockSession, None, usePackages = true)
    val path = UDFClassPath.getPathForClass(classOf[com.snowflake.snowpark.Session]).get

    val permFuncName = randomName()
    val stageName1 = randomName()
    try {
      import functions.callUDF
      val udf = (x: Int) => x + 1
      runQuery(s"create stage $stageName1", session)
      session.udf.registerPermanent(permFuncName, udf, stageName1)
      val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))

      // another session
      val df2 = newSession.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df2.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))

    } finally {
      runQuery(s"drop function if exists $permFuncName(INT)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }

    if (mockSession.isVersionSupportedByServerPackages) {
      verify(mockSession, times(0)).addDependency(path)
    } else {
      verify(mockSession, times(1)).addDependency(path)
    }
    verify(mockSession, times(1)).removeDependency(path)
    verify(mockSession, times(1)).addPackage("com.snowflake:snowpark:latest")
  }

  test("Test add version logic", JavaStoredProcExclude) {
    val newSession = Session.builder.configFile(defaultProfile).create
    val mockSession = spy(newSession)
    TestUtils.addTestDepsToClassPath(mockSession, None)
    when(mockSession.isVersionSupportedByServerPackages).thenReturn(true)
    // It's difficult to mock the result of Utils.clientPackageName. Since the version in the test
    // code might not be available on server, so wrap the udf creation with a try block.
    try {
      mockSession.udf.registerTemporary((x: Int) => x + x)
    } catch {
      case e: SQLException =>
        assert(
          e.getMessage.contains(
            s"SQL compilation error: Package '${Utils.clientPackageName}' is not supported"))
      case _ => fail("Unexpected error from server")
    }
    val path = UDFClassPath.getPathForClass(classOf[com.snowflake.snowpark.Session]).get
    verify(mockSession, never()).addDependency(path)
    verify(mockSession, times(1)).addPackage(Utils.clientPackageName)
  }

  test("Confirm jar files to be uploaded to expected location", JavaStoredProcExclude) {
    val udf = (x: Int) => x + 1
    val tempFuncName = randomFunctionName()
    val quotedTempFuncName = "\"" + randomName() + "\""
    val permFuncName = randomFunctionName()
    val quotedPermFuncName = "\"" + randomName() + "\""
    val stageName = randomStageName()
    try {
      runQuery(s"create stage $stageName", session)

      session.udf.registerTemporary(tempFuncName, udf)
      assert(session.sql(s"ls ${session.getSessionStage}/$tempFuncName/").collect().length == 2)
      session.udf.registerPermanent(permFuncName, udf, stageName)
      assert(session.sql(s"ls @$stageName/$permFuncName/").collect().length == 2)

      session.udf.registerTemporary(quotedTempFuncName, udf)
      assert(
        session.sql(s"ls ${session.getSessionStage}/$quotedTempFuncName/").collect().length == 0)
      assert(
        session
          .sql(s"ls ${session.getSessionStage}/${Utils.getUDFUploadPrefix(quotedTempFuncName)}/")
          .collect()
          .length == 2)
      session.udf.registerPermanent(quotedPermFuncName, udf, stageName)
      assert(session.sql(s"ls @$stageName/$quotedPermFuncName/").collect().length == 0)
      assert(
        session
          .sql(s"ls @$stageName/${Utils.getUDFUploadPrefix(quotedPermFuncName)}/")
          .collect()
          .length == 2)
    } finally {
      session.runQuery(s"drop function if exists $tempFuncName(INT)")
      session.runQuery(s"drop function if exists $permFuncName(INT)")
      session.runQuery(s"drop function if exists $quotedTempFuncName(INT)")
      session.runQuery(s"drop function if exists $quotedPermFuncName(INT)")
      session.runQuery(s"drop stage if exists $stageName")
    }
  }

  test("Scoped temp UDF") {
    import newSession.implicits._
    testWithAlteredSessionParameter({
      // If scoped temp objects are not enabled, skip this test.
      if (newSession.useScopedTempObjects) {
        val df = Seq(1).toDF("a")
        val doubleUDF = newSession.udf.registerTemporary((x: Int) => x + x)
        val df2 = df.select(doubleUDF(col("a")))
        checkAnswer(df2, Seq(Row(2)))
        assertEquals(0, newSession.getTempObjectMap.size)
      }
    }, "snowpark_use_scoped_temp_objects", "true", skipIfParamNotExist = true)
  }

  test("register UDF should not upload duplicated dependencies", JavaStoredProcExclude) {
    val mockSession1 = spy(session)
    val mockSession2 = spy(newSession)
    val stageName1 = randomName()
    val funcName1 = randomName()
    val funcName2 = randomName()
    try {
      session.runQuery(s"create stage $stageName1")
      val udf = (x: Int) => x + 1
      mockSession1.udf.registerPermanent(funcName1, udf, stageName1)
      mockSession2.udf.registerPermanent(funcName2, udf, stageName1)
      verify(mockSession2, never()).doUpload(any(), any())

    } finally {
      session.runQuery(s"drop function if exists $funcName1(INT)")
      session.runQuery(s"drop function if exists $funcName2(INT)")
      session.runQuery(s"drop stage if exists $stageName1")
    }
  }
}

@UDFPackageTest
class PackageUDFSuite extends UDFSuite {
  override def beforeAll: Unit = {
    super.beforeAll()
    val snClassDir = UDFClassPath.getPathForClass(classOf[Session]).get
    session.removeDependency(snClassDir.replace("scoverage-", ""))
    session.removePackage(Utils.clientPackageName)
    session.addPackage("com.snowflake:snowpark:latest")
  }

  override def afterAll: Unit = {
    val snowparkJarPath = new java.io.File(UDFClassPath.snowparkJar.location.get).toURI
    // If package is used, snowpark jar should NOT be added automatically in any scenario
    assert(!session.getDependencies.contains(snowparkJarPath))
    super.afterAll()
  }
}

@UDFPackageTest
class PackageUDTFSuite extends UDTFSuite {
  override def beforeAll: Unit = {
    super.beforeAll()
    val snClassDir = UDFClassPath.getPathForClass(classOf[Session]).get
    session.removeDependency(snClassDir.replace("scoverage-", ""))
    session.removePackage(Utils.clientPackageName)
    session.addPackage("com.snowflake:snowpark:latest")
  }

  override def afterAll: Unit = {
    val snowparkJarPath = new java.io.File(UDFClassPath.snowparkJar.location.get).toURI
    // If package is used, snowpark jar should NOT be added automatically in any scenario
    assert(!session.getDependencies.contains(snowparkJarPath))
    super.afterAll()
  }
}
