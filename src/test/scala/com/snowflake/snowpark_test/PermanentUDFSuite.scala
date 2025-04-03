package com.snowflake.snowpark_test

import com.snowflake.snowpark.TestUtils.{removeFile, writeFile}
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.SnowflakeSQLException
import java.io.FileOutputStream
import java.math.RoundingMode
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import java.util.TimeZone
import java.util.jar.JarOutputStream

@UDFTest
class PermanentUDFSuite extends TestData {

  // session to verify permanent udf
  lazy private val newSession = Session.builder.configFile(defaultProfile).create
  lazy private val stageName: String = randomName()

  // create test target directory
  private val tempDirectory1 = Files.createTempDirectory("snowpark_test_target_").toFile
  private val tempDirectory2 = Files.createTempDirectory("snowpark_test_target_").toFile

  override def beforeAll: Unit = {
    super.beforeAll
    createStage(stageName, isTemporary = false)
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session, Some(stageName))
      // In stored procs mode, there is only one session
      TestUtils.addDepsToClassPath(newSession, Some(stageName))
    }
  }

  override def afterAll: Unit = {
    dropStage(stageName)
    removeFile(tempDirectory1.getAbsolutePath, session)
    removeFile(tempDirectory2.getAbsolutePath, session)
    super.afterAll
  }

  test("mix temporary and permanent UDF", JavaStoredProcExclude) {
    import functions.callUDF
    val udf = (x: Int) => x + 1
    val tempFuncName = randomName()
    val permFuncName = randomName()
    val stageName1 = randomName()
    try {
      runQuery(s"create stage $stageName1", session)
      session.udf.registerTemporary(tempFuncName, udf)
      session.udf.registerPermanent(permFuncName, udf, stageName1)
      val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df.select(callUDF(tempFuncName, df("a"))), Seq(Row(2), Row(3)))
      checkAnswer(df.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))

      // another session
      val df2 = newSession.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df2.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))
      assertThrows[SnowflakeSQLException](df2.select(callUDF(tempFuncName, df("a"))).collect())

    } finally {
      runQuery(s"drop function if exists $tempFuncName(INT)", session)
      runQuery(s"drop function if exists $permFuncName(INT)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("permanent UDF with stage + prefix", JavaStoredProcExclude) {
    import functions.callUDF
    val udf = (x: Int) => x + 1
    val permFuncName = randomName()
    val permFuncName2 = randomName()
    val stageName1 = randomName()
    try {
      runQuery(s"create stage $stageName1", session)
      session.udf.registerPermanent(permFuncName, udf, s"$stageName1/prefix")
      session.udf.registerPermanent(permFuncName2, udf, s"$stageName1/prefix")
      val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))
      checkAnswer(df.select(callUDF(permFuncName2, df("a"))), Seq(Row(2), Row(3)))

      // another session
      val df2 = newSession.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df2.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))
      checkAnswer(df2.select(callUDF(permFuncName2, df("a"))), Seq(Row(2), Row(3)))
    } finally {
      runQuery(s"drop function if exists $permFuncName(INT)", session)
      runQuery(s"drop function if exists $permFuncName2(INT)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  // Owner mode has restrictions on temp object names
  test("test valid quoted function Name", JavaStoredProcExcludeOwner) {
    import functions.callUDF
    val udf = (x: Int) => x + 1
    val stageName1 = randomStageName()
    // Currently, JAVA UDF doesn't support function name to have any special characters
    // even the name is quoted. This test can be updated to test quoted name with
    // special characters in the future by resetting specialChars like below:
    // val specialChars = " .!| @$#,,\"\""
    val specialChars = "quoted_name"
    val tempFuncName = "\"" + randomName() + specialChars + "\""
    val permFuncName = "\"" + specialChars + randomName() + "\""
    try {
      runQuery(s"create stage $stageName1", session)
      val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      session.udf.registerTemporary(tempFuncName, udf)
      session.udf.registerPermanent(permFuncName, udf, stageName1)
      checkAnswer(df.select(callUDF(tempFuncName, df("a"))), Seq(Row(2), Row(3)))
      checkAnswer(df.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))
      runQuery(s"drop function $tempFuncName(INT)", session)
      runQuery(s"drop function $permFuncName(INT)", session)
    } finally {
      runQuery(s"drop function if exists $tempFuncName(INT)", session)
      runQuery(s"drop function if exists $permFuncName(INT)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("Support fully qualified udf name", JavaStoredProcExclude) {
    import functions.callUDF
    val udf = (x: Int) => x + 1
    val tempFuncName = session.getFullyQualifiedCurrentSchema + "." + randomName()
    val permFuncName = session.getFullyQualifiedCurrentSchema + "." + randomName()
    val stageName1 = randomName()
    try {
      runQuery(s"create stage $stageName1", session)
      session.udf.registerTemporary(tempFuncName, udf)
      session.udf.registerPermanent(permFuncName, udf, stageName1)
      val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df.select(callUDF(tempFuncName, df("a"))), Seq(Row(2), Row(3)))
      checkAnswer(df.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))

      // another session
      val df2 = newSession.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df2.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))
      assertThrows[SnowflakeSQLException](df2.select(callUDF(tempFuncName, df("a"))).collect())
    } finally {
      runQuery(s"drop function if exists $tempFuncName(INT)", session)
      runQuery(s"drop function if exists $permFuncName(INT)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("negative test invalid permanent function name") {
    val udf = (x: Int) => x + 1
    val stageName1 = randomName()
    val invalidFunctionNames = Seq("testFunction ", " testFunction", "test Function")
    invalidFunctionNames.foreach { functionName =>
      val ex = intercept[SnowparkClientException] {
        session.udf.registerPermanent(functionName, udf, stageName1)
      }
      assert(ex.getMessage.matches(".*The object name .* is invalid."))
    }
  }

  test("Clean up uploaded jar files if UDF registration fails", JavaStoredProcExclude) {
    val udf = (x: Int) => x + 1
    val permFuncName = randomName()
    val stageName1 = randomName()
    try {
      runQuery(s"create stage $stageName1", session)
      session.udf.registerPermanent(permFuncName, udf, stageName1)
      // Before the 2nd registration, one jar has been uploaded to @$stageName1/$permFuncName/
      // udfJar_xxxx_closure.jar, which contains all dependencies & potentially large
      // closure. And another jar is generated by inline compilation udfJar_xxxx.jar.
      assert(session.sql(s"ls @$stageName1/$permFuncName/").collect().length == 2)
      // Register the same name UDF, CREATE UDF will fail.
      val ex = intercept[SnowflakeSQLException] {
        session.udf.registerPermanent(permFuncName, udf, stageName1)
      }
      assert(ex.getMessage.startsWith("SQL compilation error:"))
      // Without clean up, below LIST gets 4 files.
      assert(session.sql(s"ls @$stageName1/$permFuncName/").collect().length == 2)
    } finally {
      runQuery(s"drop function if exists $permFuncName(INT)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("Clean up uploaded jar files if UDF registration fails: case 2") {
    val udf = (s: String) => s
    val permFuncName = randomName()
    val stageName1 = randomName()
    val prefix = randomName()
    try {
      runQuery(s"create stage $stageName1", session)
      // Create a same name in-line JAVA UDF with SQL
      runQuery(
        s"""create or replace function $permFuncName(x varchar)
                         |returns varchar
                         |language java
                         |called on null input
                         |handler='TestFunc.echo_varchar'
                         |target_path='@$stageName1/$prefix/testfunc.jar'
                         |as
                         |'class TestFunc {
                         |  public static String echo_varchar(String x) {
                         |    return x;
                         |  }
                         |}'""".stripMargin,
        session)
      // Before the UDF registration, there is no file in @$stageName1/$permFuncName/
      assert(session.sql(s"ls @$stageName1/$permFuncName/").collect().length == 0)
      // The same name UDF registration will fail.
      val ex = intercept[SnowflakeSQLException] {
        session.udf.registerPermanent(permFuncName, udf, stageName1)
      }
      assert(ex.getMessage.startsWith("SQL compilation error:"))
      // Without clean up, below LIST gets 1 file.
      assert(session.sql(s"ls @$stageName1/$permFuncName/").collect().length == 0)
    } finally {
      runQuery(s"drop function $permFuncName(STRING)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("register permanent: 1 argument", JavaStoredProcExclude) {
    import functions.callUDF
    val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
    val func = (x: Int) => x + 1
    val funcName: String = randomName()
    val newDf = newSession.createDataFrame(Seq(1, 2)).toDF(Seq("a"))

    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(df.select(callUDF(funcName, df("a"))), Seq(Row(2), Row(3)))
      checkAnswer(newDf.select(callUDF(funcName, newDf("a"))), Seq(Row(2), Row(3)))
    } finally {
      runQuery(s"drop function $funcName(INT)", session)
    }
  }
  /* This script generates the tests below
      (2 to 22).foreach { x =>
      val data1 = (1 to x).mkString("(", ",", ")")
      val data2 = (1 to x).map(_ + 10).mkString("(", ",", ")")
      val colNames = (1 to x).map(i => s""""a$i"""").mkString("Seq(", ",", ")")
      val funArg = (1 to x).map(i => s"a$i: Int").mkString("(", ",", ")")
      val funBody = (1 to x).map(i => s"a$i").mkString("+")
      val col1 = (1 to x).map(i => s"""df("a$i")""").mkString(",")
      val col2 = (1 to x).map(i => s"""newDf("a$i")""").mkString(",")
      val result1 = (1 to x).sum
      val result2 = result1 + 10 * x
      val ints = (1 to x).map(_ => "INT").mkString(",")
      println(s"""
        |test("register permanent: $x arguments", JavaStoredProcExclude) {
        |  // scalastyle:off
        |  import functions.callUDF
        |  val df = session.createDataFrame(Seq($data1, $data2)).toDF($colNames)
        |  val func = $funArg => $funBody
        |  val funcName: String = randomName()
        |  val newDf = newSession.createDataFrame(Seq($data1, $data2)).toDF($colNames)
        |  try {
        |    session.udf.registerPermanent(funcName, func, stageName)
        |    checkAnswer(
        |      df.select(callUDF(funcName, $col1)),
        |      Seq(Row($result1), Row($result2)),
        |      sort = false)
        |    checkAnswer(
        |      newDf.select(callUDF(funcName, $col2)),
        |      Seq(Row($result1), Row($result2)),
        |      sort = false)
        |  } finally {
        |    runQuery(s"drop function $$funcName($ints)", session)
        |  }
        |  // scalastyle:on
        |}""".stripMargin)
    }
   */

  test("register permanent: 2 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session.createDataFrame(Seq((1, 2), (11, 12))).toDF(Seq("a1", "a2"))
    val func = (a1: Int, a2: Int) => a1 + a2
    val funcName: String = randomName()
    val newDf = newSession.createDataFrame(Seq((1, 2), (11, 12))).toDF(Seq("a1", "a2"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(df.select(callUDF(funcName, df("a1"), df("a2"))), Seq(Row(3), Row(23)))
      checkAnswer(newDf.select(callUDF(funcName, newDf("a1"), newDf("a2"))), Seq(Row(3), Row(23)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 3 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session.createDataFrame(Seq((1, 2, 3), (11, 12, 13))).toDF(Seq("a1", "a2", "a3"))
    val func = (a1: Int, a2: Int, a3: Int) => a1 + a2 + a3
    val funcName: String = randomName()
    val newDf =
      newSession.createDataFrame(Seq((1, 2, 3), (11, 12, 13))).toDF(Seq("a1", "a2", "a3"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(callUDF(funcName, df("a1"), df("a2"), df("a3"))),
        Seq(Row(6), Row(36)))
      checkAnswer(
        newDf.select(callUDF(funcName, newDf("a1"), newDf("a2"), newDf("a3"))),
        Seq(Row(6), Row(36)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 4 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(Seq((1, 2, 3, 4), (11, 12, 13, 14)))
      .toDF(Seq("a1", "a2", "a3", "a4"))
    val func = (a1: Int, a2: Int, a3: Int, a4: Int) => a1 + a2 + a3 + a4
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(Seq((1, 2, 3, 4), (11, 12, 13, 14)))
      .toDF(Seq("a1", "a2", "a3", "a4"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(callUDF(funcName, df("a1"), df("a2"), df("a3"), df("a4"))),
        Seq(Row(10), Row(50)))
      checkAnswer(
        newDf.select(callUDF(funcName, newDf("a1"), newDf("a2"), newDf("a3"), newDf("a4"))),
        Seq(Row(10), Row(50)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 5 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(Seq((1, 2, 3, 4, 5), (11, 12, 13, 14, 15)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5"))
    val func = (a1: Int, a2: Int, a3: Int, a4: Int, a5: Int) => a1 + a2 + a3 + a4 + a5
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(Seq((1, 2, 3, 4, 5), (11, 12, 13, 14, 15)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(callUDF(funcName, df("a1"), df("a2"), df("a3"), df("a4"), df("a5"))),
        Seq(Row(15), Row(65)))
      checkAnswer(
        newDf.select(
          callUDF(funcName, newDf("a1"), newDf("a2"), newDf("a3"), newDf("a4"), newDf("a5"))),
        Seq(Row(15), Row(65)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 6 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6), (11, 12, 13, 14, 15, 16)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6"))
    val func =
      (a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int) => a1 + a2 + a3 + a4 + a5 + a6
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6), (11, 12, 13, 14, 15, 16)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(callUDF(funcName, df("a1"), df("a2"), df("a3"), df("a4"), df("a5"), df("a6"))),
        Seq(Row(21), Row(81)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"))),
        Seq(Row(21), Row(81)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 7 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6, 7), (11, 12, 13, 14, 15, 16, 17)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7"))
    val func = (a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int, a7: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6, 7), (11, 12, 13, 14, 15, 16, 17)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"))),
        Seq(Row(28), Row(98)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"))),
        Seq(Row(28), Row(98)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 8 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6, 7, 8), (11, 12, 13, 14, 15, 16, 17, 18)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"))
    val func = (a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int, a7: Int, a8: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6, 7, 8), (11, 12, 13, 14, 15, 16, 17, 18)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"))),
        Seq(Row(36), Row(116)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"))),
        Seq(Row(36), Row(116)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 9 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6, 7, 8, 9), (11, 12, 13, 14, 15, 16, 17, 18, 19)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"))
    val func =
      (a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int, a7: Int, a8: Int, a9: Int) =>
        a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(Seq((1, 2, 3, 4, 5, 6, 7, 8, 9), (11, 12, 13, 14, 15, 16, 17, 18, 19)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"))),
        Seq(Row(45), Row(135)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"))),
        Seq(Row(45), Row(135)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 10 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10), (11, 12, 13, 14, 15, 16, 17, 18, 19, 20)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int) => a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10), (11, 12, 13, 14, 15, 16, 17, 18, 19, 20)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"))),
        Seq(Row(55), Row(155)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"))),
        Seq(Row(55), Row(155)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 11 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int) => a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"))),
        Seq(Row(66), Row(176)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"))),
        Seq(Row(66), Row(176)))
    } finally {
      runQuery(s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)", session)
    }
    // scalastyle:on
  }

  test("register permanent: 12 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int) => a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"))),
        Seq(Row(78), Row(198)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"))),
        Seq(Row(78), Row(198)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 13 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12", "a13"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int) => a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)))
      .toDF(Seq("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12", "a13"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"))),
        Seq(Row(91), Row(221)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"))),
        Seq(Row(91), Row(221)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 14 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int) => a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"))),
        Seq(Row(105), Row(245)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"))),
        Seq(Row(105), Row(245)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 15 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"))),
        Seq(Row(120), Row(270)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"))),
        Seq(Row(120), Row(270)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 16 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int,
        a16: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"),
            df("a16"))),
        Seq(Row(136), Row(296)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"),
            newDf("a16"))),
        Seq(Row(136), Row(296)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 17 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int,
        a16: Int,
        a17: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"),
            df("a16"),
            df("a17"))),
        Seq(Row(153), Row(323)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"),
            newDf("a16"),
            newDf("a17"))),
        Seq(Row(153), Row(323)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 18 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int,
        a16: Int,
        a17: Int,
        a18: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"),
            df("a16"),
            df("a17"),
            df("a18"))),
        Seq(Row(171), Row(351)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"),
            newDf("a16"),
            newDf("a17"),
            newDf("a18"))),
        Seq(Row(171), Row(351)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 19 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int,
        a16: Int,
        a17: Int,
        a18: Int,
        a19: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18 + a19
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"),
            df("a16"),
            df("a17"),
            df("a18"),
            df("a19"))),
        Seq(Row(190), Row(380)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"),
            newDf("a16"),
            newDf("a17"),
            newDf("a18"),
            newDf("a19"))),
        Seq(Row(190), Row(380)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 20 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19",
          "a20"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int,
        a16: Int,
        a17: Int,
        a18: Int,
        a19: Int,
        a20: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18 + a19 + a20
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19",
          "a20"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"),
            df("a16"),
            df("a17"),
            df("a18"),
            df("a19"),
            df("a20"))),
        Seq(Row(210), Row(410)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"),
            newDf("a16"),
            newDf("a17"),
            newDf("a18"),
            newDf("a19"),
            newDf("a20"))),
        Seq(Row(210), Row(410)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 21 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19",
          "a20",
          "a21"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int,
        a16: Int,
        a17: Int,
        a18: Int,
        a19: Int,
        a20: Int,
        a21: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18 + a19 + a20 + a21
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(
        Seq(
          (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
          (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19",
          "a20",
          "a21"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"),
            df("a16"),
            df("a17"),
            df("a18"),
            df("a19"),
            df("a20"),
            df("a21"))),
        Seq(Row(231), Row(441)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"),
            newDf("a16"),
            newDf("a17"),
            newDf("a18"),
            newDf("a19"),
            newDf("a20"),
            newDf("a21"))),
        Seq(Row(231), Row(441)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("register permanent: 22 arguments", JavaStoredProcExclude) {
    // scalastyle:off
    import functions.callUDF
    val df = session
      .createDataFrame(Seq(
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22),
        (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19",
          "a20",
          "a21",
          "a22"))
    val func = (
        a1: Int,
        a2: Int,
        a3: Int,
        a4: Int,
        a5: Int,
        a6: Int,
        a7: Int,
        a8: Int,
        a9: Int,
        a10: Int,
        a11: Int,
        a12: Int,
        a13: Int,
        a14: Int,
        a15: Int,
        a16: Int,
        a17: Int,
        a18: Int,
        a19: Int,
        a20: Int,
        a21: Int,
        a22: Int) =>
      a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17 + a18 + a19 + a20 + a21 + a22
    val funcName: String = randomName()
    val newDf = newSession
      .createDataFrame(Seq(
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22),
        (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32)))
      .toDF(
        Seq(
          "a1",
          "a2",
          "a3",
          "a4",
          "a5",
          "a6",
          "a7",
          "a8",
          "a9",
          "a10",
          "a11",
          "a12",
          "a13",
          "a14",
          "a15",
          "a16",
          "a17",
          "a18",
          "a19",
          "a20",
          "a21",
          "a22"))
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      checkAnswer(
        df.select(
          callUDF(
            funcName,
            df("a1"),
            df("a2"),
            df("a3"),
            df("a4"),
            df("a5"),
            df("a6"),
            df("a7"),
            df("a8"),
            df("a9"),
            df("a10"),
            df("a11"),
            df("a12"),
            df("a13"),
            df("a14"),
            df("a15"),
            df("a16"),
            df("a17"),
            df("a18"),
            df("a19"),
            df("a20"),
            df("a21"),
            df("a22"))),
        Seq(Row(253), Row(473)))
      checkAnswer(
        newDf.select(
          callUDF(
            funcName,
            newDf("a1"),
            newDf("a2"),
            newDf("a3"),
            newDf("a4"),
            newDf("a5"),
            newDf("a6"),
            newDf("a7"),
            newDf("a8"),
            newDf("a9"),
            newDf("a10"),
            newDf("a11"),
            newDf("a12"),
            newDf("a13"),
            newDf("a14"),
            newDf("a15"),
            newDf("a16"),
            newDf("a17"),
            newDf("a18"),
            newDf("a19"),
            newDf("a20"),
            newDf("a21"),
            newDf("a22"))),
        Seq(Row(253), Row(473)))
    } finally {
      runQuery(
        s"drop function $funcName(INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)",
        session)
    }
    // scalastyle:on
  }

  test("UDF read file with com.snowflake.import_directory: basic case") {
    val stageName1 = randomName()
    val funcName1 = randomName()
    val fileName = "file1.csv"
    try {
      runQuery(s"create or replace stage $stageName1", session)
      // Write one file in temp dir
      writeFile(fileName, "abc,123", tempDirectory1)
      session.addDependency(tempDirectory1.getAbsolutePath + "/" + fileName)
      val df1 = session.createDataFrame(Seq(fileName)).toDF(Seq("a"))

      // Function to read file with "com.snowflake.import_directory"
      val readFileUDF = (name: String) => {
        val localFileName = System.getProperty("com.snowflake.import_directory") + name
        Files.readAllBytes(Paths.get(localFileName)).map { _.toChar }.mkString
      }

      session.udf.registerPermanent(funcName1, readFileUDF, stageName1)
      // Column name 'a' is the file name.
      checkAnswer(df1.select(callUDF(funcName1, df1("a"))), Seq(Row("abc,123")))
    } finally {
      runQuery(s"drop function if exists $funcName1(STRING)", session)
      session.removeDependency(tempDirectory1.getAbsolutePath + "/" + fileName)
      removeFile(tempDirectory1.getAbsolutePath + "/" + fileName, session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("UDF read file with com.snowflake.import_directory: complex case", JavaStoredProcExclude) {
    // Two session to read two files (same file name, but different content) in UDF
    val stageName1 = randomName()
    val funcName1 = randomName()
    val funcName2 = randomName()
    val fileName = "file1.csv"
    try {
      runQuery(s"create stage $stageName1", session)

      // Write 2 files: same filename, diff content in diff directories
      writeFile(fileName, "abc,123", tempDirectory1)
      writeFile(fileName, "abcd,1234", tempDirectory2)

      session.addDependency(tempDirectory1.getAbsolutePath + "/" + fileName)
      newSession.addDependency(tempDirectory2.getAbsolutePath + "/" + fileName)

      val df1 = session.createDataFrame(Seq(fileName)).toDF(Seq("a"))
      val df2 = newSession.createDataFrame(Seq(fileName)).toDF(Seq("a"))

      // Function to read file with "com.snowflake.import_directory"
      val readFileUDF = (name: String) => {
        val localFileName = System.getProperty("com.snowflake.import_directory") + name
        Files.readAllBytes(Paths.get(localFileName)).map { _.toChar }.mkString
      }

      session.udf.registerPermanent(funcName1, readFileUDF, stageName1)
      newSession.udf.registerPermanent(funcName2, readFileUDF, stageName1)

      // session 1 read file in tempDirectory1
      checkAnswer(df1.select(callUDF(funcName1, df1("a"))), Seq(Row("abc,123")))
      // session 2 read file in tempDirectory2
      checkAnswer(df2.select(callUDF(funcName2, df2("a"))), Seq(Row("abcd,1234")))

      // session 1: remove & add the same name file (diff checksum)
      runQuery(s"drop function if exists $funcName1(STRING)", session)
      session.removeDependency(tempDirectory1.getAbsolutePath + "/" + fileName)
      session.addDependency(tempDirectory2.getAbsolutePath + "/" + fileName)
      session.udf.registerPermanent(funcName1, readFileUDF, stageName1)
      // The UDF will read the file in tempDirectory2
      checkAnswer(df1.select(callUDF(funcName1, df1("a"))), Seq(Row("abcd,1234")))
    } finally {
      runQuery(s"drop function if exists $funcName1(STRING)", session)
      runQuery(s"drop function if exists $funcName2(STRING)", newSession)
      session.removeDependency(tempDirectory1.getAbsolutePath + "/" + fileName)
      session.removeDependency(tempDirectory2.getAbsolutePath + "/" + fileName)
      newSession.removeDependency(tempDirectory1.getAbsolutePath + "/" + fileName)
      newSession.removeDependency(tempDirectory2.getAbsolutePath + "/" + fileName)
      removeFile(tempDirectory1.getAbsolutePath + "/" + fileName, session)
      removeFile(tempDirectory2.getAbsolutePath + "/" + fileName, session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("UDF read file with archived jar") {
    val stageName1 = randomName()
    val funcName1 = randomName()
    val fileName = "file1.csv"
    val jarFile = tempDirectory1.getAbsolutePath + "/" + s"$fileName.jar"
    try {
      runQuery(s"create or replace stage $stageName1", session)

      // create a jar file in temp dir which includes file1.csv
      val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
      jarStream.putNextEntry(new java.util.jar.JarEntry(fileName))
      jarStream.write("abc,123".getBytes())
      jarStream.closeEntry()
      jarStream.close()

      session.addDependency(jarFile)
      val df1 = session.createDataFrame(Seq(fileName)).toDF(Seq("a"))

      // Function to read file with getResourceAsStream()
      val readFileUDF = (name: String) => {
        val inputStream = classOf[com.snowflake.snowpark.DataFrame]
          .getResourceAsStream("/" + name)
        if (inputStream == null) {
          throw new Exception("Can't find file " + fileName)
        }
        scala.io.Source.fromInputStream(inputStream).mkString
      }

      session.udf.registerPermanent(funcName1, readFileUDF, stageName1)
      // Column name 'a' is the file name.
      checkAnswer(df1.select(callUDF(funcName1, df1("a"))), Seq(Row("abc,123")))
    } finally {
      runQuery(s"drop function if exists $funcName1(STRING)", session)
      session.removeDependency(jarFile)
      removeFile(jarFile, session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }
  // session.file.put is not supported in stored procs
  test("UDF read file with staged file", JavaStoredProcExclude) {
    val stageName1 = randomName()
    val funcName1 = randomName()
    val fileName = "file1.csv"
    try {
      runQuery(s"create or replace stage $stageName1", session)

      // Write one file in temp dir
      writeFile(fileName, "abc,123", tempDirectory1)
      session.file.put(
        TestUtils.escapePath(
          tempDirectory1.getCanonicalPath +
            TestUtils.fileSeparator + fileName),
        stageName1,
        Map("AUTO_COMPRESS" -> "FALSE"))
      session.addDependency(s"@$stageName1/" + fileName)
      val df1 = session.createDataFrame(Seq(fileName)).toDF(Seq("a"))

      // Function to read file with "com.snowflake.import_directory"
      val readFileUDF = (name: String) => {
        val localFileName = System.getProperty("com.snowflake.import_directory") + name
        Files.readAllBytes(Paths.get(localFileName)).map { _.toChar }.mkString
      }

      session.udf.registerPermanent(funcName1, readFileUDF, stageName1)
      // Column name 'a' is the file name.
      checkAnswer(df1.select(callUDF(funcName1, df1("a"))), Seq(Row("abc,123")))
    } finally {
      runQuery(s"drop function if exists $funcName1(STRING)", session)
      session.removeDependency(s"@$stageName1/" + fileName)
      removeFile(tempDirectory1.getAbsolutePath + "/" + fileName, session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("Test callUDF with numeric literal") {
    val timestamp: Long = 1606179541282L
    val values = (
      2.toShort,
      3.toInt,
      4L,
      1.1F,
      1.2D,
      new java.math.BigDecimal(1.3).setScale(3, RoundingMode.HALF_DOWN))

    val func = (a: Short, b: Int, c: Long, d: Float, e: Double, f: java.math.BigDecimal) =>
      s"$a $b $c $d $e $f"

    val funcName = randomName()
    try {
      session.udf.registerPermanent(funcName, func, stageName)
      // test callUDF()
      val df = session
        .range(1)
        .select(
          callUDF(funcName, values._1, values._2, values._3, values._4, values._5, values._6))
      checkAnswer(df, Seq(Row("2 3 4 1.1 1.2 1.300000000000000000")))
      // test callBuiltin()
      val df2 = session
        .range(1)
        .select(
          callBuiltin(funcName, values._1, values._2, values._3, values._4, values._5, values._6))
      checkAnswer(df2, Seq(Row("2 3 4 1.1 1.2 1.300000000000000000")))
      // test builtin()()
      val df3 = session
        .range(1)
        .select(
          builtin(funcName)(values._1, values._2, values._3, values._4, values._5, values._6))
      checkAnswer(df3, Seq(Row("2 3 4 1.1 1.2 1.300000000000000000")))
    } finally {
      runQuery(
        s"drop function if exists $funcName(SMALLINT,INT,BIGINT,FLOAT,DOUBLE,NUMBER(38,18))",
        session)
    }
  }

  test("Test callUDF with non-numeric literal", JavaStoredProcExclude) {
    val timestamp: Long = 1606179541282L
    val values = (
      "str",
      true,
      Array(61.toByte, 62.toByte),
      new Timestamp(timestamp - 100),
      new Date(timestamp - 100))

    val func = (a: String, b: Boolean, c: Array[Byte], d: Timestamp, e: Date) =>
      s"$a $b 0x${c.map { _.toHexString }.mkString("")} $d $e"

    val funcName = randomName()
    val defaultTimeZone = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
      session.udf.registerPermanent(funcName, func, stageName)
      // test callUDF()
      val df = session
        .range(1)
        .select(callUDF(funcName, values._1, values._2, values._3, values._4, values._5))
      df.show()
      checkAnswer(df, Seq(Row("str true 0x3d3e 2020-11-23 16:59:01.182 2020-11-23")))
      // test callBuiltin()
      val df2 = session
        .range(1)
        .select(callBuiltin(funcName, values._1, values._2, values._3, values._4, values._5))
      checkAnswer(df2, Seq(Row("str true 0x3d3e 2020-11-23 16:59:01.182 2020-11-23")))
      // test builtin()()
      val df3 = session
        .range(1)
        .select(builtin(funcName)(values._1, values._2, values._3, values._4, values._5))
      checkAnswer(df3, Seq(Row("str true 0x3d3e 2020-11-23 16:59:01.182 2020-11-23")))
    } finally {
      TimeZone.setDefault(defaultTimeZone)
      runQuery(
        s"drop function if exists $funcName(STRING,BOOLEAN,BINARY,TIMESTAMP,DATE)",
        session)
    }
  }

  test("register temp/perm UDF doesn't commit open transaction", JavaStoredProcExclude) {
    val udf = (x: Int) => x + 1
    val tempFuncName = randomName()
    val permFuncName = randomName()
    val stageName1 = randomName()
    try {
      runQuery(s"create stage $stageName1", session)

      // Test temp/perm function registration
      session.sql("begin").collect()
      session.udf.registerTemporary(tempFuncName, udf)
      assert(isActiveTransaction(session))
      session.udf.registerPermanent(permFuncName, udf, stageName1)
      assert(isActiveTransaction(session))

      // Use temp/perm functions
      val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
      checkAnswer(df.select(callUDF(tempFuncName, df("a"))), Seq(Row(2), Row(3)))
      checkAnswer(df.select(callUDF(permFuncName, df("a"))), Seq(Row(2), Row(3)))
      assert(isActiveTransaction(session))

      session.sql("commit").collect()
      assert(!isActiveTransaction(session))
    } finally {
      runQuery(s"drop function if exists $tempFuncName(INT)", session)
      runQuery(s"drop function if exists $permFuncName(INT)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }
}
