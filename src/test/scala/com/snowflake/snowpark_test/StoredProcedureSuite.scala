package com.snowflake.snowpark_test

import com.snowflake.snowpark._
import com.snowflake.snowpark.internal.Utils.SnowparkPackageName
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.sql.{Date, Timestamp}

@UDFTest
class StoredProcedureSuite extends SNTestBase {
  private val testStage = randomName()

  override def beforeAll: Unit = {
    super.beforeAll
    createStage(testStage, isTemporary = false)
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session, Some(testStage))
    }
    enableScala213UdxfSprocParams(session)
  }

  override def afterAll: Unit = {
    dropStage(testStage)
    disableScala213UdxfSprocParams(session)
    super.afterAll
  }

  test("call") {
    val spName = randomName()
    val query =
      s"""create or replace procedure $spName(str STRING)
         |returns STRING
         |language scala
         |runtime_version=2.12
         |packages=('${SnowparkPackageName}:latest')
         |handler='Test.run'
         |as
         |$$$$
         |object Test {
         |  def run(session: com.snowflake.snowpark.Session, str: String): String = {
         |    str
         |  }
         |}
         |$$$$""".stripMargin
    try {
      session.sql(query).show()
      checkAnswer(session.storedProcedure(spName, "test"), Seq(Row("test")))
      // can composite new dataframe
      checkAnswer(session.storedProcedure(spName, "test").select("*"), Seq(Row("test")))
    } finally {
      session.sql(s"drop procedure if exists $spName (STRING)").show()
    }
  }

  test("call 0 argument") {
    val spName = randomName()
    val query =
      s"""create or replace procedure $spName()
         |returns STRING
         |language scala
         |runtime_version=2.12
         |packages=('${SnowparkPackageName}:latest')
         |handler='Test.run'
         |as
         |$$$$
         |object Test {
         |  def run(session: com.snowflake.snowpark.Session): String = {
         |    "SUCCESS"
         |  }
         |}
         |$$$$""".stripMargin
    try {
      session.sql(query).show()
      checkAnswer(session.storedProcedure(spName), Seq(Row("SUCCESS")))
    } finally {
      session.sql(s"drop procedure if exists $spName ()").show()
    }
  }

  test("call multiple types of arguments") {
    val spName = randomName()
    val query =
      s"""create or replace procedure $spName(str STRING, num INT, flo FLOAT, boo BOOLEAN)
         |returns STRING
         |language scala
         |runtime_version=2.12
         |packages=('${SnowparkPackageName}:latest')
         |handler='Test.run'
         |as
         |$$$$
         |object Test {
         |  def run(session: com.snowflake.snowpark.Session, str: String,
         |    num: Int, flo: Float, boo: Boolean): String = {
         |    s"input: $$str, $$num, $$flo, $$boo"
         |  }
         |}
         |$$$$""".stripMargin
    try {
      session.sql(query).show()
      checkAnswer(
        session.storedProcedure(spName, "a", 1, 1.1, true),
        Seq(Row("input: a, 1, 1.1, true")))
    } finally {
      session.sql(s"drop procedure if exists $spName (STRING, INT, FLOAT, BOOLEAN)").show()
    }
  }

  test("invalid name") {
    val msg = intercept[SnowparkClientException](session.storedProcedure("ad#asd")).getMessage
    assert(msg.contains("The object name 'ad#asd' is invalid"))
  }

  // temporary disabled, waiting for server side JDBC upgrade
  ignore("closure") {
    val num1 = 123
    val sp = session.sproc.registerTemporary((session: Session, num2: Int) => {
      val result = session.sql(s"select $num2").collect().head.getInt(0)
      result + num1
    })
    checkAnswer(session.storedProcedure(sp, 1000), Seq(Row(1123)))
  }

  test("basic") {
    val str = "input"
    val sp = session.sproc.registerTemporary((_: Session, num: Int) => s"$str: $num")
    checkAnswer(session.storedProcedure(sp, 123), Seq(Row("input: 123")))
  }

  test("multiple input types") {
    val sp = session.sproc.registerTemporary(
      (_: Session, num1: Int, num2: Long, num3: Short, num4: Float, num5: Double, bool: Boolean) =>
        {
          val num = num1 + num2 + num3
          val float = (num4 + num5).ceil
          s"$num, $float, $bool"
        })
    checkAnswer(
      session.storedProcedure(sp, 1, 2L, 3.toShort, 4.4f, 5.5, false),
      Seq(Row(s"6, 10.0, false")))
  }

  test("decimal input") {
    val sp = session.sproc.registerTemporary((_: Session, num: java.math.BigDecimal) => num)
    session.storedProcedure(sp, java.math.BigDecimal.valueOf(123)).show()
  }

  test("binary type") {
    val toBinary = session.sproc.registerTemporary((_: Session, str: String) => str.getBytes)
    val fromBinary =
      session.sproc.registerTemporary((_: Session, bytes: Array[Byte]) => new String(bytes))
    checkAnswer(session.storedProcedure(toBinary, "hello"), Seq(Row("hello".getBytes)))
    checkAnswer(session.storedProcedure(fromBinary, "hello".getBytes), Seq(Row("hello")))
  }

  test("Timestamp") {
    testWithTimezone() {
      val time = Timestamp.valueOf("2019-01-01 00:00:00")
      val date = Date.valueOf("2019-01-01")
      val d = session.sproc.registerTemporary((_: Session, d: Date) => d)
      val t = session.sproc.registerTemporary((_: Session, t: Timestamp) => t)
      checkAnswer(session.storedProcedure(d, date), Seq(Row(date)))
      checkAnswer(session.storedProcedure(t, time), Seq(Row(time)))
    }
  }

  test("permanent: 0 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session) => s"SUCCESS",
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp), Seq(Row("SUCCESS")))
      checkAnswer(session.storedProcedure(spName), Seq(Row("SUCCESS")))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName ()").show()
    }
  }

  test("permanent: 1 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session, num1: Int) => num1 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1), Seq(Row(101)))
      checkAnswer(session.storedProcedure(spName, 1), Seq(Row(101)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT)").show()
    }
  }

  // scalastyle:off line.size.limit
  /* This script generates the tests below
   * (2 to 21).foreach { x =>
   * val cols = (1 to x).map(i => s"num$i: Int").mkString(", ")
   * val statements = (1 to x).map(i => s"num$i").mkString(" + ")
   * val result = (1 to x).reduceLeft(_ + _) + 100
   * val data = (1 to x).map(i => s"$i").mkString(", ")
   * val argTypes = (1 to x).map(_ => "INT").mkString(",")
   * println(s"""
   *  |test("permanent: $x args", JavaStoredProcExclude) {
   *  |  val spName = randomName()
   *  |  val stageName = randomName()
   *  |  try {
   *  |    createStage(stageName, isTemporary = false)
   *  |    val sp = session.sproc.registerPermanent(
   *  |      spName,
   *  |      (_: Session, $cols) => $statements + 100,
   *  |      stageName,
   *  |      isCallerMode = true)
   *  |    checkAnswer(session.storedProcedure(sp, $data), Seq(Row($result)))
   *  |    checkAnswer(session.storedProcedure(spName, $data), Seq(Row($result)))
   *  |  } finally {
   *  |    dropStage(stageName)
   *  |    session.sql(s"drop procedure if exists $$spName ($argTypes)").show()
   *  |  }
   *  |}""".stripMargin)
   * }
   */
  // scalastyle:on line.size.limit

  test("permanent: 2 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session, num1: Int, num2: Int) => num1 + num2 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2), Seq(Row(103)))
      checkAnswer(session.storedProcedure(spName, 1, 2), Seq(Row(103)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT,INT)").show()
    }
  }

  test("permanent: 3 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session, num1: Int, num2: Int, num3: Int) => num1 + num2 + num3 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3), Seq(Row(106)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3), Seq(Row(106)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT,INT,INT)").show()
    }
  }

  test("permanent: 4 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session, num1: Int, num2: Int, num3: Int, num4: Int) => num1 + num2 + num3 + num4 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4), Seq(Row(110)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4), Seq(Row(110)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT,INT,INT,INT)").show()
    }
  }

  test("permanent: 5 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int) =>
          num1 + num2 + num3 + num4 + num5 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5), Seq(Row(115)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4, 5), Seq(Row(115)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT)").show()
    }
  }

  test("permanent: 6 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int, num6: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6), Seq(Row(121)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4, 5, 6), Seq(Row(121)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT)").show()
    }
  }

  test("permanent: 7 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int, num6: Int, num7: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7), Seq(Row(128)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7), Seq(Row(128)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT)").show()
    }
  }

  test("permanent: 8 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8), Seq(Row(136)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8), Seq(Row(136)))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT)").show()
    }
  }

  test("permanent: 9 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9), Seq(Row(145)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9), Seq(Row(145)))
    } finally {
      dropStage(stageName)
      session
        .sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 10 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Seq(Row(155)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Seq(Row(155)))
    } finally {
      dropStage(stageName)
      session
        .sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 11 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Seq(Row(166)))
      checkAnswer(session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Seq(Row(166)))
    } finally {
      dropStage(stageName)
      session
        .sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 12 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), Seq(Row(178)))
      checkAnswer(
        session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        Seq(Row(178)))
    } finally {
      dropStage(stageName)
      session
        .sql(s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 13 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
        Seq(Row(191)))
      checkAnswer(
        session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
        Seq(Row(191)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 14 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
        Seq(Row(205)))
      checkAnswer(
        session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
        Seq(Row(205)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 15 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int,
            num15: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + num15 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        Seq(Row(220)))
      checkAnswer(
        session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        Seq(Row(220)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 16 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int,
            num15: Int,
            num16: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + num15 + num16 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
        Seq(Row(236)))
      checkAnswer(
        session.storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
        Seq(Row(236)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 17 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int,
            num15: Int,
            num16: Int,
            num17: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
        Seq(Row(253)))
      checkAnswer(
        session
          .storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
        Seq(Row(253)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 18 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int,
            num15: Int,
            num16: Int,
            num17: Int,
            num18: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session
          .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
        Seq(Row(271)))
      checkAnswer(
        session
          .storedProcedure(spName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
        Seq(Row(271)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 19 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int,
            num15: Int,
            num16: Int,
            num17: Int,
            num18: Int,
            num19: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + num19 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session
          .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
        Seq(Row(290)))
      checkAnswer(
        session.storedProcedure(
          spName,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19),
        Seq(Row(290)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 20 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int,
            num15: Int,
            num16: Int,
            num17: Int,
            num18: Int,
            num19: Int,
            num20: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 +
            num18 + num19 + num20 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session.storedProcedure(
          sp,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20),
        Seq(Row(310)))
      checkAnswer(
        session.storedProcedure(
          spName,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20),
        Seq(Row(310)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent: 21 args", JavaStoredProcExclude) {
    val spName = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      val sp = session.sproc.registerPermanent(
        spName,
        (
            _: Session,
            num1: Int,
            num2: Int,
            num3: Int,
            num4: Int,
            num5: Int,
            num6: Int,
            num7: Int,
            num8: Int,
            num9: Int,
            num10: Int,
            num11: Int,
            num12: Int,
            num13: Int,
            num14: Int,
            num15: Int,
            num16: Int,
            num17: Int,
            num18: Int,
            num19: Int,
            num20: Int,
            num21: Int) =>
          num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
            num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 +
            num18 + num19 + num20 + num21 + 100,
        stageName,
        isCallerMode = true)
      checkAnswer(
        session.storedProcedure(
          sp,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21),
        Seq(Row(331)))
      checkAnswer(
        session.storedProcedure(
          spName,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21),
        Seq(Row(331)))
    } finally {
      dropStage(stageName)
      session
        .sql(
          s"drop procedure if exists $spName (INT,INT,INT,INT,INT,INT,INT," +
            "INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT,INT)")
        .show()
    }
  }

  test("permanent is permanent") {
    val spName = randomName()
    val stageName = randomName()
    val newSession = Session.builder.configFile(defaultProfile).create
    try {
      createStage(stageName, isTemporary = false)
      session.sproc.registerPermanent(
        spName,
        (_: Session) => s"SUCCESS",
        stageName,
        isCallerMode = true)
      checkAnswer(session.storedProcedure(spName), Seq(Row("SUCCESS")))
      // works in other sessions
      checkAnswer(newSession.storedProcedure(spName), Seq(Row("SUCCESS")))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName ()").show()
      newSession.close()
    }
  }

  test("caller and owner mode") {
    val spName1 = randomName()
    val spName2 = randomName()
    val stageName = randomName()
    try {
      createStage(stageName, isTemporary = false)
      session.sproc.registerPermanent(
        spName1,
        (_: Session) => s"SUCCESS",
        stageName,
        isCallerMode = true)
      import com.snowflake.snowpark.functions.col
      checkAnswer(
        session
          .sql(s"describe procedure $spName1()")
          .where(col(""""property"""") === "execute as")
          .select(col(""""value"""")),
        Seq(Row("CALLER")))

      session.sproc.registerPermanent(
        spName2,
        (_: Session) => s"SUCCESS",
        stageName,
        isCallerMode = false)
      checkAnswer(
        session
          .sql(s"describe procedure $spName2()")
          .where(col(""""property"""") === "execute as")
          .select(col(""""value"""")),
        Seq(Row("OWNER")))
    } finally {
      dropStage(stageName)
      session.sql(s"drop procedure if exists $spName1()").show()
      session.sql(s"drop procedure if exists $spName2()").show()
    }
  }

  test("anonymous temporary: 0 args", JavaStoredProcExclude) {
    val func = (_: Session) => s"SUCCESS"
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func)
    assert(result == "SUCCESS")
    checkAnswer(session.storedProcedure(sp), Seq(Row(result)))
  }

  test("anonymous temporary: 1 args", JavaStoredProcExclude) {
    val func = (_: Session, num1: Int) => num1 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1)
    assert(result == 101)
    checkAnswer(session.storedProcedure(sp, 1), Seq(Row(result)))
  }

  // scalastyle:off line.size.limit
  /* This script generates the tests below
(2 to 21).foreach { x =>
val cols = (1 to x).map(i => s"num$i: Int").mkString(", ")
val statements = (1 to x).map(i => s"num$i").mkString(" + ")
val result = (1 to x).reduceLeft(_ + _) + 100
val data = (1 to x).map(i => s"$i").mkString(", ")
println(s"""
  |test("anonymous temporary: $x args", JavaStoredProcExclude) {
  |val func = (_: Session, $cols) => $statements + 100
  |val sp = session.sproc.registerTemporary(func)
  |val result = session.sproc.runLocally(func, $data)
  |assert(result == $result)
  |checkAnswer(session.storedProcedure(sp, $data), Seq(Row(result)))
  |}""".stripMargin)
}
   */
  // scalastyle:on line.size.limit

  test("anonymous temporary: 2 args", JavaStoredProcExclude) {
    val func = (_: Session, num1: Int, num2: Int) => num1 + num2 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2)
    assert(result == 103)
    checkAnswer(session.storedProcedure(sp, 1, 2), Seq(Row(result)))
  }

  test("anonymous temporary: 3 args", JavaStoredProcExclude) {
    val func = (_: Session, num1: Int, num2: Int, num3: Int) => num1 + num2 + num3 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3)
    assert(result == 106)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3), Seq(Row(result)))
  }

  test("anonymous temporary: 4 args", JavaStoredProcExclude) {
    val func =
      (_: Session, num1: Int, num2: Int, num3: Int, num4: Int) => num1 + num2 + num3 + num4 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4)
    assert(result == 110)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4), Seq(Row(result)))
  }

  test("anonymous temporary: 5 args", JavaStoredProcExclude) {
    val func = (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int) =>
      num1 + num2 + num3 + num4 + num5 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5)
    assert(result == 115)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5), Seq(Row(result)))
  }

  test("anonymous temporary: 6 args", JavaStoredProcExclude) {
    val func = (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int, num6: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6)
    assert(result == 121)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6), Seq(Row(result)))
  }

  test("anonymous temporary: 7 args", JavaStoredProcExclude) {
    val func =
      (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int, num6: Int, num7: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7)
    assert(result == 128)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7), Seq(Row(result)))
  }

  test("anonymous temporary: 8 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8)
    assert(result == 136)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8), Seq(Row(result)))
  }

  test("anonymous temporary: 9 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    assert(result == 145)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9), Seq(Row(result)))
  }

  test("anonymous temporary: 10 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    assert(result == 155)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Seq(Row(result)))
  }

  test("anonymous temporary: 11 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    assert(result == 166)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Seq(Row(result)))
  }

  test("anonymous temporary: 12 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    assert(result == 178)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
      Seq(Row(result)))
  }

  test("anonymous temporary: 13 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
    assert(result == 191)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
      Seq(Row(result)))
  }

  test("anonymous temporary: 14 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
    assert(result == 205)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
      Seq(Row(result)))
  }

  test("anonymous temporary: 15 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int,
        num15: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + num15 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
    assert(result == 220)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
      Seq(Row(result)))
  }

  test("anonymous temporary: 16 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int,
        num15: Int,
        num16: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + num15 + num16 + 100
    val sp = session.sproc.registerTemporary(func)
    val result =
      session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    assert(result == 236)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
      Seq(Row(result)))
  }

  test("anonymous temporary: 17 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int,
        num15: Int,
        num16: Int,
        num17: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + 100
    val sp = session.sproc.registerTemporary(func)
    val result =
      session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    assert(result == 253)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
      Seq(Row(result)))
  }

  test("anonymous temporary: 18 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int,
        num15: Int,
        num16: Int,
        num17: Int,
        num18: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + 100
    val sp = session.sproc.registerTemporary(func)
    val result =
      session.sproc.runLocally(func, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
    assert(result == 271)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
      Seq(Row(result)))
  }

  test("anonymous temporary: 19 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int,
        num15: Int,
        num16: Int,
        num17: Int,
        num18: Int,
        num19: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + num19 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(
      func,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19)
    assert(result == 290)
    checkAnswer(
      session
        .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
      Seq(Row(result)))
  }

  test("anonymous temporary: 20 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int,
        num15: Int,
        num16: Int,
        num17: Int,
        num18: Int,
        num19: Int,
        num20: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + num19 + num20 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(
      func,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20)
    assert(result == 310)
    checkAnswer(
      session
        .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      Seq(Row(result)))
  }

  test("anonymous temporary: 21 args", JavaStoredProcExclude) {
    val func = (
        _: Session,
        num1: Int,
        num2: Int,
        num3: Int,
        num4: Int,
        num5: Int,
        num6: Int,
        num7: Int,
        num8: Int,
        num9: Int,
        num10: Int,
        num11: Int,
        num12: Int,
        num13: Int,
        num14: Int,
        num15: Int,
        num16: Int,
        num17: Int,
        num18: Int,
        num19: Int,
        num20: Int,
        num21: Int) =>
      num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
        num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 +
        num18 + num19 + num20 + num21 + 100
    val sp = session.sproc.registerTemporary(func)
    val result = session.sproc.runLocally(
      func,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20,
      21)
    assert(result == 331)
    checkAnswer(
      session.storedProcedure(
        sp,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21),
      Seq(Row(result)))
  }

  test("named temporary: duplicated name") {
    val name = randomName()
    val sp1 = session.sproc.registerTemporary(name, (_: Session) => s"SP 1")
    val msg = intercept[SnowflakeSQLException](
      session.sproc.registerTemporary(name, (_: Session) => s"SP 2")).getMessage
    assert(msg.contains("already exists"))
  }

  test("named temporary: 0 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(name, (_: Session) => s"SUCCESS")
    checkAnswer(session.storedProcedure(sp), Seq(Row("SUCCESS")))
    checkAnswer(session.storedProcedure(name), Seq(Row("SUCCESS")))
  }

  test("named temporary: 1 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(name, (_: Session, num1: Int) => num1 + 100)
    checkAnswer(session.storedProcedure(sp, 1), Seq(Row(101)))
    checkAnswer(session.storedProcedure(name, 1), Seq(Row(101)))
  }

  // scalastyle:off line.size.limit
  /* This script generates the tests below
  (2 to 21).foreach { x =>
    val cols = (1 to x).map(i => s"num$i: Int").mkString(", ")
    val statements = (1 to x).map(i => s"num$i").mkString(" + ")
    val result = (1 to x).reduceLeft(_ + _) + 100
    val data = (1 to x).map(i => s"$i").mkString(", ")
    println(s"""
      |test("named temporary: $x args", JavaStoredProcExclude) {
      |val name = randomName()
      |val sp = session.sproc.registerTemporary(name, (_: Session, $cols) => $statements + 100)
      |checkAnswer(session.storedProcedure(sp, $data), Seq(Row($result)))
      |checkAnswer(session.storedProcedure(name, $data), Seq(Row($result)))
      |}""".stripMargin)
   }
   */
  // scalastyle:on line.size.limit

  test("named temporary: 2 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc
      .registerTemporary(name, (_: Session, num1: Int, num2: Int) => num1 + num2 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2), Seq(Row(103)))
    checkAnswer(session.storedProcedure(name, 1, 2), Seq(Row(103)))
  }

  test("named temporary: 3 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (_: Session, num1: Int, num2: Int, num3: Int) => num1 + num2 + num3 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3), Seq(Row(106)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3), Seq(Row(106)))
  }

  test("named temporary: 4 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (_: Session, num1: Int, num2: Int, num3: Int, num4: Int) => num1 + num2 + num3 + num4 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4), Seq(Row(110)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4), Seq(Row(110)))
  }

  test("named temporary: 5 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int) =>
        num1 + num2 + num3 + num4 + num5 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5), Seq(Row(115)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5), Seq(Row(115)))
  }

  test("named temporary: 6 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int, num6: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6), Seq(Row(121)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5, 6), Seq(Row(121)))
  }

  test("named temporary: 7 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (_: Session, num1: Int, num2: Int, num3: Int, num4: Int, num5: Int, num6: Int, num7: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7), Seq(Row(128)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7), Seq(Row(128)))
  }

  test("named temporary: 8 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8), Seq(Row(136)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8), Seq(Row(136)))
  }

  test("named temporary: 9 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9), Seq(Row(145)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9), Seq(Row(145)))
  }

  test("named temporary: 10 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int) => num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Seq(Row(155)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Seq(Row(155)))
  }

  test("named temporary: 11 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Seq(Row(166)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Seq(Row(166)))
  }

  test("named temporary: 12 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11 + num12 + 100)
    checkAnswer(session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), Seq(Row(178)))
    checkAnswer(session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), Seq(Row(178)))
  }

  test("named temporary: 13 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + 100)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
      Seq(Row(191)))
    checkAnswer(
      session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
      Seq(Row(191)))
  }

  test("named temporary: 14 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + 100)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
      Seq(Row(205)))
    checkAnswer(
      session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
      Seq(Row(205)))
  }

  test("named temporary: 15 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int,
          num15: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + num15 + 100)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
      Seq(Row(220)))
    checkAnswer(
      session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
      Seq(Row(220)))
  }

  test("named temporary: 16 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int,
          num15: Int,
          num16: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + num15 + num16 + 100)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
      Seq(Row(236)))
    checkAnswer(
      session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
      Seq(Row(236)))
  }

  test("named temporary: 17 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int,
          num15: Int,
          num16: Int,
          num17: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + 100)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
      Seq(Row(253)))
    checkAnswer(
      session.storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
      Seq(Row(253)))
  }

  test("named temporary: 18 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int,
          num15: Int,
          num16: Int,
          num17: Int,
          num18: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + 100)
    checkAnswer(
      session.storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
      Seq(Row(271)))
    checkAnswer(
      session
        .storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
      Seq(Row(271)))
  }

  test("named temporary: 19 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int,
          num15: Int,
          num16: Int,
          num17: Int,
          num18: Int,
          num19: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 + num18 + num19 + 100)
    checkAnswer(
      session
        .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
      Seq(Row(290)))
    checkAnswer(
      session
        .storedProcedure(name, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
      Seq(Row(290)))
  }

  test("named temporary: 20 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int,
          num15: Int,
          num16: Int,
          num17: Int,
          num18: Int,
          num19: Int,
          num20: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 +
          num18 + num19 + num20 + 100)
    checkAnswer(
      session
        .storedProcedure(sp, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      Seq(Row(310)))
    checkAnswer(
      session.storedProcedure(
        name,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20),
      Seq(Row(310)))
  }

  test("named temporary: 21 args", JavaStoredProcExclude) {
    val name = randomName()
    val sp = session.sproc.registerTemporary(
      name,
      (
          _: Session,
          num1: Int,
          num2: Int,
          num3: Int,
          num4: Int,
          num5: Int,
          num6: Int,
          num7: Int,
          num8: Int,
          num9: Int,
          num10: Int,
          num11: Int,
          num12: Int,
          num13: Int,
          num14: Int,
          num15: Int,
          num16: Int,
          num17: Int,
          num18: Int,
          num19: Int,
          num20: Int,
          num21: Int) =>
        num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 +
          num10 + num11 + num12 + num13 + num14 + num15 + num16 + num17 +
          num18 + num19 + num20 + num21 + 100)
    checkAnswer(
      session.storedProcedure(
        sp,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21),
      Seq(Row(331)))
    checkAnswer(
      session.storedProcedure(
        name,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21),
      Seq(Row(331)))
  }

  test("temp is temp") {
    val name1 = randomName()
    session.sproc.registerTemporary(name1, (_: Session) => "Success")
    val sp2 = session.sproc.registerTemporary((_: Session) => "Success")
    val name2 = sp2.name.get.split("\\.").last

    // SPs exist in this session
    assert(session.sql(s"show procedures like '$name1'").collect().length == 1)
    assert(session.sql(s"show procedures like '$name2'").collect().length == 1)

    val newSession = Session.builder.configFile(defaultProfile).create
    // SPs don't exist in other sessions
    assert(newSession.sql(s"show procedures like '$name1'").collect().isEmpty)
    assert(newSession.sql(s"show procedures like '$name2'").collect().isEmpty)
    newSession.close()
  }

  // temporary disabled, waiting for server side JDBC upgrade
  ignore("runLocally from Sproc") {
    val sp = session.sproc.registerTemporary((session: Session, num: Int) => {
      val func = (session: Session, num: Int) => s"NUM: $num"
      session.sproc.runLocally(func, num)
    })
    checkAnswer(session.storedProcedure(sp, 123), Seq(Row("NUM: 123")))
  }

}
