package com.snowflake.snowpark

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.ParameterUtils.{
  SnowparkLazyAnalysis,
  SnowparkMaxFileUploadRetryCount,
  SnowparkRequestTimeoutInSeconds
}
import com.snowflake.snowpark.internal.{ParameterUtils, Utils}
import com.snowflake.snowpark.internal.analyzer.{
  Attribute,
  Literal,
  Project,
  Query,
  SnowflakePlan,
  schemaValueStatement
}
import com.snowflake.snowpark.types._
import net.snowflake.client.core.SFSessionProperty
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.nio.file.Files
import java.sql.{Date, Timestamp}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class APIInternalSuite extends TestData {
  private val userSchema: StructType = StructType(
    Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", DoubleType)))

  val tmpStageName: String = randomStageName()

  override def beforeAll: Unit = {
    super.beforeAll()
    // create temporary stage to store the file
    runQuery(s"CREATE TEMPORARY STAGE $tmpStageName", session)
    // upload the file to stage
    uploadFileToStage(tmpStageName, testFileCsv, compress = false)
    if (isPreprodAccount) {
      session.sql("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE=true").show()
      session
        .sql("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true")
        .show()
      session
        .sql("alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT=true")
        .show()
    }
  }

  override def afterAll: Unit = {
    if (isPreprodAccount) {
      session.sql("alter session unset ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE").show()
      session.sql("alter session unset IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE").show()
      session.sql("alter session unset FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT").show()
    }
    // drop the temporary stages
    runQuery(s"DROP STAGE IF EXISTS $tmpStageName", session)

    super.afterAll()
  }

  // session tests

  test("Test for no default db and schema") {
    val s2 = Session.builder
      .configFile(defaultProfile)
      .removeConfig("DB")
      .removeConfig("SCHEMA")
      .create

    assert(s2.getDefaultDatabase == None)
    assert(s2.getDefaultSchema == None)
  }

  // Just need to test that creating a Session with a custom jdbc connection object works
  test("Test for create Session for stored procs") {
    try {
      val conn = TestUtils.createJDBCConnection(defaultProfile)
      val spSession = Session(conn)
      assert(spSession.conn.isStoredProc)
      // Don't set statement level query_tag for store_proc
      assert(spSession.conn.getStatementParameters().isEmpty)
      assert(Session.getActiveSession.get == spSession)

      assertThrows[SnowparkClientException](Session(conn))
      // The default session for the test should return isStoredProc false
      assert(!session.conn.isStoredProc)

      // Hit exception if set the session again
      val conn2 = TestUtils.createJDBCConnection(defaultProfile)
      var ex = intercept[SnowparkClientException] {
        Session(conn2)
      }
      assert(ex.errorCode.equals("0413"))
      assert(ex.message.contains("Unexpected stored procedure active session reset."))
      conn2.close()

      // The users can't close a session used by stored procedure.
      ex = intercept[SnowparkClientException] {
        spSession.close()
      }
      assert(ex.errorCode.equals("0416"))
      assert(
        ex.message.contains("Cannot close this session because it is used by stored procedure."))
    } finally {
      Session.resetGlobalStoredProcSession()
    }
  }

  test("snowpark lazy analysis parameter") {
    val session1 = Session.builder
      .configFile(defaultProfile)
      .create
    // default value is true
    assert(session1.conn.isLazyAnalysis)

    val session2 = Session.builder
      .configFile(defaultProfile)
      .config(SnowparkLazyAnalysis, "false")
      .create

    assert(!session2.conn.isLazyAnalysis)

    val session3 = Session.builder
      .configFile(defaultProfile)
      .config(SnowparkLazyAnalysis, "true")
      .create

    assert(session3.conn.isLazyAnalysis)
  }

  test("snowpark udf jar upload timeout") {
    val session1 = Session.builder
      .configFile(defaultProfile)
      .create

    assert(session1.requestTimeoutInSeconds == 86400)

    val session2 = Session.builder
      .configFile(defaultProfile)
      .config(SnowparkRequestTimeoutInSeconds, "10")
      .create

    assert(session2.requestTimeoutInSeconds == 10)

    try {
      Session.builder
        .configFile(defaultProfile)
        .config(SnowparkRequestTimeoutInSeconds, "-1")
        .create
        .requestTimeoutInSeconds
      // Expecting error
      throw new TestFailedException("Expect an exception")
    } catch {
      case _: SnowparkClientException => // expected
      case _: SnowflakeSQLException => // expected
      case e => throw e
    }

    try {
      Session.builder
        .configFile(defaultProfile)
        .config(SnowparkRequestTimeoutInSeconds, "10000000")
        .create
        .requestTimeoutInSeconds
      // Expecting error
      throw new TestFailedException("Expect an exception")
    } catch {
      case _: SnowparkClientException => // expected
      case _: SnowflakeSQLException => // expected
      case e => throw e
    }

    // int max is 2147483647
    assertThrows[SnowparkClientException](
      Session.builder
        .configFile(defaultProfile)
        .config(SnowparkRequestTimeoutInSeconds, "2147483648")
        .create
        .requestTimeoutInSeconds)

    // int min is -2147483648
    assertThrows[SnowparkClientException](
      Session.builder
        .configFile(defaultProfile)
        .config(SnowparkRequestTimeoutInSeconds, "-2147483649")
        .create
        .requestTimeoutInSeconds)

    assertThrows[SnowparkClientException](
      Session.builder
        .configFile(defaultProfile)
        .config(SnowparkRequestTimeoutInSeconds, "abcd")
        .create
        .requestTimeoutInSeconds)

  }

  test("snowpark max file upload retry count") {
    val session1 = Session.builder
      .configFile(defaultProfile)
      .create

    // default is 5
    assert(session1.maxFileUploadRetryCount == 5)

    // configure it as 10
    val session2 = Session.builder
      .configFile(defaultProfile)
      .config(SnowparkMaxFileUploadRetryCount, "10")
      .create
    assert(session2.maxFileUploadRetryCount == 10)

    // negative test
    val ex = intercept[SnowparkClientException] {
      Session.builder
        .configFile(defaultProfile)
        .config(SnowparkMaxFileUploadRetryCount, "negative_not_number")
        .create
        .maxFileUploadRetryCount
    }
    assert(ex.errorCode.equals("0418"))
    assert(
      ex.message.contains(
        "Invalid value negative_not_number for parameter snowpark_max_file_upload_retry_count."))
  }

  test("cancel all", UnstableTest) {
    val tableName = randomName()
    val largeTable = session
      .range(10000000)
      .select(
        random().as("a"),
        random().as("b"),
        random().as("c"),
        random().as("d"),
        random().as("e"))

    try {
      val q1 = testCanceled {
        largeTable.write.saveAsTable(tableName)
      }

      val q2 = testCanceled {
        largeTable
          .select(col("a") + col("b") + col("c") + col("d") + col("e").as("result"))
          .filter(col("result") > 0)
          .count()
      }

      Thread.sleep(5000)
      session.cancelAll()

      assert(Await.result(q1, 10 minutes))
      assert(Await.result(q2, 10 minutes))
      assert(session.generateNewActionID == session.getLastCanceledID + 1)
    } finally {
      session.runQuery(s"drop table if exists $tableName")
    }
  }

  test("Java cancel all", UnstableTest) {
    val javaSession = com.snowflake.snowpark_java.Session
      .builder()
      .configFile(defaultProfile)
      .create()
    val rand = com.snowflake.snowpark_java.Functions.random()
    val df = javaSession
      .range(10000000)
      .select(rand.as("a"), rand.as("b"), rand.as("c"), rand.as("d"), rand.as("e"))

    val query = testCanceled {
      df.select(
          df.col("a")
            .plus(df.col("b"))
            .plus(df.col("c"))
            .plus(df.col("d"))
            .plus(df.col("e"))
            .as("result"))
        .filter(df.col("result").gt(com.snowflake.snowpark_java.Functions.lit(0)))
        .count()
    }

    Thread.sleep(5000)
    javaSession.cancelAll()

    assert(Await.result(query, 10 minutes))
  }

  test("test get schema/database works after USE ROLE") {
    val currentRole = session.conn.getStringDatum("SELECT current_role()")
    try {
      val db = session.getCurrentDatabase.get
      val schema = session.getCurrentSchema.get
      session.runQuery("USE ROLE PUBLIC")
      assert(session.getCurrentDatabase.get.equals(db))
      session.runQuery("USE ROLE PUBLIC")
      assert(session.getCurrentSchema.get.equals(schema))
    } finally {
      session.runQuery(s"USE ROLE $currentRole")
    }
  }

  /*
   * This test asserts that the config properties in snowpark are
   * correctly propagated to JDBC session's connection map. This test does depend
   * on some of the internal implementation logic of JDBC, so it is not ideal.
   * But this simple test does help ensure that the snowpark config is correctly set in JDBC.
   *
   */
  test("Test that all params are correctly propagated to JDBC session") {
    val session2 = Session.builder.configFile(defaultProfile).config("ROLE", "PUBLIC").create
    val jdbcSession = session2.conn.connection.getSFBaseSession
    val propertySet = jdbcSession.getConnectionPropertiesMap.keySet()
    assert(propertySet.contains(SFSessionProperty.DATABASE))
    assert(propertySet.contains(SFSessionProperty.SCHEMA))
    assert(propertySet.contains(SFSessionProperty.ROLE))
    assert(!propertySet.contains(SFSessionProperty.PROXY_HOST))
  }

  test("Test Session.sessionInfo") {
    val sessionInfo = session.sessionInfo
    assert(sessionInfo.contains(Utils.Version))
    assert(sessionInfo.contains(Utils.JavaVersion))
    assert(sessionInfo.contains(Utils.ScalaVersion))
    assert(sessionInfo.contains(session.conn.getJDBCSessionID))
    assert(sessionInfo.contains("jdbc.version"))
    assert(sessionInfo.contains("snowpark.library"))
    assert(sessionInfo.contains("scala.library"))
    assert(sessionInfo.contains("jdbc.library"))
    assert(sessionInfo.contains("client.language"))
  }

  test("close session") {
    val newSession = Session.builder.configFile(defaultProfile).create
    assert(Session.getActiveSession.nonEmpty)
    newSession.close()
    assert(
      Session.getActiveSession.isEmpty ||
        Session.getActiveSession.get != newSession)
    // It's no problem to close the session multiple times
    newSession.close()

    // Raise SnowparkClientException to indicate the session has been closed after it is closed.
    var ex = intercept[SnowparkClientException] {
      newSession.sql("select current_timestamp()")
    }
    assert(ex.errorCode.equals("0414"))
    ex = intercept[SnowparkClientException] {
      newSession.range(10).collect()
    }
    assert(ex.errorCode.equals("0414"))
  }

  test("session.tableExists()") {
    val name1 = randomName()
    val name2 = randomName()
    createTable(name1, "c1 int")
    try {
      assert(session.tableExists(name1))
      assert(!session.tableExists(name2))
    } finally {
      dropTable(name1)
    }
  }

  // literal
  test("negative test") {
    val ex = intercept[SnowparkClientException] {
      Literal(this)
    }
    assert(
      ex.getMessage.contains("Cannot create a Literal for com.snowflake.snowpark." +
        "APIInternalSuite(APIInternalSuite)"))
  }

  test("special BigDecimal literals") {
    val normalScale = java.math.BigDecimal.valueOf(0.1)
    val smallScale = java.math.BigDecimal.valueOf(1, 5)
    val negativeScale = java.math.BigDecimal.valueOf(1, -5)

    val lit1 = Literal(normalScale)
    assert(lit1.dataTypeOption.get.toString.equals("Decimal(1, 1)"))
    val lit2 = Literal(smallScale)
    assert(lit2.dataTypeOption.get.toString.equals("Decimal(5, 5)"))
    val lit3 = Literal(negativeScale)
    assert(lit3.dataTypeOption.get.toString.equals("Decimal(6, 0)"))

    val df = session
      .range(2)
      .select(lit(normalScale), lit(smallScale), lit(negativeScale))
    assert(
      df.showString(10) ==
        """------------------------------------------------------------------------------------
          ||"0.1 ::  NUMBER (1, 1)"  |"0.00001 ::  NUMBER (5, 5)"  |"1E+5 ::  NUMBER (6, 0)"  |
          |------------------------------------------------------------------------------------
          ||0.1                      |0.00001                      |100000                    |
          ||0.1                      |0.00001                      |100000                    |
          |------------------------------------------------------------------------------------
          |""".stripMargin)
  }

  test("show structured types mix") {
    val query =
      // scalastyle:off
      """SELECT
        |  NULL :: OBJECT(a VARCHAR, b NUMBER) as object1,
        |  1 as NUM1,
        |  'abc' as STR1,
        |  {'a':1,'b':2} :: MAP(VARCHAR, NUMBER) as map1,
        |  {'1':'a','2':'b'} :: MAP(NUMBER, VARCHAR) as map2,
        |  {'a': 1, 'b': [1,2,3,4]} :: OBJECT(a NUMBER, b ARRAY(NUMBER)) as object2,
        |  [1, 2, 3]::ARRAY(NUMBER) AS arr1,
        |  [1.1, 2.2, 3.3]::ARRAY(FLOAT) AS arr2,
        |  {'a1':{'b':2}, 'a2':{'b':3}} :: MAP(VARCHAR, OBJECT(b NUMBER)) as map1
        |""".stripMargin
    // scalastyle:on
    val df = session.sql(query)
    // scalastyle:off
    assert(
      df.showString(10) ==
        """--------------------------------------------------------------------------------------------------------------------------------------------------
          ||"OBJECT1"  |"NUM1"  |"STR1"  |"MAP1"     |"MAP2"     |"OBJECT2"                     |"ARR1"   |"ARR2"         |"MAP1"                           |
          |--------------------------------------------------------------------------------------------------------------------------------------------------
          ||NULL       |1       |abc     |{b:2,a:1}  |{2:b,1:a}  |Object(a:1,b:Array(1,2,3,4))  |[1,2,3]  |[1.1,2.2,3.3]  |{a1:Object(b:2),a2:Object(b:3)}  |
          |--------------------------------------------------------------------------------------------------------------------------------------------------
          |""".stripMargin)
    // scalastyle:on
  }

  test("show object") {
    val query =
      // scalastyle:off
      """SELECT
        |  {'b': 1, 'a': '22'} :: OBJECT(a VARCHAR, b NUMBER) as object1,
        |  {'a': 1, 'b': [1,2,3,4]} :: OBJECT(a NUMBER, b ARRAY(NUMBER)) as object2,
        |  {'a': 1, 'b': [1,2,3,4], 'c': {'1':'a'}} :: OBJECT(a VARCHAR, b ARRAY(NUMBER), c MAP(NUMBER, VARCHAR)) as object3,
        |  {'a': {'b': {'a':10,'c': 1}}} :: OBJECT(a OBJECT(b OBJECT(c NUMBER, a NUMBER))) as object4,
        |  [{'a':1,'b':2},{'b':3,'a':4}] :: ARRAY(OBJECT(a NUMBER, b NUMBER)) as arr1,
        |  {'a1':{'b':2}, 'a2':{'b':3}} :: MAP(VARCHAR, OBJECT(b NUMBER)) as map1
        |""".stripMargin
    // scalastyle:on

    val df = session.sql(query)
    // scalastyle:off
    assert(
      df.showString(10) ==
        """----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          ||"OBJECT1"           |"OBJECT2"                     |"OBJECT3"                                    |"OBJECT4"                             |"ARR1"                             |"MAP1"                           |
          |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          ||Object(a:"22",b:1)  |Object(a:1,b:Array(1,2,3,4))  |Object(a:"1",b:Array(1,2,3,4),c:Map(1:"a"))  |Object(a:Object(b:Object(c:1,a:10)))  |[Object(a:1,b:2),Object(a:4,b:3)]  |{a1:Object(b:2),a2:Object(b:3)}  |
          |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          |""".stripMargin)
    // scalastyle:on

  }

  test("show structured map") {
    val query =
      """SELECT
        |  {'a':1,'b':2} :: MAP(VARCHAR, NUMBER) as map1,
        |  {'1':'a','2':'b'} :: MAP(NUMBER, VARCHAR) as map2,
        |  {'1':[1,2,3],'2':[4,5,6]} :: MAP(NUMBER, ARRAY(NUMBER)) as map3,
        |  {'1':{'a':1,'b':2},'2':{'c':3}} :: MAP(NUMBER, MAP(VARCHAR, NUMBER)) as map4,
        |  [{'a':1,'b':2},{'c':3}] :: ARRAY(MAP(VARCHAR, NUMBER)) as map5,
        |  {'a':1,'b':2} :: OBJECT as map0
        |""".stripMargin

    val df = session.sql(query)
    // scalastyle:off
    assert(
      df.showString(10) ==
        """---------------------------------------------------------------------------------------------------------
          ||"MAP1"     |"MAP2"     |"MAP3"                 |"MAP4"                 |"MAP5"             |"MAP0"     |
          |---------------------------------------------------------------------------------------------------------
          ||{b:2,a:1}  |{2:b,1:a}  |{2:[4,5,6],1:[1,2,3]}  |{2:{c:3},1:{a:1,b:2}}  |[{a:1,b:2},{c:3}]  |{          |
          ||           |           |                       |                       |                   |  "a": 1,  |
          ||           |           |                       |                       |                   |  "b": 2   |
          ||           |           |                       |                       |                   |}          |
          |---------------------------------------------------------------------------------------------------------
          |""".stripMargin)
    // scalastyle:on
  }

  test("show structured array") {
    val query =
      """SELECT
        |    [1, 2, 3]::ARRAY(NUMBER) AS arr1,
        |    [1.1, 2.2, 3.3]::ARRAY(FLOAT) AS arr2,
        |    [true, false]::ARRAY(BOOLEAN) AS arr3,
        |    ['a', 'b']::ARRAY(VARCHAR) AS arr4,
        |    [parse_json(31000000)::timestamp_ntz]::ARRAY(TIMESTAMP_NTZ) AS arr5,
        |    [TO_BINARY('SNOW', 'utf-8')]::ARRAY(BINARY) AS arr6,
        |    [TO_DATE('2013-05-17')]::ARRAY(DATE) AS arr7,
        |    [[1,2]]::ARRAY(ARRAY) AS arr9,
        |    [OBJECT_CONSTRUCT('name', 1)]::ARRAY(OBJECT) AS arr10,
        |    [[1, 2], [3, 4]]::ARRAY(ARRAY(NUMBER)) AS arr11,
        |    [1.234::DECIMAL(13, 5)]::ARRAY(DECIMAL(13,5)) as arr12,
        |    [time '10:03:56']::ARRAY(TIME) as arr21
        |""".stripMargin
    val df = session.sql(query)
    // scalastyle:off
    assert(
      df.showString(10) ==
        """---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          ||"ARR1"   |"ARR2"         |"ARR3"        |"ARR4"  |"ARR5"                   |"ARR6"        |"ARR7"        |"ARR9"  |"ARR10"      |"ARR11"        |"ARR12"    |"ARR21"     |
          |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          ||[1,2,3]  |[1.1,2.2,3.3]  |[true,false]  |[a,b]   |[1970-12-25 11:06:40.0]  |['534E4F57']  |[2013-05-17]  |[[      |[{           |[[1,2],[3,4]]  |[1.23400]  |[10:03:56]  |
          ||         |               |              |        |                         |              |              |  1,    |  "name": 1  |               |           |            |
          ||         |               |              |        |                         |              |              |  2     |}]           |               |           |            |
          ||         |               |              |        |                         |              |              |]]      |             |               |           |            |
          |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          |""".stripMargin)
    // scalastyle:on
  }

  // dataframe
  test("withColumn function uses * instead of full column name list") {
    import session.implicits._
    val df = Seq((1, 2), (2, 3)).toDF("a", "b")
    assert(
      df.withColumn("newCol", lit(1))
        .snowflakePlan
        .queries
        .last
        .sql
        .trim
        .startsWith("SELECT *, 1 :: int AS \"NEWCOL\" FROM"))

    // use full name list if replacing existing column
    assert(
      df.withColumn("a", lit(1))
        .snowflakePlan
        .queries
        .last
        .sql
        .trim
        .startsWith("SELECT \"B\", 1 :: int AS \"A\" FROM"))
  }

  test("union by name should not list all columns if not reorder") {
    import session.implicits._
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((3, 4)).toDF("a", "b")
    val df3 = Seq((5, 6)).toDF("b", "a")

    val result1 = df1.unionByName(df2)
    checkAnswer(result1, Seq(Row(1, 2), Row(3, 4)))
    assert(result1.snowflakePlan.queries.last.countString("SELECT \"A\", \"B\"") == 0)

    val result2 = df1.unionByName(df3)
    checkAnswer(result2, Seq(Row(1, 2), Row(6, 5)))
    assert(result2.snowflakePlan.queries.last.countString("SELECT \"A\", \"B\"") == 1)
  }

  test("union all by name should not list all columns if not reorder") {
    import session.implicits._
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((3, 4)).toDF("a", "b")
    val df3 = Seq((5, 6)).toDF("b", "a")

    val result1 = df1.unionAllByName(df2)
    checkAnswer(result1, Seq(Row(1, 2), Row(3, 4)))
    assert(result1.snowflakePlan.queries.last.countString("SELECT \"A\", \"B\"") == 0)

    val result2 = df1.unionAllByName(df3)
    checkAnswer(result2, Seq(Row(1, 2), Row(6, 5)))
    assert(result2.snowflakePlan.queries.last.countString("SELECT \"A\", \"B\"") == 1)
  }

  test("createDataFrame for large values: check plan") {
    testWithAlteredSessionParameter(() => {
      import session.implicits._
      val schema = StructType(Seq(StructField("ID", LongType)))
      val largeData = new ArrayBuffer[Row]()
      for (i <- 0 to 1024) {
        largeData.append(Row(i.toLong))
      }
      // With specific schema
      var df = session.createDataFrame(largeData, schema)
      assert(df.snowflakePlan.queries.size == 3)
      assert(df.snowflakePlan.queries(0).sql.trim().startsWith("CREATE  SCOPED TEMPORARY  TABLE"))
      assert(df.snowflakePlan.queries(1).sql.trim().startsWith("INSERT  INTO"))
      assert(df.snowflakePlan.queries(2).sql.trim().startsWith("SELECT"))
      assert(df.snowflakePlan.postActions.size == 1)
      checkAnswer(df.sort(col("id")), largeData, sort = false)

      // infer schema
      val inferData = new ArrayBuffer[Long]()
      for (i <- 0 to 1024) {
        inferData.append(i.toLong)
      }
      df = inferData.toDF("id2")
      assert(df.snowflakePlan.queries.size == 3)
      assert(df.snowflakePlan.queries(0).sql.trim().startsWith("CREATE  SCOPED TEMPORARY  TABLE"))
      assert(df.snowflakePlan.queries(1).sql.trim().startsWith("INSERT  INTO"))
      assert(df.snowflakePlan.queries(2).sql.trim().startsWith("SELECT"))
      assert(df.snowflakePlan.postActions.size == 1)
      checkAnswer(df.sort(col("id2")), largeData, sort = false)

    }, ParameterUtils.SnowparkUseScopedTempObjects, "true")
  }

  // functions

  test("seq") {
    val queries = session
      .generator(
        10,
        seq1(),
        seq1(false),
        seq2(),
        seq2(false),
        seq4(),
        seq4(false),
        seq8(),
        seq8(false))
      .snowflakePlan
      .queries

    assert(queries.size == 1)
    assert(
      queries.head.sql.contains("SELECT seq1(0), seq1(1), seq2(0), seq2(1), seq4(0)," +
        " seq4(1), seq8(0), seq8(1) FROM ( TABLE (GENERATOR(ROWCOUNT => 10)))"))
  }

  // This test DataFrame can't be defined in TestData,
  // because it breaks the test for UDFSuite
  lazy val multipleQueriesDF1: DataFrame = {
    val tableName1 = randomName()
    val queries = Seq(
      s"create temporary table $tableName1 (A int)",
      s"insert into $tableName1 values(1),(2),(3)",
      s"select * from $tableName1").map(Query(_))
    val attrs = Seq(Attribute("A", IntegerType, nullable = true))
    val postActions = Seq(Query(s"drop table if exists $tableName1", true))
    val plan =
      new SnowflakePlan(
        queries,
        schemaValueStatement(attrs),
        postActions,
        session,
        None,
        supportAsyncMode = true)

    new DataFrame(session, session.analyzer.resolve(plan), Seq())
  }

  // This test DataFrame can't be defined in TestData,
  // because it breaks the test for UDFSuite
  lazy val multipleQueriesDF2: DataFrame = {
    val tableName2 = randomName()
    val queries2 = Seq(
      s"create temporary table $tableName2 (A int, B string)",
      s"insert into $tableName2 values(1, 'a'), (2, 'b'), (3, 'c')",
      s"select * from $tableName2").map(Query(_))
    val attrs2 = Seq(
      Attribute("A", IntegerType, nullable = true),
      Attribute("B", StringType, nullable = true))
    val postActions2 = Seq(Query(s"drop table if exists $tableName2"))
    val plan2 =
      new SnowflakePlan(
        queries2,
        schemaValueStatement(attrs2),
        postActions2,
        session,
        None,
        supportAsyncMode = true)

    new DataFrame(session, session.analyzer.resolve(plan2), Seq())
  }

  test("test col(DataFrame) with multiple queries") {
    // Re-use multipleQueriesDF1 and multipleQueriesDF1 for testing
    // multipleQueriesDF1.show()
    // multipleQueriesDF2.show()

    val df1 = multipleQueriesDF1
    val df2 = multipleQueriesDF2
    val df3 = multipleQueriesDF1.filter(col("A") === 1)

    // SELECT 1 sub query
    df2.select(Column("A"), Column("B"), col(df3)).show()
    checkAnswer(
      df2.select(Column("A"), Column("B"), col(df3)),
      Seq(Row(1, "a", 1), Row(2, "b", 1), Row(3, "c", 1)))

    // SELECT 2 sub queries
    checkAnswer(
      df2.select(Column("A"), Column("B"), col(df3).as("s1"), col(df3).as("s2")),
      Seq(Row(1, "a", 1, 1), Row(2, "b", 1, 1), Row(3, "c", 1, 1)))

    // WHERE 2 sub queries
    checkAnswer(
      df2.filter(col("A") > col(df3) and col("A") < col(df1.groupBy().agg(max(col("A"))))),
      Seq(Row(2, "b")))

    // SELECT 2 sub queries + WHERE 2 sub queries
    checkAnswer(
      df2
        .select(col("A"), col("B"), col(df3).as("s1"), col(df1.filter(col("A") === 2)).as("s2"))
        .filter(col("A") > col(df1.groupBy().agg(mean(col("A")))) and
          col("A") <= col(df1.groupBy().agg(max(col("A"))))),
      Seq(Row(3, "c", 1, 2)))
  }

  test("explain") {
    columnNameHasSpecialCharacter.explain()
    val explainString = columnNameHasSpecialCharacter.explainString

    assert(explainString.contains("Query List"))
    assert(explainString.contains("SELECT\n  \"_1\" AS \"col %\",\n  \"_2\" AS \"col *\""))
    assert(explainString.contains("Logical Execution Plan"))

    // can't analyze multiple queries
    val tempTableName = randomName()
    val queries =
      Seq(s"create table $tempTableName (num int)", s"select * from $tempTableName")
        .map(Query(_))
    val plan =
      SnowflakePlan(
        queries,
        schemaValueStatement(Seq(Attribute("NUM", LongType))),
        session,
        None,
        supportAsyncMode = true)
    val df = new DataFrame(session, plan, Seq())
    df.explain()

    val explainString1 = df.explainString
    assert(explainString1.contains("create table"))
    assert(explainString1.contains("\n---\n"))
    assert(explainString1.contains("select\n  *\nfrom"))
    assert(!explainString1.contains("Logical Execution Plan"))
  }

  test("toString of RelationalGroupedDataFrame") {
    assert(RelationalGroupedDataFrame.GroupByType.toString == "GroupBy")
    assert(RelationalGroupedDataFrame.CubeType.toString == "Cube")
    assert(RelationalGroupedDataFrame.RollupType.toString == "Rollup")
    assert(RelationalGroupedDataFrame.PivotType(null, Seq.empty).toString == "Pivot")
  }

  test("non-select query composition, self union") {
    val tableName = randomName()
    try {
      session.runQuery(s"create or replace table $tableName (num int)")
      val df = session.sql("show tables")
      val union = df
        .union(df)
        .select(""""name"""")
        .filter(col(""""name"""") === tableName)
      assert(union.collect().length == 1)
      assert(union.snowflakePlan.queries.size == 3)
    } finally {
      session.runQuery(s"drop table if exists $tableName")
    }
  }

  test("non-select query composition, self unionAll") {
    val tableName = randomName()
    try {
      session.runQuery(s"create or replace table $tableName (num int)")
      val df = session.sql("show tables")
      val union = df
        .unionAll(df)
        .select(""""name"""")
        .filter(col(""""name"""") === tableName)
      assert(union.collect().length == 2)
      assert(union.snowflakePlan.queries.size == 3)
    } finally {
      session.runQuery(s"drop table if exists $tableName")
    }
  }

  test("only use result scan when composing queries") {
    val df: DataFrame = session.sql("show tables")
    assert(df.snowflakePlan.queries.last.sql == "show tables")
    assert(df.snowflakePlan.queries.length == 1)

    val df1 = df.select(""""name"""")
    assert(df1.snowflakePlan.queries.length == 2)
    assert(df1.snowflakePlan.queries.last.sql.contains("RESULT_SCAN"))
  }

  // Used temporary STAGE which is not supported by owner's mode stored proc yet
  test("show()/collect() with MISC commands", JavaStoredProcExcludeOwner) {
    val objectName = randomName()
    val stageName = randomName()
    val filePath = TestUtils.createTempJarFile(
      "com.snowflake.snowpark.test",
      "TestClass",
      "snowpark_test_",
      "test.jar")
    val fileName = TestUtils.getFileName(filePath)

    val miscCommands = Seq(
      s"create or replace temp stage $stageName",
      s"put file://${TestUtils.escapePath(filePath)} @$stageName",
      s"get @$stageName file://${TestUtils.tempDirWithEscape}",
      s"list @$stageName",
      s"remove @$stageName/$fileName",
      s"remove @$stageName/$fileName", // second REMOVE returns 0 rows.
      s"create temp table $objectName (c1 int)",
      s"drop table $objectName",
      s"create temp view $objectName (string) as select current_version()",
      s"drop view $objectName",
      s"show tables",
      s"drop stage $stageName")

    // Misc commands with show()
    miscCommands.foreach(session.sql(_).show())
    // Misc commands with collect()
    miscCommands.foreach(session.sql(_).collect())
    // Misc commands with session.conn.getResultAndMetadata
    miscCommands.foreach { query =>
      val (rows, meta) = session.conn.getResultAndMetadata(session.sql(query).snowflakePlan)
      assert(rows.length == 0 || rows(0).length == meta.size)
    }
  }

  // reader
  test("copy") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"

    val df = session.read
      .schema(userSchema)
      .option("on_error", "continue")
      .option("COMPRESSION", "none")
      .csv(testFileOnStage)

    // use copy
    assert(df.plan.queries.size == 3)
    assert(df.plan.queries(1).sql.contains("COPY"))
    assert(df.plan.postActions.size == 1)

    val df1 = session.read
      .schema(userSchema)
      .option("COMPRESSION", "none")
      .csv(testFileOnStage)

    // no copy
    assert(df1.plan.queries.size == 2)

    checkAnswer(df, Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))

    // fail to read since the file is not compressed.
    // return empty result since enable on_error = continue
    checkAnswer(
      session.read
        .schema(userSchema)
        .option("on_error", "continue")
        .option("COMPRESSION", "gzip")
        .csv(testFileOnStage),
      Seq())
  }

  // The constructor for AsyncJob/TypedAsyncJob is package private.
  test("AsyncJob/TypedAsyncJob internal") {
    val queryID = session.range(5).async.collect().getQueryId()
    // negative test for un-recognize type
    val asyncJob1 = new TypedAsyncJob[TestClassForTypeAsyncJob](queryID, session, None)
    val ex1 = intercept[SnowparkClientException] {
      asyncJob1.getResult()
    }
    assert(ex1.errorCode.equals("0319"))

    // negative test result doesn't match type
    val asyncJob2 = new TypedAsyncJob[Long](queryID, session, None)
    val ex2 = intercept[SnowparkClientException] {
      asyncJob2.getResult()
    }
    assert(ex2.errorCode.equals("0320") && ex2.message.contains("getLong"))
  }

  test("negative test for Merge return 1 rows") {
    val tableName = randomTableName()
    createTable(tableName, "k int")
    val target = session.table(tableName)
    val source = session.table(tableName)
    val mergeBuilder = target.merge(source, target("k") === source("k"))
    val ex1 = intercept[SnowparkClientException] {
      MergeBuilder.getMergeResult(Array.empty, mergeBuilder)
    }
    assert(ex1.errorCode.equals("0321"))
    val ex2 = intercept[SnowparkClientException] {
      MergeBuilder.getMergeResult(Array(Row(1), Row(2)), mergeBuilder)
    }
    assert(ex2.errorCode.equals("0321"))
  }

  def checkExecuteAndGetQueryId(df: DataFrame): Unit = {
    val query = Query.resultScanQuery(df.executeAndGetQueryId())
    val res = query.runQueryGetResult(session.conn, mutable.HashMap.empty[String, String], false)
    res.attributes
      .zip(df.snowflakePlan.attributes)
      .foreach(pair => assert(pair._1.toString == pair._2.toString))
    checkAnswer(df, res.rows.get, sort = false)

    // Test df.executeAndGetQueryId() with statement parameter
    checkExecuteAndGetQueryIdWithStatementParameter(df)
  }

  private def checkExecuteAndGetQueryIdWithStatementParameter(df: DataFrame): Unit = {
    val testQueryTagValue = s"test_query_tag_${Random.nextLong().abs}"
    val queryId = df.executeAndGetQueryId(Map("QUERY_TAG" -> testQueryTagValue))
    val rows = session
      .sql(
        s"select QUERY_TAG from table(information_schema.QUERY_HISTORY_BY_SESSION())" +
          s" where QUERY_ID = '$queryId'")
      .collect()
    assert(rows.length == 1 && rows(0).getString(0).equals(testQueryTagValue))
  }

  test("DataFrame.executeAndGetQueryId basic query") {
    checkExecuteAndGetQueryId(multipleQueriesDF1)
    checkExecuteAndGetQueryId(multipleQueriesDF2)
  }

  test("DataFrame.executeAndGetQueryId df from Seq") {
    import session.implicits._
    val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("col1", "col2")
    checkExecuteAndGetQueryId(df)
  }

  test("DataFrame.executeAndGetQueryId put/get") {
    val targetDirectoryFile = Files.createTempDirectory("snowpark_test_target_").toFile
    val path = TestUtils.escapePath(targetDirectoryFile.getCanonicalPath)
    try {
      val dfGet = session.sql(s"get @$tmpStageName file://$path")
      val queryGet = Query.resultScanQuery(dfGet.executeAndGetQueryId())
      val getEx = intercept[SnowflakeSQLException] {
        queryGet.runQueryGetResult(session.conn, mutable.HashMap.empty[String, String], false)
      }
      assert(
        getEx.getMessage.contains("Result for query")
          && getEx.getMessage.contains("has expired"))

      val dfPut =
        session.sql(s"put file://$path/$testFileCsv @$tmpStageName/testExecuteAndGetQueryId")
      val queryPut = Query.resultScanQuery(dfPut.executeAndGetQueryId())
      val putEx = intercept[SnowflakeSQLException] {
        queryPut.runQueryGetResult(session.conn, mutable.HashMap.empty[String, String], false)
      }
      assert(
        putEx.getMessage.contains("Result for query")
          && putEx.getMessage.contains("has expired"))
    } finally {
      TestUtils.removeFile(path, session)
    }
  }

  test("DataFrame.executeAndGetQueryId copy") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"

    val df = session.read
      .schema(userSchema)
      .option("on_error", "continue")
      .option("COMPRESSION", "none")
      .csv(testFileOnStage)
    checkExecuteAndGetQueryId(df)
  }

  test("DataFrame.executeAndGetQueryId large local relation") {
    val schema = StructType(
      Seq(
        StructField("ID", LongType),
        StructField("string", StringType),
        StructField("byte", ByteType),
        StructField("short", ShortType),
        StructField("int", IntegerType),
        StructField("long", LongType),
        StructField("float", FloatType),
        StructField("double", DoubleType),
        StructField("decimal", DecimalType(10, 3)),
        StructField("boolean", BooleanType),
        StructField("binary", BinaryType),
        StructField("timestamp", TimestampType),
        StructField("date", DateType)))

    val timestamp: Long = 1606179541282L

    val largeData = new ArrayBuffer[Row]()
    for (i <- 0 to 1024) {
      largeData.append(
        Row(
          i.toLong,
          "a",
          1.toByte,
          2.toShort,
          3,
          4L,
          1.1F,
          1.2D,
          new java.math.BigDecimal(1.2),
          true,
          Array(1.toByte, 2.toByte),
          new Timestamp(timestamp - 100),
          new Date(timestamp - 100)))
    }
    largeData.append(
      Row(1025, null, null, null, null, null, null, null, null, null, null, null, null))

    val df = session.createDataFrame(largeData, schema)
    checkExecuteAndGetQueryId(df)

    // Statement parameters are applied for all queries.
    val uniqueQueryTag = s"test_query_tag_${Random.nextLong().abs}"
    df.executeAndGetQueryId(Map("QUERY_TAG" -> uniqueQueryTag))
    val rows = session
      .sql(
        s"select QUERY_TAG from table(information_schema.QUERY_HISTORY_BY_SESSION())" +
          s" where QUERY_TAG = '$uniqueQueryTag'")
      .collect()
    // The statement parameter is applied for the last query only,
    // even if there are 3 queries and 1 post actions for large local relation,
    assert(rows.length == 1)
  }

  test("DataFrame.executeAndGetQueryId with statement parameters") {
    // case 1: statement parameters are applied for all queries.
    val uniqueQueryTag = s"test_query_tag_${Random.nextLong().abs}"
    multipleQueriesDF1.executeAndGetQueryId(Map("QUERY_TAG" -> uniqueQueryTag))
    val rows = session
      .sql(
        s"select QUERY_TAG from table(information_schema.QUERY_HISTORY_BY_SESSION())" +
          s" where QUERY_TAG = '$uniqueQueryTag'")
      .collect()
    // The statement parameter is applied for the last query only,
    // even if there are 3 queries and 1 post actions for multipleQueriesDF1
    assert(rows.length == 1)

    // case 2: test int/boolean parameter
    multipleQueriesDF1.executeAndGetQueryId(
      Map("STATEMENT_TIMEOUT_IN_SECONDS" -> 100, "USE_CACHED_RESULT" -> false))
  }

  test("VariantTypes.getType") {
    assert(Variant.VariantTypes.getType("RealNumber") == Variant.VariantTypes.RealNumber)
    assert(Variant.VariantTypes.getType("FixedNumber") == Variant.VariantTypes.FixedNumber)
    assert(Variant.VariantTypes.getType("Boolean") == Variant.VariantTypes.Boolean)
    assert(Variant.VariantTypes.getType("String") == Variant.VariantTypes.String)
    assert(Variant.VariantTypes.getType("Binary") == Variant.VariantTypes.Binary)
    assert(Variant.VariantTypes.getType("Time") == Variant.VariantTypes.Time)
    assert(Variant.VariantTypes.getType("Date") == Variant.VariantTypes.Date)
    assert(Variant.VariantTypes.getType("Timestamp") == Variant.VariantTypes.Timestamp)
    assert(Variant.VariantTypes.getType("Array") == Variant.VariantTypes.Array)
    assert(Variant.VariantTypes.getType("Object") == Variant.VariantTypes.Object)
    intercept[Exception] { Variant.VariantTypes.getType("not_exist_type") }
  }

  test("HasCachedResult doesn't cache again") {
    import session.implicits._
    val df = Seq(1, 2).toDF("a")
    val cachedResult = df.cacheResult()
    val expected = Seq(Row(1), Row(2))
    checkAnswer(cachedResult, expected)
    val cachedResult2 = cachedResult.cacheResult()
    assert(
      cachedResult.snowflakePlan.queries.last.sql ==
        cachedResult2.snowflakePlan.queries.last.sql)
    checkAnswer(cachedResult2, expected)
  }

  test("Logical Plan Summary") {
    import session.implicits._
    val df = Seq(1).toDF("a")
    val plan = df.select("a").filter(df("a") > 1).plan
    assert(plan.summarize == "Filter(Project(Project(Project(SnowflakeValues()))))")

    val plan1 = df.filter(df("a") > 1).union(df.select("a")).plan
    assert(
      plan1.summarize ==
        "Union(Filter(Project(Project(SnowflakeValues())))" +
          ",Project(Project(Project(SnowflakeValues()))))")
  }

  test("DataFrame toDF should not generate useless project") {
    import session.implicits._
    val df = Seq((1, 2)).toDF("a", "b")
    val result1 = df.toDF("b", "a")
    assert(
      result1.snowflakePlan.queries.last
        .countString("SELECT \"A\" AS \"B\", \"B\" AS \"A\" FROM") == 1)
    val result2 = df.toDF("a", "B")
    assert(result2.eq(df))
    assert(result2.snowflakePlan.queries.last.countString("\"A\" AS \"A\"") == 0)
  }
}

private[snowpark] class TestClassForTypeAsyncJob
