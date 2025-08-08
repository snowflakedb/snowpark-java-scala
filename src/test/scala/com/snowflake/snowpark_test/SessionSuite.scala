package com.snowflake.snowpark_test

import com.snowflake.snowpark.internal.analyzer.quoteName
import com.snowflake.snowpark.internal.{SnowparkSFConnectionHandler, Utils}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.{SnowflakeConnectionV1, SnowflakeDriver, SnowflakeSQLException}
import com.snowflake.snowpark.functions._
import java.io.File

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper

class SessionSuite extends SNTestBase {
  import session.implicits._
  val randomSchema = randomName()

  override def afterAll: Unit = {
    runQuery(s"drop schema if exists $randomSchema", session)
    super.afterAll
  }

  test("Test for getDependencies") {
    assert(session.getDependencies sameElements Set.empty)
    val snClassDir = classOf[Session].getProtectionDomain.getCodeSource.getLocation.getPath
    val scalaClassDir =
      classOf[scala.Product].getProtectionDomain.getCodeSource.getLocation.getPath
    session.addDependency(snClassDir)
    session.addDependency(scalaClassDir)
    val result = Set(snClassDir, scalaClassDir).map((new File(_).toURI))
    // scalastyle:off println
    println(session.getDependencies)
    println(result)
    // scalastyle:on println
    assert(session.getDependencies sameElements result)
  }

  test("Test for create with multiple threads") {
    val t1 = new Thread {
      override def run {
        val t1Session = Session.builder.configFile(defaultProfile).create
        assert(t1Session != session)
      }
    }

    val t2 = new Thread {
      override def run {
        val t2Session = Session.builder.configFile(defaultProfile).create
        assert(t2Session != session)
      }
    }

    t1.run()
    t2.run()
  }

  test("Test for get or create session") {
    val session1 = Session.builder.getOrCreate
    val session2 = Session.builder.getOrCreate
    assert(session1 == session2)
  }

  test("Test for invalid configs") {
    val badSessionBuilder = Session.builder
      .configFile(defaultProfile)
      .config("DB", "badNotExistingDB")
    val ex = intercept[SnowflakeSQLException] {
      badSessionBuilder.create
    }
    assert(ex.getMessage.contains("does not exist"))
  }

  test("Test for default and current db and schema") {
    val defaultDB = session.getDefaultDatabase
    val defaultSchema = session.getDefaultSchema

    assert(equalsIgnoreCase(session.getDefaultDatabase, session.getCurrentDatabase))
    assert(equalsIgnoreCase(session.getDefaultSchema, session.getCurrentSchema))

    runQuery(s"create schema $randomSchema", session)

    assert(equalsIgnoreCase(session.getDefaultDatabase, defaultDB))
    assert(equalsIgnoreCase(session.getDefaultSchema, defaultSchema))

    assert(equalsIgnoreCase(session.getCurrentDatabase, defaultDB))
    assert(equalsIgnoreCase(session.getCurrentSchema, Some(quoteName(randomSchema))))
  }

  test("Quote all db and schema names") {
    def isQuoted(name: String): Boolean = name.startsWith("\"") && name.endsWith("\"")
    assert(isQuoted(session.getDefaultSchema.get))
    assert(isQuoted(session.getDefaultDatabase.get))
    assert(isQuoted(session.getCurrentSchema.get))
    assert(isQuoted(session.getCurrentDatabase.get))
  }

  test("createDataFrame sequence") {
    var df = session.createDataFrame(Seq((1, "one", 1.0), (2, "two", 2.0)))
    assert(df.schema.map(_.name).toSeq == Seq("_1", "_2", "_3"))
    checkAnswer(df, Row(1, "one", 1.0) :: Row(2, "two", 2.0) :: Nil)

    df = session.createDataFrame(Seq(1, 2))
    assert(df.schema.map(_.name).toSeq == Seq("VALUE"))
    checkAnswer(df, Row(1) :: Row(2) :: Nil)

    df = session.createDataFrame(Seq("one", "two"))
    assert(df.schema.map(_.name).toSeq == Seq("VALUE"))
    checkAnswer(df, Row("one") :: Row("two") :: Nil)
  }

  case class P1(a: Int, b: String, c: Double)
  test("createDataFrame case class") {
    var df = session.createDataFrame(Seq(P1(1, "one", 1.0), P1(2, "two", 2.0)))
    assert(df.schema.map(_.name).toSeq == Seq("A", "B", "C"))
  }

  test("session builder shouldn't be reused") {
    Session.builder.configFile(defaultProfile)
    assertThrows[IllegalArgumentException](Session.builder.create)
  }

  test("Negative test for missing required parameter: SCHEMA") {
    val allProperties = loadConfFromFile(defaultProfile)
    val propertiesWithoutSchema = allProperties - "SCHEMA"

    val ex = intercept[SnowparkClientException] {
      val newSession = Session.builder.configs(propertiesWithoutSchema).create
      newSession.getFullyQualifiedCurrentSchema
    }
    assert(ex.getMessage.contains("The SCHEMA is not set for the current session."))
  }

  test("select current_client()") {
    val currentClient = getShowString(session.sql("select current_client()"), 10)
    assert(currentClient contains "Snowpark")
    assert(
      currentClient contains SnowparkSFConnectionHandler.extractValidVersionNumber(Utils.Version))
  }

  test("negative test to input invalid table name for Session.table()") {
    val ex = intercept[SnowparkClientException] {
      session.table("negative.test.invalid.table.name")
    }
    assert(ex.getMessage.matches(".*The object name .* is invalid."))
  }

  test("create DataFrame from Seq(None)") {
    import session.implicits._
    checkAnswer(Seq(None, Some(1)).toDF("int"), Seq(Row(null), Row(1)))

    checkAnswer(Seq(None, Some(Array(1, 2))).toDF("arr"), Seq(Row(null), Row("[\n  1,\n  2\n]")))
  }

  test("create dataframe from array") {
    val data = Array(Row(1, "a"), Row(2, "b"))
    val schema = StructType(Seq(StructField("num", IntegerType), StructField("str", StringType)))
    val df = session.createDataFrame(data, schema)
    checkAnswer(df, data)

    // negative
    val data1 = Array(Row("a", 1), Row(2, "b"))
    val err =
      intercept[UnsupportedOperationException](session.createDataFrame(data1, schema).collect())
    assert(err.getMessage.contains("Unsupported datatype"))
  }

  test("DataFrame created before session close are not usable after closing the session") {
    val newSession = Session.builder.configFile(defaultProfile).create
    val df = newSession.range(10)
    val read = newSession.read
    newSession.close()

    var ex = intercept[SnowparkClientException] {
      df.collect()
    }
    assert(ex.errorCode.equals("0414"))
    ex = intercept[SnowparkClientException] {
      read.json("@mystage/prefix")
    }
    assert(ex.errorCode.equals("0414"))
  }

  test("load table from array multipart identifier") {
    val name = randomName()
    try {
      createTable(name, "col int")
      val db = session.getCurrentDatabase.get
      val sc = session.getCurrentSchema.get
      val multipart = Array(db, sc, name)
      assert(session.table(multipart).schema.length == 1)
    } finally {
      dropTable(name)
    }
  }

  test("Session level queryTag") {
    val queryTag = randomName()
    session.setQueryTag(queryTag)
    assert(session.getQueryTag().get.equals(queryTag))
    session.createDataFrame(Seq(1, 2, 3)).collect()
    // Unset  before fetching query history
    session.unsetQueryTag()
    val queries = getQueryHistoryForTags(queryTag, session)
    assert(queries.length == 1)
  }

  test("Tests for code stack in query tag") {
    // Unset  first
    session.unsetQueryTag()
    val tableName = randomName()
    createTable(tableName, "a int, b int")
    val query = s"select * from $tableName"
    val df = session.sql(query)
    df.collect()

    // Find the query above by searching by name
    val queryTag = getQueryTagForQuery(query, session)

    assert(queryTag.contains("com.snowflake.snowpark_test.SessionSuite"))

  }

  test("Tests for query tag for cacheResult") {
    val queryTag = randomName()
    session.setQueryTag(queryTag)
    session.createDataFrame(Seq(1, 2, 3)).cacheResult()
    // Unset  before fetching query history
    session.unsetQueryTag()
    val queries = getQueryHistoryForTags(queryTag, session)
    assert(queries.length == 2)
  }

  test("Tests for Session.setQueryTag") {
    val queryTag = randomName()
    session.setQueryTag(queryTag)
    assert(getParameterValue("query_tag", session) == queryTag)

    // Set to another random value
    val queryTag2 = randomName()
    session.setQueryTag(queryTag2)
    assert(getParameterValue("query_tag", session) == queryTag2)
  }

  test("updateQueryTag when adding new key-value pairs") {
    val queryTag1 = """{"key1":"value1"}"""
    session.setQueryTag(queryTag1)

    val queryTag2 = """{"key2":"value2","key3":{"key4":0},"key5":{"key6":"value6"}}"""
    session.updateQueryTag(queryTag2)

    val expected = {
      """{"key1":"value1","key2":"value2","key3":{"key4":0},"key5":{"key6":"value6"}}"""
    }
    val actual = getParameterValue("query_tag", session)
    assert(actual == expected)
  }

  test("updateQueryTag when updating an existing key-value pair") {
    val queryTag1 = """{"key1":"value1","key2":"value2","key3":"value3"}"""
    session.setQueryTag(queryTag1)

    val queryTag2 = """{"key2":"newValue2"}"""
    session.updateQueryTag(queryTag2)

    val expected = """{"key1":"value1","key2":"newValue2","key3":"value3"}"""
    val actual = getParameterValue("query_tag", session)
    assert(actual == expected)
  }

  test("updateQueryTag when the query tag of the current session is empty") {
    session.setQueryTag("")

    val queryTag = """{"key1":"value1"}"""
    session.updateQueryTag(queryTag)

    val actual = getParameterValue("query_tag", session)
    assert(actual == queryTag)
  }

  test("updateQueryTag when the given query tag is not a valid JSON") {
    val queryTag = "tag1"
    val exception = intercept[SnowparkClientException](session.updateQueryTag(queryTag))
    assert(
      exception.message.startsWith(
        "Error Code: 0426, Error message: The given query tag must be a valid JSON string. " +
          "Ensure it's correctly formatted as JSON."))
  }

  test("updateQueryTag when the query tag of the current session is not a valid JSON") {
    val queryTag1 = "tag1"
    session.setQueryTag(queryTag1)

    val queryTag2 = """{"key1":"value1"}"""
    val exception = intercept[SnowparkClientException](session.updateQueryTag(queryTag2))
    assert(
      exception.message.startsWith(
        "Error Code: 0427, Error message: The query tag of the current session must be a valid " +
          "JSON string. Current query tag: tag1"))
  }

  test("updateQueryTag when the query tag of the current session is set with an ALTER SESSION") {
    val queryTag1 = """{"key1":"value1"}"""
    session.sql(s"ALTER SESSION SET QUERY_TAG = '$queryTag1'").collect()

    val queryTag2 = """{"key2":"value2"}"""
    session.updateQueryTag(queryTag2)

    val expected = """{"key1":"value1","key2":"value2"}"""
    val actual = getParameterValue("query_tag", session)
    assert(actual == expected)
  }

  test("Multiple queries test for query tags") {
    val queryTag = randomName()
    session.setQueryTag(queryTag)
    session.createDataFrame(Seq(1, 2, 3)).collect()
    session.createDataFrame(Seq("a", "b", "c")).collect()
    // Unset  before fetching query history
    session.unsetQueryTag()
    val queries = getQueryHistoryForTags(queryTag, session)
    assert(queries.length == 2)

  }

  test("Set an app name in the query tag") {
    val appName = "my_app"
    val expectedAppName = s"""{"APPNAME":"$appName"}"""
    val newSession = Session.builder.appName(appName).configFile(defaultProfile).create
    assert(getParameterValue("query_tag", newSession) == expectedAppName)
  }

  test("The app name is not defined") {
    val newSession = Session.builder.configFile(defaultProfile).create
    assert(getParameterValue("query_tag", newSession) == "")
  }

  test("generator") {
    checkAnswer(
      session.generator(3, Seq(lit(1).as("a"), lit(2).as("b"))),
      Seq(Row(1, 2), Row(1, 2), Row(1, 2)))

    checkAnswer(
      session.generator(3, lit(1).as("a"), lit(2).as("b")),
      Seq(Row(1, 2), Row(1, 2), Row(1, 2)))

    val msg = intercept[SnowparkClientException](session.generator(3, Seq.empty))
    assert(msg.message.contains("The column list of generator function can not be empty"))
  }

  test("join, union on generator") {
    import functions.seq2
    val df1 = session.generator(2, seq2())
    val df2 = session.generator(3, seq2())

    checkAnswer(df1.unionAll(df2), Seq(Row(0), Row(0), Row(1), Row(1), Row(2)))

    checkAnswer(df1.toDF("a").join(df2.toDF("b"), col("a") === col("b")), Seq(Row(0, 0), Row(1, 1)))
  }

  test("get session info") {
    val mapper = new ObjectMapper()
    val jsonSessionInfo = mapper.readTree(session.getSessionInfo())
    assert(jsonSessionInfo.get("snowpark.version").asText().equals(Utils.Version))
    assert(jsonSessionInfo.get("java.version").asText().equals(Utils.JavaVersion))
    assert(jsonSessionInfo.get("scala.version").asText().equals(Utils.ScalaVersion))
    assert(
      jsonSessionInfo
        .get("jdbc.session.id")
        .asText()
        .equals(session.jdbcConnection.asInstanceOf[SnowflakeConnectionV1].getSessionID))
    assert(jsonSessionInfo.get("os.name").asText().equals(Utils.OSName))
    assert(jsonSessionInfo.get("jdbc.version").asText().equals(SnowflakeDriver.implementVersion))
    assert(jsonSessionInfo.get("snowpark.library").asText().nonEmpty)
    assert(jsonSessionInfo.get("scala.library").asText().nonEmpty)
    assert(jsonSessionInfo.get("jdbc.library").asText().nonEmpty)
  }

  test("CREATE TEMP TABLE doesn't commit open transaction") {
    val tableName = randomTableName()

    try {
      session.sql("begin").collect()
      assert(isActiveTransaction(session))
      // large local relation create/drop temp internally.
      (1 to 10000).toDF("ID").collect()
      assert(isActiveTransaction(session))
      session.sql("commit").collect()
      assert(!isActiveTransaction(session))
    } finally {
      dropTable(tableName)
    }

    try {
      session.sql(s"create or replace table $tableName (c1 int)").collect()
      session.sql(s"insert into $tableName values(1),(2)").collect()
      session.sql("begin").collect()
      assert(isActiveTransaction(session))
      // DataFrame Cache use temp table
      session.table(tableName).cacheResult()
      assert(isActiveTransaction(session))
      session.sql("commit").collect()
      assert(!isActiveTransaction(session))
    } finally {
      dropTable(tableName)
    }
  }
}
