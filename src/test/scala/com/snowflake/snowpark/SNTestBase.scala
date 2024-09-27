package com.snowflake.snowpark

import java.sql.{Statement, Types}
import java.util.TimeZone
import com.snowflake.snowpark.internal.ParameterUtils.SnowparkLazyAnalysis
import com.snowflake.snowpark.internal.analyzer.Query
import com.snowflake.snowpark.internal.{ParameterUtils, ServerConnection, UDFClassPath}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark_test.TestFiles
import org.mockito.Mockito.{doReturn, spy, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

trait SNTestBase extends AnyFunSuite with BeforeAndAfterAll with SFTestUtils with SnowTestFiles {

  protected val defaultProfile: String = TestUtils.defaultProfile

  protected def randomName(): String = randomTableName()
  private val randomTempSchema = "SCHEMA_" + randomName()

  // Defined as a function so that it can be overwritten
  // by Java Stored Proc test
  def randTempSchema: String = randomTempSchema

  // Defined as a function so that it can be overwritten
  // by Java Stored Proc test
  def isJavaStoredProcOwnersRightTest: Boolean = false

  protected def getFullyQualifiedTempSchema(): String = {
    session.getCurrentDatabase.get + "." + randTempSchema
  }

  def getParameterValue(param: String, sess: Session): String = sess.conn.getParameterValue(param)

  def runQuery(sql: String, sess: Session): Unit = sess.runQuery(sql)

  def runQueryReturnStatement(sql: String, sess: Session): Statement =
    TestUtils.runQueryReturnStatement(sql, sess)

  lazy val isPreprodAccount: Boolean =
    !session.sql("select current_account()").collect().head.getString(0).contains("SFCTEST0")

  case class TypeMap(colName: String, sfType: String, jdbcType: Int, tsType: DataType)
  lazy val typeMap = List(
    TypeMap("number", "number(10,2)", Types.DECIMAL, DecimalType(10, 2)),
    TypeMap("decimal", "decimal(38,0)", Types.BIGINT, LongType),
    TypeMap("numeric", "numeric(0,0)", Types.BIGINT, LongType),
    TypeMap("int", "int", Types.BIGINT, LongType),
    TypeMap("integer", "integer", Types.BIGINT, LongType),
    TypeMap("bigint", "bigint", Types.BIGINT, LongType),
    TypeMap("smallint", "smallint", Types.BIGINT, LongType),
    TypeMap("tinyint", "tinyint", Types.BIGINT, LongType),
    TypeMap("byteint", "byteint", Types.BIGINT, LongType),
    TypeMap("float", "float", Types.DOUBLE, DoubleType),
    TypeMap("float4", "float4", Types.DOUBLE, DoubleType),
    TypeMap("float8", "float8", Types.DOUBLE, DoubleType),
    TypeMap("double", "double", Types.DOUBLE, DoubleType),
    TypeMap("doubleprecision", "double precision", Types.DOUBLE, DoubleType),
    TypeMap("real", "real", Types.DOUBLE, DoubleType),
    TypeMap("varchar", "varchar", Types.VARCHAR, StringType),
    TypeMap("char", "char", Types.VARCHAR, StringType),
    TypeMap("character", "character", Types.VARCHAR, StringType),
    TypeMap("string", "string", Types.VARCHAR, StringType),
    TypeMap("text", "text", Types.VARCHAR, StringType),
    TypeMap("binary", "binary", Types.BINARY, BinaryType),
    TypeMap("varbinary", "varbinary", Types.BINARY, BinaryType),
    TypeMap("boolean", "boolean", Types.BOOLEAN, BooleanType),
    TypeMap("date", "date", Types.DATE, DateType),
    TypeMap("datetime", "datetime", Types.TIMESTAMP, TimestampType),
    TypeMap("time", "time", Types.TIME, TimeType),
    TypeMap("timestamp", "timestamp", Types.TIMESTAMP, TimestampType),
    TypeMap("timestamp_ltz", "timestamp_ltz", Types.TIMESTAMP, TimestampType),
    TypeMap("timestamp_ntz", "timestamp_ntz", Types.TIMESTAMP, TimestampType),
    TypeMap("timestamp_tz", "timestamp_tz", Types.TIMESTAMP_WITH_TIMEZONE, TimestampType),
    TypeMap("variant", "variant", Types.VARCHAR, VariantType),
    TypeMap("object", "object", Types.VARCHAR, MapType(StringType, StringType)),
    TypeMap("array", "array", Types.VARCHAR, ArrayType(StringType)),
    TypeMap("geography", "geography", Types.VARCHAR, GeographyType),
    TypeMap("geometry", "geometry", Types.VARCHAR, GeometryType))

  implicit lazy val session: Session = {
    TestUtils.tryToLoadFipsProvider()
    val session = Session.builder
      .configFile(defaultProfile)
      .create
    // trigger this lazy parameter to make some tests more stable
    // e.g. count queries
    session.conn.hideInternalAlias
    session
  }

  def equalsIgnoreCase(a: Option[String], b: Option[String]): Boolean = {
    (a, b) match {
      case (Some(l), Some(r)) => l.equalsIgnoreCase(r)
      case _ => a == b
    }
  }

  def checkAnswer(df1: DataFrame, df2: DataFrame, sort: Boolean): Unit = {
    if (sort) {
      assert(TestUtils.compare(df1.collect().sortBy(_.toString), df2.collect().sortBy(_.toString)))
    } else {
      assert(TestUtils.compare(df1.collect(), df2.collect()))
    }
  }

  def checkAnswer(df: DataFrame, result: Row): Unit =
    checkResult(df.collect(), Seq(result), false)

  def checkAnswer(df: DataFrame, result: Seq[Row], sort: Boolean = true): Unit =
    checkResult(df.collect(), result, sort)

  def checkResult(result: Array[Row], expected: Seq[Row], sort: Boolean = true): Unit =
    TestUtils.checkResult(result, expected, sort)
  def checkResultIterator(result: Iterator[Row], expected: Seq[Row], sort: Boolean = true): Unit =
    checkResult(result.toArray, expected, sort)

  private def readPropertyFromFile(key: String): Option[String] = {
    // scalastyle:off
    Session
      .loadConfFromFile(defaultProfile)
      .map { case (key, value) =>
        key.toLowerCase -> value
      }
      .get(key.toLowerCase)
    // scalastyle:on
  }

  def getUserFromProperties: String = readPropertyFromFile("user").get

  def getDatabaseFromProperties: String = readPropertyFromFile("db").get

  def getSchemaFromProperties: String = readPropertyFromFile("schema").get

  def getWarehouseFromProperties: String = readPropertyFromFile("Warehouse").get

  def testCanceled[T](thunk: => T): Future[Boolean] = Future {
    val query = Future {
      try {
        thunk
        false
      } catch {
        case _: Exception => true // canceled
      }
    }
    Await.result(query, 10 minutes)
  }

  // Only create temp schema for non-java-sp tests, because owner's right SP does not support 'use'
  override def beforeAll: Unit = {
    if (!isJavaStoredProcOwnersRightTest) {
      val currentSchema = session.getFullyQualifiedCurrentSchema
      session.runQuery(s"create schema if not exists $randTempSchema")
      session.runQuery(s"use $currentSchema")
    }
  }

  override def afterAll: Unit = {
    if (!isJavaStoredProcOwnersRightTest) {
      session.runQuery(s"drop schema if exists $randTempSchema cascade")
    }
  }

  // Added a boolean to skip the test code if the parameter does not exist on server yet
  def testWithAlteredSessionParameter[T](
      thunk: => T,
      parameter: String,
      value: String,
      skipIfParamNotExist: Boolean = false): Unit = {
    var parameterNotExist = false
    try {
      session.runQuery(s"alter session set $parameter = $value")
      thunk
    } catch {
      case e: Throwable =>
        if (!skipIfParamNotExist || !e.getMessage.contains(s"invalid parameter '$parameter'")) {
          throw e
        }
        parameterNotExist = true
    }
    if (!parameterNotExist) {
      // best effort to unset the parameter.
      session.runQuery(s"alter session unset $parameter")
    }
  }

  def testWithTimezone[T](thunk: => T, timezone: String): T = {
    val defaultTimezone = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(timezone))
      thunk
    } finally {
      TimeZone.setDefault(defaultTimezone)
    }
  }

  def getShowString(df: DataFrame, n: Int, maxWidth: Int = 50): String =
    df.showString(n, maxWidth)

  def createDataFrame(df: DataFrame): DataFrame = DataFrame(df.session, df.plan)

  def getSchemaString(schema: StructType): String = schema.treeString(0)

  def getDataType(
      sqlType: Int,
      columnTypeName: String,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType =
    ServerConnection.getDataType(sqlType, columnTypeName, precision, scale, signed)

  def loadConfFromFile(path: String): Map[String, String] = Session.loadConfFromFile(path)

  def isStoredProc(sess: Session): Boolean = session.conn.isStoredProc

  def getTimeZone(sess: Session): String = sess.conn.getParameterValue("TIMEZONE")

  def getQueryHistoryForTags(tag: String, sess: Session): List[String] = {
    val statement = runQueryReturnStatement(
      s"select query_text from " +
        s"table(information_schema.QUERY_HISTORY_BY_SESSION()) " +
        s"where query_tag ='$tag'",
      sess)
    val result = statement.getResultSet
    val resArray = new ArrayBuffer[String]()
    while (result.next()) {
      resArray += result.getString(1)
    }
    statement.close()
    resArray.toList
  }

  def getQueryTagForQuery(queryText: String, session: Session): String = {
    val statement = runQueryReturnStatement(
      s"select query_tag from " +
        s"table(information_schema.QUERY_HISTORY_BY_SESSION()) " +
        s"where query_text ilike '%$queryText%'",
      session)
    val result = statement.getResultSet
    val resArray = new ArrayBuffer[String]()

    while (result.next) {
      val value = result.getString(1)
      if (value != null && !value.isEmpty()) {
        resArray += value
      }
    }
    statement.close()
    if (resArray.length > 1) {
      throw new IllegalArgumentException(s"Found more than 1 queryTag $resArray")
    }
    resArray.head
  }

  // "SELECT CURRENT_TRANSACTION()" returns a valid TX ID if there is active TX,
  // otherwise it returns NULL
  def isActiveTransaction(sess: Session): Boolean =
    !sess.sql("SELECT CURRENT_TRANSACTION()").collect()(0).isNullAt(0)

  implicit class QueryTestFunc(query: Query) {
    // count how many the given string token contained by this query
    // the input string is not a regex, and case sensitive.
    def countString(str: String): Int = {
      val targetLength = str.length
      val queryString = query.sql
      val queryLength = queryString.length
      if (queryLength == 0 || targetLength == 0 || queryLength < targetLength) {
        0
      } else {
        var count = 0
        (0 to (queryLength - targetLength)).foreach(i => {
          if (queryString.substring(i, i + targetLength) == str) {
            count += 1
          }
        })
        count
      }
    }
  }

  def withSessionParameters(
      params: Seq[(String, String)],
      currentSession: Session,
      skipPreprod: Boolean = false)(thunk: => Unit): Unit = {
    if (!(skipPreprod && isPreprodAccount)) {
      try {
        params.foreach { case (paramName, value) =>
          runQuery(s"alter session set $paramName = $value", currentSession)
        }
        thunk
      } finally {
        params.foreach { case (paramName, _) =>
          runQuery(s"alter session unset $paramName", currentSession)
        }
      }
    }
  }

  def structuredTypeTest(thunk: => Unit)(implicit currentSession: Session): Unit = {
    withSessionParameters(
      Seq(
        ("ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE", "true"),
        ("IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE", "true"),
        ("FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT", "true"),
        ("ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT", "true"),
        ("ENABLE_STRUCTURED_TYPES_IN_BINDS", "enable")),
      currentSession,
      skipPreprod = true)(thunk)
    // disable these tests on preprod daily tests until these parameters are enabled by default.
  }
}

trait SnowTestFiles {
  protected val testFileCsv: String = TestFiles.testFileCsv
  protected val testFile2Csv: String = "test2CSV.csv"
  protected val testFileCsvColon: String = TestFiles.testFileCsvColon
  protected val testFileCsvQuotes: String = "testCSVquotes.csv"
  protected val testFileJson: String = TestFiles.testFileJson
  protected val testFileAvro: String = TestFiles.testFileAvro
  protected val testFileParquet: String = TestFiles.testFileParquet
  protected val testFileOrc: String = TestFiles.testFileOrc
  protected val testFileXml: String = TestFiles.testFileXml
  protected val testBrokenCsv: String = "broken.csv"
}

trait LazySession extends SNTestBase {
  implicit override lazy val session: Session =
    Session.builder
      .configFile(defaultProfile)
      .config(SnowparkLazyAnalysis, "true")
      .create
}

trait EagerSession extends SNTestBase {
  implicit override lazy val session: Session =
    Session.builder
      .configFile(defaultProfile)
      .config(SnowparkLazyAnalysis, "false")
      .create
}

trait AlwaysCleanSession extends SNTestBase {
  implicit override lazy val session: Session =
    Session.builder
      .configFile(defaultProfile)
      .config(ParameterUtils.SnowparkEnableClosureCleaner, "always")
      .create
}

// No need to add trait 'repl_only', because in Intellij's test, 'repl_only' is
// equivalent with 'never'
trait NeverCleanSession extends SNTestBase {
  implicit override lazy val session: Session =
    Session.builder
      .configFile(defaultProfile)
      .config(ParameterUtils.SnowparkEnableClosureCleaner, "never")
      .create
}

trait UploadTimeoutSession extends SNTestBase {
  val mockSession: Session = spy(session)
  TestUtils.addDepsToClassPath(mockSession)
  when(mockSession.requestTimeoutInSeconds).thenReturn(0)
  assert(mockSession.requestTimeoutInSeconds == 0)
}
