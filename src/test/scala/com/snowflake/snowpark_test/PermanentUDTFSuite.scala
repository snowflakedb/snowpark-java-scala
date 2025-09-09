package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._

import com.snowflake.snowpark._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.udtf._
import net.snowflake.client.jdbc.SnowflakeSQLException

import scala.collection.mutable

@UDFTest
class PermanentUDTFSuite extends TestData {
  import session.implicits._

  // session to verify permanent udf
  lazy private val newSession = Session.builder.configFile(defaultProfile).create
  lazy private val stageName: String = randomStageName()
  val wordCountTableName = randomTableName()

  override def beforeAll: Unit = {
    super.beforeAll
    createStage(stageName, isTemporary = false)
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session, Some(stageName))
      TestUtils.addDepsToClassPath(newSession, Some(stageName))
    }
    enableScala213UdxfSprocParams(session)
    enableScala213UdxfSprocParams(newSession)
  }

  override def afterAll: Unit = {
    dropStage(stageName)
    disableScala213UdxfSprocParams(session)
    disableScala213UdxfSprocParams(newSession)
    super.afterAll
  }

  test("mix temporary and permanent UDTF", JavaStoredProcExclude) {
    val tempFuncName = randomFunctionName()
    val permFuncName = randomFunctionName()
    val stageName1 = randomStageName()
    try {
      runQuery(s"create stage $stageName1", session)
      // register a perm and a temp UDTF
      val tempTableFunction = session.udtf.registerTemporary(tempFuncName, new PermWordCount)
      val permTableFunction =
        session.udtf.registerPermanent(permFuncName, new PermWordCount, stageName1)
      val tempDF = session.tableFunction(tempTableFunction, lit("a b b"))
      val permDF = session.tableFunction(permTableFunction, lit("a b b"))
      // Use both in current session.
      checkAnswer(tempDF, Seq(Row("a", 1), Row("b", 2)))
      checkAnswer(permDF, Seq(Row("a", 1), Row("b", 2)))

      // another session
      val newTempDF = newSession.tableFunction(tempTableFunction, lit("a b b"))
      val newPermDF = newSession.tableFunction(permTableFunction, lit("a b b"))
      checkAnswer(newPermDF, Seq(Row("a", 1), Row("b", 2)))
      // use temp in another session will fail.
      assertThrows[SnowflakeSQLException](newTempDF.collect())
    } finally {
      runQuery(s"drop function if exists $tempFuncName(VARCHAR)", session)
      runQuery(s"drop function if exists $permFuncName(VARCHAR)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }

  test("mix temporary and permanent UDTF + prefix", JavaStoredProcExclude) {
    val tempFuncName = randomFunctionName()
    val permFuncName = randomFunctionName()
    val stageName1 = randomStageName()
    try {
      runQuery(s"create stage $stageName1", session)
      // register a perm and a temp UDTF
      val tempTableFunction = session.udtf.registerTemporary(tempFuncName, new PermWordCount)
      val permTableFunction =
        session.udtf.registerPermanent(permFuncName, new PermWordCount, s"$stageName1/prefix")
      val tempDF = session.tableFunction(tempTableFunction, lit("a b b"))
      val permDF = session.tableFunction(permTableFunction, lit("a b b"))
      // Use both in current session.
      checkAnswer(tempDF, Seq(Row("a", 1), Row("b", 2)))
      checkAnswer(permDF, Seq(Row("a", 1), Row("b", 2)))

      // another session
      val newTempDF = newSession.tableFunction(tempTableFunction, lit("a b b"))
      val newPermDF = newSession.tableFunction(permTableFunction, lit("a b b"))
      checkAnswer(newPermDF, Seq(Row("a", 1), Row("b", 2)))
      // use temp in another session will fail.
      assertThrows[SnowflakeSQLException](newTempDF.collect())
    } finally {
      runQuery(s"drop function if exists $tempFuncName(VARCHAR)", session)
      runQuery(s"drop function if exists $permFuncName(VARCHAR)", session)
      runQuery(s"drop stage if exists $stageName1", session)
    }
  }
}

class PermWordCount extends UDTF1[String] {
  override def process(s1: String): Iterable[Row] = {
    val wordCounts = mutable.Map[String, Int]()
    s1.split(" ").foreach { x =>
      wordCounts.update(x, wordCounts.getOrElse(x, 0) + 1)
    }
    wordCounts.toArray.map { p =>
      Row(p._1, p._2)
    }
  }
  override def endPartition(): Iterable[Row] = Seq.empty
  override def outputSchema(): StructType =
    StructType(StructField("word", StringType), StructField("count", IntegerType))
}
