package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.Utils
import net.snowflake.client.jdbc.SnowflakeSQLException

import scala.collection.mutable.ArrayBuffer

class SnowflakePlanSuite extends SNTestBase {

  val tableName: String = randomName()

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTable(tableName, "num int, str string")
    session.runQuery(s"insert into $tableName values(1, 'a'),(2, 'b'),(3, 'c')")

  }

  override def afterAll(): Unit = {
    dropTable(tableName)
    super.afterAll()
  }

  test("single query") {
    val df = session.table(tableName)
    assert(df.count == 3)
    // todo: re-enable SNOW-179093
    // assert(df.filter("num < 3").count() == 2)

    // build plan
    val tablePlan = session.plans.table(tableName)
    val project = session.plans.project(Seq("num"), tablePlan, None)
    // only one query
    assert(project.queries.tail.isEmpty)
  }

  test("multiple queries") {
    // unary query
    val tableName1 = randomName()
    val queries = Seq(
      s"create or replace temporary table $tableName1 as select * from " +
        "values(1::INT, 'a'::STRING),(2::INT, 'b'::STRING) as T(A,B)",
      s"select * from $tableName1").map(Query(_))
    val attrs =
      Seq(Attribute("A", IntegerType, nullable = true), Attribute("B", StringType, nullable = true))

    val plan =
      new SnowflakePlan(
        queries,
        schemaValueStatement(attrs),
        Seq.empty,
        session,
        None,
        supportAsyncMode = true)
    val plan1 = session.plans.project(Seq("A"), plan, None)

    assert(plan1.attributes.length == 1)
    assert(plan1.attributes.head.name == "\"A\"")
    // it is always nullable.
    assert(plan1.attributes.head.nullable)
    // SF always returns Long Type
    assert(plan1.attributes.head.dataType == LongType)

    val result = session.conn.execute(plan1)

    checkResult(result, Seq(Row(1), Row(2)))

    // binary query
    val tableName2 = randomName()
    val queries2 = Seq(
      s"create or replace temporary table $tableName2 as select * from " +
        "values(3::INT),(4::INT) as T(A)",
      s"select * from $tableName2").map(Query(_))
    val attrs2 = Seq(Attribute("C", LongType))

    val plan2 =
      new SnowflakePlan(
        queries2,
        schemaValueStatement(attrs2),
        Seq.empty,
        session,
        None,
        supportAsyncMode = true)
    val plan3 = session.plans.setOperator(plan1, plan2, "UNION ALL", None)

    assert(plan3.attributes.length == 1)
    assert(plan3.attributes.head.name == "\"A\"")
    assert(plan3.attributes.head.nullable)
    assert(plan3.attributes.head.dataType == LongType)

    val result2 = session.conn.execute(plan3)

    checkResult(result2, Seq(Row(1), Row(2), Row(3), Row(4)))
  }

  test("empty schema query") {
    assertThrows[SnowflakeSQLException](
      new SnowflakePlan(
        Seq.empty,
        "",
        Seq.empty,
        session,
        None,
        supportAsyncMode = true).attributes)
  }

  test("test SnowflakePlan.supportAsyncMode()") {
    // positive tests
    val df1 = session.sql("select current_version()")
    assert(df1.snowflakePlan.supportAsyncMode && df1.clone.snowflakePlan.supportAsyncMode)
    assert(session.sql("create table t1 (c1 int)").snowflakePlan.supportAsyncMode)
    assert(session.sql("insert t1 (c1 int)").snowflakePlan.supportAsyncMode)
    // Single "show tables" and "list stage" can be async executed
    assert(session.sql("show tables").snowflakePlan.supportAsyncMode)
    assert(session.sql("ls @~").snowflakePlan.supportAsyncMode)
    // Select, Join and sub query
    assert(session.range(5).select(lit("abc")).snowflakePlan.supportAsyncMode)
    assert(session.range(5).join(session.range(3)).snowflakePlan.supportAsyncMode)
    assert(session.range(5).select(toScalar(session.range(3))).snowflakePlan.supportAsyncMode)

    // negative tests
    val largeData = new ArrayBuffer[Row]()
    for (i <- 0 to 1024) {
      largeData.append(Row(i))
    }
    val df2 = session.createDataFrame(largeData, StructType(Seq(StructField("ID", LongType))))
    assert(!df2.snowflakePlan.supportAsyncMode && !df2.clone.snowflakePlan.supportAsyncMode)
    assert(!session.sql(" put file").snowflakePlan.supportAsyncMode)
    assert(!session.sql("get file ").snowflakePlan.supportAsyncMode)

    val dfShowTableSelect = session.sql("show tables").select("\"name\"")
    assert(!dfShowTableSelect.snowflakePlan.supportAsyncMode)
    val dfShowTableJoin = session.sql("show tables").join(session.range(3))
    assert(!dfShowTableJoin.snowflakePlan.supportAsyncMode)
    val dfResultScanSubQuery = session.range(3).select(toScalar(dfShowTableSelect))
    assert(!dfResultScanSubQuery.snowflakePlan.supportAsyncMode)
  }

  test("test single-query-plan for query tags") {
    val df = session.range(5)
    var queryTag = Utils.getUserCodeMeta()

    // Test query tag with statement level query_tag and sync
    var beginCount = getQueryHistoryForTags(queryTag, session).length
    df.collect()
    var afterCount = getQueryHistoryForTags(queryTag, session).length
    assert(afterCount - beginCount == 1)

    // Test query tag with statement level query_tag and async
    beginCount = getQueryHistoryForTags(queryTag, session).length
    df.async.collect().getResult()
    afterCount = getQueryHistoryForTags(queryTag, session).length
    // plan async execution needs 1 extra RESULT_SCAN
    assert(afterCount - beginCount == 2)

    try {
      // Test query tag with session level query_tag and sync
      queryTag = randomName()
      session.setQueryTag(queryTag)
      df.collect()
      session.unsetQueryTag()
      assert(getQueryHistoryForTags(queryTag, session).length == 1)

      // Test query tag with session level query_tag and async
      queryTag = randomName()
      session.setQueryTag(queryTag)
      df.async.collect().getResult()
      session.unsetQueryTag()
      assert(getQueryHistoryForTags(queryTag, session).length == 2)
    } finally {
      session.unsetQueryTag()
    }
  }

  test("test multiple-queries-plan for query tags") {
    val queries = Seq(s"select 1", s"select 2", s"select 3", "select 4").map(Query(_))
    val attrs = Seq(Attribute("A", IntegerType, nullable = true))
    val plan =
      new SnowflakePlan(
        queries,
        schemaValueStatement(attrs),
        Seq.empty,
        session,
        None,
        supportAsyncMode = true)
    val df = DataFrame(session, plan)
    var queryTag = Utils.getUserCodeMeta()

    // Test query tag with statement level query_tag and sync
    var beginCount = getQueryHistoryForTags(queryTag, session).length
    df.collect()
    var afterCount = getQueryHistoryForTags(queryTag, session).length
    assert(afterCount - beginCount == 4)

    // Test query tag with statement level query_tag and async
    beginCount = getQueryHistoryForTags(queryTag, session).length
    df.async.collect().getResult()
    afterCount = getQueryHistoryForTags(queryTag, session).length
    // plan async execution for multiple queries plan has 2 queries
    // be tagged, one is the multiple-statement query, the other is the result scan.
    // If GS changes to apply the statement level query_tag for all statements
    // below asserion can be changed to "assert(afterCount - beginCount == 6)"
    assert(afterCount - beginCount == 2)

    try {
      // Test query tag with session level query_tag and sync
      queryTag = randomName()
      session.setQueryTag(queryTag)
      df.collect()
      session.unsetQueryTag()
      assert(getQueryHistoryForTags(queryTag, session).length == 4)

      // Test query tag with session level query_tag and async
      queryTag = randomName()
      session.setQueryTag(queryTag)
      df.async.collect().getResult()
      session.unsetQueryTag()
      assert(getQueryHistoryForTags(queryTag, session).length == 6)
    } finally {
      session.unsetQueryTag()
    }
  }

}
