package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.types._
import net.snowflake.client.core.QueryStatus
import net.snowflake.client.jdbc.{SnowflakeResultSet, SnowflakeSQLException, SnowflakeStatement}

import scala.collection.mutable.ArrayBuffer

class ServerConnectionSuite extends SNTestBase {

  test("Confirm: JDBC Statement.getResultSet() can get the expected ResultSet") {
    val statement = session.conn.connection.createStatement()
    val rs = statement.executeQuery("select 123")
    assert(rs.eq(statement.getResultSet))
    rs.next()
    assert(rs.getInt(1) == 123)
  }

  test("Confirm: JDBC Statement.close() can close the ResultSet") {
    val statement = session.conn.connection.createStatement()
    val rs = statement.executeQuery("select 123")
    statement.close()
    val ex = intercept[SnowflakeSQLException] {
      rs.next()
      rs.getInt(1)
    }
    assert(ex.getMessage.contains("Result set has been closed."))
  }

  test("test ServerConnection.waitForQueryDone(queryID)") {
    val statement = session.conn.connection.createStatement()
    try {
      // positive test
      var queryID = statement
        .asInstanceOf[SnowflakeStatement]
        .executeAsyncQuery("select SYSTEM$WAIT(2)")
        .asInstanceOf[SnowflakeResultSet]
        .getQueryID
      assert(session.conn.waitForQueryDone(queryID, 100).equals(QueryStatus.SUCCESS))

      // Negative tests
      queryID = statement
        .asInstanceOf[SnowflakeStatement]
        .executeAsyncQuery("select SYSTEM$WAIT(100)")
        .asInstanceOf[SnowflakeResultSet]
        .getQueryID
      val ex1 = intercept[SnowparkClientException] {
        session.conn.waitForQueryDone(queryID, 20)
      }
      assert(
        ex1.errorCode.equals("0318") &&
          ex1.message.contains("The function call has been running for 19 seconds"))
    } finally {
      session.cancelAll()
      statement.close()
    }
  }

  test("test ServerConnection.executeAsync(plan) negative") {
    val df1 = session.sql("show tables").select("\"name\"")
    val ex1 = intercept[SnowparkClientException] {
      session.conn.executeAsync(df1.snowflakePlan)
    }
    assert(ex1.errorCode.equals("0317") && ex1.getMessage.contains("show tables"))

    val largeData = new ArrayBuffer[Row]()
    for (i <- 0 to 1024) {
      largeData.append(Row(i))
    }
    val df2 = session.createDataFrame(largeData, StructType(Seq(StructField("ID", LongType))))
    val ex2 = intercept[SnowparkClientException] {
      session.conn.executeAsync(df2.snowflakePlan)
    }
    assert(ex2.errorCode.equals("0317") && ex2.getMessage.contains("INSERT  INTO"))
  }

  test("test ServerConnection.executeAsync(plan) and getAsyncResult() positive") {
    // one query
    var plan = session.range(100).snowflakePlan
    var asyncJob = session.conn.executeAsync(plan)
    var iterator = session.conn.getAsyncResult(asyncJob.getQueryId(), Int.MaxValue, Some(plan))._1
    var rows = iterator.toSeq
    assert(rows.size == 100 && rows.head.getLong(0) == 0 && rows.last.getLong(0) == 99)

    // multiple queries
    val tableName = randomName()
    val queries = Seq(
      s"create or replace temporary table $tableName (c1 int, c2 string)",
      s"insert into $tableName values (1, 'abc'), (123, 'dfdffdfdf')",
      "select SYSTEM$WAIT(2)",
      s"select max(c1) from $tableName").map(Query(_))
    val attrs = Seq(Attribute("C1", LongType))
    plan = new SnowflakePlan(
      queries,
      schemaValueStatement(attrs),
      Seq(Query(s"drop table if exists $tableName", true)),
      session,
      None,
      supportAsyncMode = true)
    asyncJob = session.conn.executeAsync(plan)
    iterator = session.conn.getAsyncResult(asyncJob.getQueryId(), Int.MaxValue, Some(plan))._1
    rows = iterator.toSeq
    assert(rows.size == 1 && rows.head.getLong(0) == 123)
  }

  test("test ServerConnection.executeAsync(plan) and getAsyncResult() negative") {
    // one query
    var plan = session.sql(s"select to_number('not_a_number') as C1").snowflakePlan
    var asyncJob = session.conn.executeAsync(plan)
    val ex1 = intercept[SnowflakeSQLException] {
      session.conn.getAsyncResult(asyncJob.getQueryId(), Int.MaxValue, Some(plan))._1
    }
    assert(ex1.getMessage.contains("Numeric value 'not_a_number' is not recognized"))

    // multiple queries
    val tableName = randomName()
    val attrs = Seq(Attribute("C1", LongType))
    val queries2 = Seq(
      s"create or replace temporary table $tableName (c1 int, c2 string)",
      s"insert into $tableName values (1, 'abc'), (123, 'dfdffdfdf')",
      "select SYSTEM$WAIT(2)",
      s"select to_number('not_a_number') as C1").map(Query(_))
    plan = new SnowflakePlan(
      queries2,
      schemaValueStatement(attrs),
      Seq(Query(s"drop table if exists $tableName", true)),
      session,
      None,
      supportAsyncMode = true)
    asyncJob = session.conn.executeAsync(plan)
    val ex2 = intercept[SnowflakeSQLException] {
      session.conn.getAsyncResult(asyncJob.getQueryId(), Int.MaxValue, Some(plan))._1
    }
    assert(ex2.getMessage.contains("Numeric value 'not_a_number' is not recognized"))
    assert(ex2.getMessage.contains("Uncaught Execution of multiple statements failed on statement"))
  }

  test("ServerConnection.getStatementParameters()") {
    val parameters1 = session.conn.getStatementParameters()
    assert(parameters1.size == 1 && parameters1.contains("QUERY_TAG"))

    val parameters2 = session.conn.getStatementParameters(true)
    assert(
      parameters2.size == 2 && parameters2.contains("QUERY_TAG") && parameters2.contains(
        "SNOWPARK_SKIP_TXN_COMMIT_IN_DDL"))

    try {
      session.setQueryTag("test_tag_123")
      assert(session.conn.getStatementParameters().isEmpty)
      val parameters3 = session.conn.getStatementParameters(true)
      assert(parameters3.size == 1 && parameters3.contains("SNOWPARK_SKIP_TXN_COMMIT_IN_DDL"))
    } finally {
      session.unsetQueryTag()
    }
  }

}
