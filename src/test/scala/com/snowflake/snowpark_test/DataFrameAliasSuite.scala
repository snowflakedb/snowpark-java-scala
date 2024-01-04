package com.snowflake.snowpark_test

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.types._
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.scalatest.BeforeAndAfterEach
import java.sql.{Date, Time, Timestamp}
import scala.util.Random

class DataFrameAliasSuite extends TestData with BeforeAndAfterEach with EagerSession {
  val tableName1: String = randomName()
  val tableName2: String = randomName()
  import session.implicits._

  override def afterEach(): Unit = {
    dropTable(tableName1)
    dropTable(tableName2)
    super.afterEach()
  }

  test("Test for alias with df.col, col and $") {
    createTable(tableName1, "num int")
    runQuery(s"insert into $tableName1 values(1),(2),(3)", session)
    val df = session.table(tableName1).alias("A")
    checkAnswer(df.select(df.col("A.num")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df.select(col("A.num")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df.select($"A.num"), Seq(Row(1), Row(2), Row(3)))

    val df1 = df.alias("B")
    checkAnswer(df1.select(df1.col("A.num")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df1.select(col("A.num")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df1.select($"A.num"), Seq(Row(1), Row(2), Row(3)))

    checkAnswer(df1.select(df1.col("B.num")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df1.select(col("B.num")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df1.select($"B.num"), Seq(Row(1), Row(2), Row(3)))
  }

  test("Test for alias with dot in column name") {
    createTable(tableName1, "\"num.col\" int")
    runQuery(s"insert into $tableName1 values(1),(2),(3)", session)
    val df = session.table(tableName1).alias("A")
    checkAnswer(df.select(df.col("A.num.col")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df.select(col("A.num.col")), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df.select($"A.num.col"), Seq(Row(1), Row(2), Row(3)))
  }

  test("Test for alias with join") {
    createTable(tableName1, "id1 int, num1 int")
    createTable(tableName2, "id2 int, num2 int")
    runQuery(s"insert into $tableName1 values(1, 4),(2, 5),(3, 6)", session)
    runQuery(s"insert into $tableName2 values(1, 7),(2, 8),(3, 9)", session)
    val df1 = session.table(tableName1).alias("A")
    val df2 = session.table(tableName2).alias("B")
    checkAnswer(df1.join(df2, $"id1" === $"id2")
      .select(df1.col("A.num1")), Seq(Row(4), Row(5), Row(6)))
    checkAnswer(df1.join(df2, $"id1" === $"id2")
      .select(df2.col("B.num2")), Seq(Row(7), Row(8), Row(9)))

    checkAnswer(df1.join(df2, $"id1" === $"id2")
      .select($"A.num1"), Seq(Row(4), Row(5), Row(6)))
    checkAnswer(df1.join(df2, $"id1" === $"id2")
      .select($"B.num2"), Seq(Row(7), Row(8), Row(9)))
  }

  test("Test for alias with join with column renaming") {
    createTable(tableName1, "id int, num int")
    createTable(tableName2, "id int, num int")
    runQuery(s"insert into $tableName1 values(1, 4),(2, 5),(3, 6)", session)
    runQuery(s"insert into $tableName2 values(1, 7),(2, 8),(3, 9)", session)
    val df1 = session.table(tableName1).alias("A")
    val df2 = session.table(tableName2).alias("B")
    checkAnswer(df1.join(df2, df1.col("id") === df2.col("id"))
      .select(df1.col("A.num")), Seq(Row(4), Row(5), Row(6)))
    checkAnswer(df1.join(df2, df1.col("id") === df2.col("id"))
      .select(df2.col("B.num")), Seq(Row(7), Row(8), Row(9)))

    // The following use case is out of the scope of supporting alias
    // We still follow the old ambiguity resolving policy and require DF to be used
    assertThrows[SnowparkClientException](
      df1.join(df2, df1.col("id") === df2.col("id"))
      .select($"A.num"))
  }

  test("Test for alias conflict") {
    createTable(tableName1, "id int, num int")
    createTable(tableName2, "id int, num int")
    val df1 = session.table(tableName1).alias("A")
    val df2 = session.table(tableName2).alias("A")
    assertThrows[SnowparkClientException](
      df1.join(df2, df1.col("id") === df2.col("id"))
      .select(df1.col("A.num")))
  }
}
