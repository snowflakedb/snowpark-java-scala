package com.snowflake.snowpark_test

import com.snowflake.snowpark.{Row, SaveMode, TestData, UpdateResult}
import com.snowflake.snowpark.functions.{col, lit}

import java.sql.Date

class DataFrameNonStoredProcSuite extends TestData {

  private def testDataframeStatPivot(): Unit = {
    testWithTimezone() {
      checkAnswer(
        monthlySales.stat.crosstab("empid", "month").sort(col("empid")),
        Seq(Row(1, 2, 2, 2, 2), Row(2, 2, 2, 2, 2)))
      checkAnswer(
        monthlySales.stat.crosstab("month", "empid").sort(col("month")),
        Seq(Row("APR", 2, 2), Row("FEB", 2, 2), Row("JAN", 2, 2), Row("MAR", 2, 2)))
      checkAnswer(
        date1.sort(col("b")).stat.crosstab("a", "b").sort(col("a")),
        Seq(Row(Date.valueOf("2010-12-01"), 0, 1), Row(Date.valueOf("2020-08-01"), 1, 0)))
      checkAnswer(
        date1
          .sort(col("a"))
          .stat
          .crosstab("b", "a")
          .sort(col("b")),
        Seq(Row(1, 0, 1), Row(2, 1, 0)))
      checkAnswer(
        string7.stat.crosstab("a", "b").sort(col("a")),
        Seq(Row(null, 0, 1), Row("str", 1, 0)))
      // Pivot column order is non-deterministic (depends on GROUP BY result order).
      // Check pivot values as a sorted set to make the test order-independent.
      val crosstabResult = string7.sort(col("a")).stat.crosstab("b", "a").sort(col("b")).collect()
      assert(crosstabResult.length == 2)
      // For b=1: one pivot column should be 1 (for 'str'), one should be 0 (for null)
      assert(crosstabResult(0).getInt(0) == 1)
      assert(crosstabResult(0).toSeq.drop(1).map(_.asInstanceOf[Long]).sorted == Seq(0L, 1L))
      // For b=2: both pivot columns should be 0 (COUNT(null)=0, no 'str' rows)
      assert(crosstabResult(1).getInt(0) == 2)
      assert(crosstabResult(1).toSeq.drop(1).map(_.asInstanceOf[Long]).sorted == Seq(0L, 0L))
    }
  }

  test("df.stat.pivot") {
    testWithAlteredSessionParameter(
      testDataframeStatPivot(),
      "ENABLE_PIVOT_VIEW_WITH_OBJECT_AGG",
      "disable",
      skipIfParamNotExist = true)

    testWithAlteredSessionParameter(
      testDataframeStatPivot(),
      "ENABLE_PIVOT_VIEW_WITH_OBJECT_AGG",
      "enable",
      skipIfParamNotExist = true)
  }

  test("ERROR_ON_NONDETERMINISTIC_UPDATE = true") {
    val tableName: String = randomName()
    createTable(tableName, "num int")
    try {
      runQuery(s"insert into $tableName values(1),(2),(3)", session)
      withSessionParameters(Seq(("ERROR_ON_NONDETERMINISTIC_UPDATE", "true")), session) {
        testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
        val updatable = session.table(tableName)
        testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
        assert(updatable.update(Map(col("a") -> lit(1), col("b") -> lit(0))) == UpdateResult(6, 0))
      }
    } finally {
      dropTable(tableName)
    }

  }
}
