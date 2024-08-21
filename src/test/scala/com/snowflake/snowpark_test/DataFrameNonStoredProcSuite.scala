package com.snowflake.snowpark_test

import com.snowflake.snowpark.{SaveMode, TestData, UpdateResult}
import com.snowflake.snowpark.functions.{col, lit}

class DataFrameNonStoredProcSuite extends TestData {

  private def testDataframeStatPivot(): Unit = {
    assert(
      getShowString(monthlySales.stat.crosstab("empid", "month").sort(col("empid")), 10) ==
        """---------------------------------------------------
          ||"EMPID"  |"'JAN'"  |"'FEB'"  |"'MAR'"  |"'APR'"  |
          |---------------------------------------------------
          ||1        |2        |2        |2        |2        |
          ||2        |2        |2        |2        |2        |
          |---------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(monthlySales.stat.crosstab("month", "empid").sort(col("month")), 10) ==
        """-------------------------------------------------------------------
          ||"MONTH"  |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |
          |-------------------------------------------------------------------
          ||APR      |2                          |2                          |
          ||FEB      |2                          |2                          |
          ||JAN      |2                          |2                          |
          ||MAR      |2                          |2                          |
          |-------------------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(date1.stat.crosstab("a", "b").sort(col("a")), 10) ==
        """----------------------------------------------------------------------
          ||"A"         |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |
          |----------------------------------------------------------------------
          ||2010-12-01  |0                          |1                          |
          ||2020-08-01  |1                          |0                          |
          |----------------------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(date1.stat.crosstab("b", "a").sort(col("b")), 10) ==
        """-----------------------------------------------------------
          ||"B"  |"TO_DATE('2020-08-01')"  |"TO_DATE('2010-12-01')"  |
          |-----------------------------------------------------------
          ||1    |1                        |0                        |
          ||2    |0                        |1                        |
          |-----------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(string7.stat.crosstab("a", "b").sort(col("a")), 10) ==
        """----------------------------------------------------------------
          ||"A"   |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |
          |----------------------------------------------------------------
          ||NULL  |0                          |1                          |
          ||str   |1                          |0                          |
          |----------------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(string7.stat.crosstab("b", "a").sort(col("b")), 10) ==
        """--------------------------
          ||"B"  |"'str'"  |"NULL"  |
          |--------------------------
          ||1    |1        |0       |
          ||2    |0        |0       |
          |--------------------------
          |""".stripMargin)
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
