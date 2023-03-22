package com.snowflake.snowpark_test

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.types._
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.scalatest.BeforeAndAfterEach
import java.sql.{Date, Time, Timestamp}
import scala.util.Random

trait DataFrameSuite extends TestData with BeforeAndAfterEach {
  val tableName = randomName()
  val viewName = randomName()
  val samplingDeviation = 0.4
  import session.implicits._
  override def afterEach(): Unit = {
    dropTable(tableName)
    dropView(viewName)
    super.afterEach()
  }

  test("Test for null data in tables") {
    createTable(tableName, "num int")
    runQuery(s"insert into $tableName values(null),(null),(null)", session)
    val df = session.table(tableName)
    checkAnswer(df, Seq(Row(null), Row(null), Row(null)))
  }

  test("Test for null data in LocalRelation with filters") {
    val df = Seq((1, null), (2, "NotNull"), (3, null)).toDF("a", "b")
    checkAnswer(df, Seq(Row(1, null), Row(2, "NotNull"), Row(3, null)))
    val df2 = session.createDataFrame(Seq((1, null), (2, "NotNull"), (3, null)))
    checkAnswer(df, df2, true)

    checkAnswer(df.filter(is_null($"b")), Seq(Row(1, null), Row(3, null)))
    checkAnswer(df.filter(!is_null($"b")), Seq(Row(2, "NotNull")))
    checkAnswer(
      df.sort(col("b").asc_nulls_last),
      Seq(Row(2, "NotNull"), Row(1, null), Row(3, null)),
      false)
  }

  test("Project null values") {
    val df = Seq(1, 2).toDF("a").withColumn("b", lit(null))
    checkAnswer(df, Seq(Row(1, null), Row(2, null)))

    val df2 = Seq(1, 2).toDF("a").select(lit(null))
    assert(df2.schema.size === 1)
    assert(df2.schema.head.dataType === StringType)
    checkAnswer(df2, Seq(Row(null), Row(null)))
  }

  test("Write null data to table") {
    val df = Seq((1, null), (2, "NotNull"), (3, null)).toDF("a", "b")
    // write to table
    df.write.saveAsTable(tableName)
    checkAnswer(session.table(tableName), df, true)
  }

  test("createOrReplaceView with null data") {
    val df = Seq((1, null), (2, "NotNull"), (3, null)).toDF("a", "b")
    df.createOrReplaceView(viewName)

    val df2 = session.sql(s"select * from $viewName")
    checkAnswer(df2, Seq(Row(1, null), Row(2, "NotNull"), Row(3, null)))
  }

  test("adjust column width of show") {
    // run show function, make sure no error reported
    val df = Seq((1, null), (2, "NotNull")).toDF("a", "b")
    df.show(10, 4)

    assert(
      getShowString(df, 10, 4) ==
        """--------------
          ||"A"  |"B"   |
          |--------------
          ||1    |NULL  |
          ||2    |N...  |
          |--------------
          |""".stripMargin)
  }

  test("show with null data") {
    // run show function, make sure no error reported
    val df = Seq((1, null), (2, "NotNull")).toDF("a", "b")
    df.show()

    assert(
      getShowString(df, 10) ==
        """-----------------
          ||"A"  |"B"      |
          |-----------------
          ||1    |NULL     |
          ||2    |NotNull  |
          |-----------------
          |""".stripMargin)
  }

  test("show multi-lines row") {
    val df = Seq(("line1\nline2", null), ("single line", "NotNull\none more line\nlast line"))
      .toDF("a", "b")
    assert(
      getShowString(df, 2) ==
        """-------------------------------
      ||"A"          |"B"            |
      |-------------------------------
      ||line1        |NULL           |
      ||line2        |               |
      ||single line  |NotNull        |
      ||             |one more line  |
      ||             |last line      |
      |-------------------------------
      |""".stripMargin)
  }

  test("show") {
    // run show function, make sure no error reported
    testData1.show()

    assert(
      getShowString(testData1, 10) ==
        """--------------------------
          ||"NUM"  |"BOOL"  |"STR"  |
          |--------------------------
          ||1      |true    |a      |
          ||2      |false   |b      |
          |--------------------------
          |""".stripMargin)

    session.sql("show tables").show()

    session.sql("drop table if exists test_table_123").show()

    assert( // truncate result, no more than 50 characters
      getShowString(session.sql("drop table if exists test_table_123"), 1) ==
        s"""------------------------------------------------------
           ||"status"                                            |
           |------------------------------------------------------
           ||Drop statement executed successfully (TEST_TABL...  |
           |------------------------------------------------------
           |""".stripMargin)
  }

  test("cacheResult") {
    testCacheResult()
  }

  test("cacheResult with scoped temp object", JavaStoredProcExcludeOwner) {
    // snowpark_use_scoped_temp_objects might not exist on server,
    // so skip this test if it does not exist
    testWithAlteredSessionParameter(
      testCacheResult(),
      "snowpark_use_scoped_temp_objects",
      "true",
      skipIfParamNotExist = true)
  }

  private def testCacheResult(): Unit = {
    val tableName = randomName()
    runQuery(s"create table $tableName (num int)", session)
    runQuery(s"insert into $tableName values(1),(2)", session)

    val df = session.table(tableName)
    checkAnswer(df, Seq(Row(1), Row(2)))

    // update df
    runQuery(s"insert into $tableName values(3)", session)
    checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

    val df1 = df.cacheResult()
    runQuery(s"insert into $tableName values(4)", session)
    checkAnswer(df1, Seq(Row(1), Row(2), Row(3))) // no change on cached df
    checkAnswer(df, Seq(Row(1), Row(2), Row(3), Row(4)))

    val df2 = df1.where(col("num") > 2)
    checkAnswer(df2, Seq(Row(3))) // no change on df based on cached df

    val df3 = df.where(col("num") > 2)
    checkAnswer(df3, Seq(Row(3), Row(4)))

    val df4 = df1.cacheResult()
    checkAnswer(df4, Seq(Row(1), Row(2), Row(3)))

    // drop original table, the make sure original query will not be invoked
    runQuery(s"drop table $tableName", session)
    checkAnswer(df1, Seq(Row(1), Row(2), Row(3)))
    checkAnswer(df2, Seq(Row(3)))
  }

  test("cacheResult with SHOW TABLES") {
    val tableName1 = randomTableName()
    try {
      runQuery(s"create temp table $tableName1 (name string)", session)
      runQuery(s"insert into $tableName1 values('$tableName1')", session)
      val table = session.table(tableName1)

      // SHOW TABLES
      val df1 = session.sql("show tables").cacheResult()
      var tableNames = df1.collect().map(_.getString(1))
      assert(tableNames.contains(tableName1))

      // SHOW TABLES + SELECT
      val df2 = session.sql("show tables").select("\"created_on\"", "\"name\"").cacheResult()
      tableNames = df2.collect().map(_.getString(1))
      assert(tableNames.contains(tableName1))

      // SHOW TABLES + SELECT + Join
      val df3 = session
        .sql("show tables")
        .select("\"created_on\"", "\"name\"")
      df3.show()
      val df4 = df3.join(table, df3("\"name\"") === table("name")).cacheResult()
      tableNames = df4.select("name").collect().map(_.getString(0))
      assert(tableNames.contains(tableName1))
    } finally {
      runQuery(s"drop table if exists $tableName1", session)
    }
  }

  test("non-select query composition") {
    val tableName = randomName()
    try {
      runQuery(s"create or replace table $tableName (num int)", session)
      val df =
        session.sql("show tables").select(""""name"""").filter(col(""""name"""") === tableName)
      assert(df.collect().length == 1)
      val schema = df.schema
      assert(schema.length == 1)
      assert(schema.head.dataType == StringType)
      assert(schema.head.name == """"name"""")
    } finally {
      runQuery(s"drop table if exists $tableName", session)
    }
  }

  test("non-select query composition, union") {
    val tableName = randomName()
    try {
      runQuery(s"create or replace table $tableName (num int)", session)
      val df1 = session.sql("show tables")
      val df2 = session.sql("show tables")
      assert(
        df1
          .union(df2)
          .select(""""name"""")
          .filter(col(""""name"""") === tableName)
          .collect()
          .length == 1)
    } finally {
      runQuery(s"drop table if exists $tableName", session)
    }
  }

  test("non-select query composition, unionAll") {
    val tableName = randomName()
    try {
      runQuery(s"create or replace table $tableName (num int)", session)
      val df1 = session.sql("show tables")
      val df2 = session.sql("show tables")
      assert(
        df1
          .unionAll(df2)
          .select(""""name"""")
          .filter(col(""""name"""") === tableName)
          .collect()
          .length == 2)
    } finally {
      runQuery(s"drop table if exists $tableName", session)
    }
  }

  test("joins on result scan") {
    val df1 = session.sql("show tables").select(""""name"""", """"kind"""")
    val df2 = session.sql("show stages").select(""""name"""", """"cloud"""")

    val result = df1.join(df2, """"name"""")
    // no error
    result.collect()
    assert(result.schema.length == 3)
  }

  test("drop") {
    checkAnswer(double3.na.drop(1, Seq("a")), Seq(Row(1.0, 1), Row(4.0, null)))
    checkAnswer(
      double3.na.drop(1, Seq("a", "b")),
      Seq(Row(1.0, 1), Row(4.0, null), Row(Double.NaN, 2), Row(null, 3)))
    assert(double3.na.drop(0, Seq("a")).count() == 6)
    assert(double3.na.drop(3, Seq("a", "b")).count() == 0)
    assert(double3.na.drop(1, Seq()).count() == 6)
    assertThrows[SnowparkClientException](double3.na.drop(1, Seq("c")))
  }

  test("fill") {
    checkAnswer(
      nullData3.na.fill(Map("flo" -> 12.3, "int" -> 11, "boo" -> false, "str" -> "f")),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(12.3, 2, false, "b"),
        Row(12.3, 3, false, "f"),
        Row(4.0, 11, false, "d"),
        Row(12.3, 11, false, "f"),
        Row(12.3, 11, false, "f")),
      sort = false)
    checkAnswer(
      nullData3.na.fill(Map("flo" -> 22.3f, "int" -> 22L, "boo" -> false, "str" -> "f")),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(22.3, 2, false, "b"),
        Row(22.3, 3, false, "f"),
        Row(4.0, 22, false, "d"),
        Row(22.3, 22, false, "f"),
        Row(22.3, 22, false, "f")),
      sort = false)
    checkAnswer(
      nullData3.na.fill(
        Map("flo" -> 12.3, "int" -> 33.asInstanceOf[Short], "boo" -> false, "str" -> "f")),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(12.3, 2, false, "b"),
        Row(12.3, 3, false, "f"),
        Row(4.0, 33, false, "d"),
        Row(12.3, 33, false, "f"),
        Row(12.3, 33, false, "f")),
      sort = false)
    checkAnswer(
      nullData3.na.fill(
        Map("flo" -> 12.3, "int" -> 44.asInstanceOf[Byte], "boo" -> false, "str" -> "f")),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(12.3, 2, false, "b"),
        Row(12.3, 3, false, "f"),
        Row(4.0, 44, false, "d"),
        Row(12.3, 44, false, "f"),
        Row(12.3, 44, false, "f")),
      sort = false)
    // wrong type
    checkAnswer(
      nullData3.na.fill(Map("flo" -> 12.3, "int" -> "11", "boo" -> false, "str" -> 1)),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(12.3, 2, false, "b"),
        Row(12.3, 3, false, null),
        Row(4.0, null, false, "d"),
        Row(12.3, null, false, null),
        Row(12.3, null, false, null)),
      sort = false)

    // wrong column name
    assertThrows[SnowparkClientException](nullData3.na.fill(Map("wrong" -> 11)))
  }

  test("replace") {
    checkAnswer(
      nullData3.na.replace("flo", Map(2 -> 300, 1 -> 200)),
      Seq(
        Row(200.0, 1, true, "a"),
        Row(Double.NaN, 2, null, "b"),
        Row(null, 3, false, null),
        Row(4.0, null, null, "d"),
        Row(null, null, null, null),
        Row(Double.NaN, null, null, null)),
      sort = false)
    // replace null
    checkAnswer(
      nullData3.na.replace("boo", Map(None -> true)),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(Double.NaN, 2, true, "b"),
        Row(null, 3, false, null),
        Row(4.0, null, true, "d"),
        Row(null, null, true, null),
        Row(Double.NaN, null, true, null)),
      sort = false)

    // replace NaN
    checkAnswer(
      nullData3.na.replace("flo", Map(Double.NaN -> 11)),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(11, 2, null, "b"),
        Row(null, 3, false, null),
        Row(4.0, null, null, "d"),
        Row(null, null, null, null),
        Row(11, null, null, null)),
      sort = false)

    // incompatible type
    assertThrows[SnowflakeSQLException](nullData3.na.replace("flo", Map(None -> "aa")).collect())

    // null value
    checkAnswer(
      nullData3.na.replace("flo", Map(Double.NaN -> null)),
      Seq(
        Row(1.0, 1, true, "a"),
        Row(null, 2, null, "b"),
        Row(null, 3, false, null),
        Row(4.0, null, null, "d"),
        Row(null, null, null, null),
        Row(null, null, null, null)),
      sort = false)

    assert(
      getSchemaString(nullData3.na.replace("flo", Map(Double.NaN -> null)).schema) ==
        """root
      | |--FLO: Double (nullable = true)
      | |--INT: Long (nullable = true)
      | |--BOO: Boolean (nullable = true)
      | |--STR: String (nullable = true)
      |""".stripMargin)

  }

  test("df.stat.corr") {
    assertThrows[SnowflakeSQLException](string1.stat.corr("a", "b"))

    // Too few data points to calculate correlation
    assert(nullData2.stat.corr("a", "b").isEmpty)

    // Too few data points to calculate correlation
    assert(double4.stat.corr("a", "b").isEmpty)

    // There is NaN value in the columns
    assert(double3.stat.corr("a", "b").get.isNaN)

    assert(double2.stat.corr("a", "b").get == 0.9999999999999991)
  }

  test("df.stat.cov") {
    assertThrows[SnowflakeSQLException](string1.stat.cov("a", "b"))

    assert(nullData2.stat.cov("a", "b").get == 0)

    // Too few data points to calculate correlation
    assert(double4.stat.cov("a", "b").isEmpty)

    // There is NaN value in the columns
    assert(double3.stat.cov("a", "b").get.isNaN)

    assert(double2.stat.cov("a", "b").get == 0.010000000000000037)
  }

  test("df.stat.approxQuantile") {
    assert(approxNumbers.stat.approxQuantile("a", Array(0.5))(0).get == 4.5)
    assert(
      approxNumbers.stat.approxQuantile("a", Array(0, 0.1, 0.4, 0.6, 1)).deep ==
        Array(Some(0.0), Some(0.9), Some(3.6), Some(5.3999999999999995), Some(9.0)).deep)

    // Probability out of range error and apply on string column error.
    assertThrows[SnowflakeSQLException](approxNumbers.stat.approxQuantile("a", Array(-1d)))
    assertThrows[SnowflakeSQLException](string1.stat.approxQuantile("a", Array(0.5)))

    createTable(tableName, "num int")
    assert(session.table(tableName).stat.approxQuantile("num", Array(0.5))(0).isEmpty)

    val res = double2.stat.approxQuantile(Array("a", "b"), Array(0, 0.1, 0.6))
    assert(res(0).deep == Array(Some(0.1), Some(0.12000000000000001), Some(0.22)).deep)
    assert(res(1).deep == Array(Some(0.5), Some(0.52), Some(0.62)).deep)

    // ApproxNumbers2 contains a column called T, which conflicts with tmpColumnName.
    // This test demos that the query still works.
    assert(approxNumbers2.stat.approxQuantile("a", Array(0.5))(0).get == 4.5)
    assert(approxNumbers2.stat.approxQuantile("t", Array(0.5))(0).get == 3.0)

    assert(double2.stat.approxQuantile("a", Array[Double]()).isEmpty)
    assert(double2.stat.approxQuantile(Array[String](), Array[Double]()).isEmpty)
  }

  test("df.stat.pivot") {
    assert(
      getShowString(monthlySales.stat.crosstab("empid", "month"), 10) ==
        """---------------------------------------------------
          ||"EMPID"  |"'JAN'"  |"'FEB'"  |"'MAR'"  |"'APR'"  |
          |---------------------------------------------------
          ||1        |2        |2        |2        |2        |
          ||2        |2        |2        |2        |2        |
          |---------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(monthlySales.stat.crosstab("month", "empid"), 10) ==
        """-------------------------------------------------------------------
          ||"MONTH"  |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |
          |-------------------------------------------------------------------
          ||JAN      |2                          |2                          |
          ||FEB      |2                          |2                          |
          ||MAR      |2                          |2                          |
          ||APR      |2                          |2                          |
          |-------------------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(date1.stat.crosstab("a", "b"), 10) ==
        """----------------------------------------------------------------------
          ||"A"         |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |
          |----------------------------------------------------------------------
          ||2020-08-01  |1                          |0                          |
          ||2010-12-01  |0                          |1                          |
          |----------------------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(date1.stat.crosstab("b", "a"), 10) ==
        """-----------------------------------------------------------
          ||"B"  |"TO_DATE('2020-08-01')"  |"TO_DATE('2010-12-01')"  |
          |-----------------------------------------------------------
          ||1    |1                        |0                        |
          ||2    |0                        |1                        |
          |-----------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(string7.stat.crosstab("a", "b"), 10) ==
        """----------------------------------------------------------------
          ||"A"   |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |
          |----------------------------------------------------------------
          ||str   |1                          |0                          |
          ||NULL  |0                          |1                          |
          |----------------------------------------------------------------
          |""".stripMargin)

    assert(
      getShowString(string7.stat.crosstab("b", "a"), 10) ==
        """--------------------------
          ||"B"  |"'str'"  |"NULL"  |
          |--------------------------
          ||1    |1        |0       |
          ||2    |0        |0       |
          |--------------------------
          |""".stripMargin)
  }

  test("df.stat.sampleBy") {
    assert(
      getShowString(monthlySales.stat.sampleBy(col("empid"), Map(1 -> 0.0, 2 -> 1.0)), 10) ==
        """--------------------------------
        ||"EMPID"  |"AMOUNT"  |"MONTH"  |
        |--------------------------------
        ||2        |4500      |JAN      |
        ||2        |35000     |JAN      |
        ||2        |200       |FEB      |
        ||2        |90500     |FEB      |
        ||2        |2500      |MAR      |
        ||2        |9500      |MAR      |
        ||2        |800       |APR      |
        ||2        |4500      |APR      |
        |--------------------------------
        |""".stripMargin)

    assert(
      getShowString(monthlySales.stat.sampleBy(col("month"), Map("JAN" -> 1.0)), 10) ==
        """--------------------------------
        ||"EMPID"  |"AMOUNT"  |"MONTH"  |
        |--------------------------------
        ||1        |10000     |JAN      |
        ||1        |400       |JAN      |
        ||2        |4500      |JAN      |
        ||2        |35000     |JAN      |
        |--------------------------------
        |""".stripMargin)

    assert(
      getShowString(monthlySales.stat.sampleBy(col("month"), Map()), 10) ==
        """--------------------------------
        ||"EMPID"  |"AMOUNT"  |"MONTH"  |
        |--------------------------------
        |--------------------------------
        |""".stripMargin)
  }

  // On GitHub Action this test time out. But locally it passed.
  ignore("df.stat.pivot max column test") {
    def randomString(n: Int): String = Random.alphanumeric.filter(_.isLetter).take(n).mkString
    // Local execution time: 1000 -> 25s, 3000 -> 2.5 min, 5000 -> 10 min.
    val df1 = Seq.fill(1000) { (randomString(230), randomString(230)) }.toDF("a", "b")
    getShowString(df1.stat.crosstab("a", "b"), 1)

    val df2 = Seq.fill(1001) { (randomString(230), randomString(230)) }.toDF("a", "b")
    assertThrows[SnowparkClientException](getShowString(df2.stat.crosstab("a", "b"), 1))

    val df3 = Seq.fill(1000) { (1, 1) }.toDF("a", "b")
    assert(
      getShowString(df3.stat.crosstab("a", "b"), 10) ==
        """-----------------------------------
        ||"A"  |"CAST(1 AS NUMBER(38,0))"  |
        |-----------------------------------
        ||1    |1000                       |
        |-----------------------------------
        |""".stripMargin)

    val df4 = Seq.fill(1001) { (1, 1) }.toDF("a", "b")
    assert(
      getShowString(df4.stat.crosstab("a", "b"), 10) ==
        """-----------------------------------
        ||"A"  |"CAST(1 AS NUMBER(38,0))"  |
        |-----------------------------------
        ||1    |1001                       |
        |-----------------------------------
        |""".stripMargin)
  }

  test("select *") {
    val expected = double2.collect()
    checkAnswer(double2.select("*"), expected)
    checkAnswer(double2.select(double2.col("*")), expected)
  }

  test("first") {
    assert(integer1.first().get == Row(1))
    assert(nullData1.first().get == Row(null))
    assert(integer1.filter(col("a") < 0).first().isEmpty)

    integer1.first(2) sameElements Seq(Row(1), Row(2))

    // return all elements
    integer1.first(3) sameElements Seq(Row(1), Row(2), Row(3))
    integer1.first(10) sameElements Seq(Row(1), Row(2), Row(3))
    integer1.first(-10) sameElements Seq(Row(1), Row(2), Row(3))
  }

  test("sample() with row count") {
    val rowCount = 10000
    val df1 = session.range(rowCount)

    // Sample 0 rows.
    assert(df1.sample(0).count() == 0)
    // Sample 10 percent of rows
    val rowCount10Percent: Long = rowCount / 10
    assert(df1.sample(rowCount10Percent).count() == rowCount10Percent)
    // Sample all rows
    assert(df1.sample(rowCount).count() == rowCount)
    // Sample row count is greater than total row count
    assert(df1.sample(rowCount + 10).count() == rowCount)
  }

  test("sample() with fraction") {
    val rowCount = 10000
    val df1 = session.range(rowCount)

    // Sample 0 rows.
    assert(df1.sample(0.0).count() == 0)
    // Sample 50 percent of rows, sample with fraction allows some Deviation.
    val halfRowCount = Math.max(rowCount * 0.5, 1)
    assert(
      Math.abs(df1.sample(0.50).count() - halfRowCount) <
        halfRowCount * samplingDeviation)
    // Sample all rows
    assert(df1.sample(1.0).count() == rowCount)
  }

  test("sample() negative test") {
    val rowCount = 10000
    val df1 = session.range(rowCount)

    // valid row count range [0, 1_000_000]
    assertThrows[SnowflakeSQLException](df1.sample(-1).count())
    assertThrows[SnowflakeSQLException](df1.sample(1000001).count())
    // valid fraction range [0.0, 1.0]
    assertThrows[SnowflakeSQLException](df1.sample(-0.01).count())
    assertThrows[SnowflakeSQLException](df1.sample(1.01).count())
  }

  test("sample() on join") {
    val rowCount = 10000
    val df1 = session.range(rowCount).withColumn(""""name"""", lit("value1"))
    val df2 = session.range(rowCount).withColumn(""""name"""", lit("value2"))

    val result = df1.join(df2, """"ID"""")
    val sampleRowCount: Long = Math.max(result.count() / 10, 1)

    assert(result.sample(sampleRowCount).count() == sampleRowCount)
    assert(
      Math.abs(result.sample(0.10).count() - sampleRowCount) <
        sampleRowCount * samplingDeviation)
  }

  test("sample() on union") {
    val rowCount = 10000
    val df1 = session.range(rowCount).withColumn(""""name"""", lit("value1"))
    val df2 = session.range(5000, 5000 + rowCount).withColumn(""""name"""", lit("value2"))

    // Test union
    var result = df1.union(df2)
    var sampleRowCount: Long = Math.max(result.count() / 10, 1)
    assert(result.sample(sampleRowCount).count() == sampleRowCount)
    assert(
      Math.abs(result.sample(0.10).count() - sampleRowCount) <
        sampleRowCount * samplingDeviation)
    // Test union all
    result = df1.unionAll(df2)
    sampleRowCount = Math.max(result.count() / 10, 1)
    assert(result.sample(sampleRowCount).count() == sampleRowCount)
    assert(
      Math.abs(result.sample(0.10).count() - sampleRowCount) <
        sampleRowCount * samplingDeviation)
  }

  test("randomSplit()") {
    val rowCount = 10000
    val df1 = session.range(rowCount)
    var partCount0 = 0L
    var partCount1 = 0L
    var partCount2 = 0L

    def checkPartRowCount(
        weights: Array[Double],
        index: Int,
        count: Long,
        TotalCount: Long): Unit = {
      val expectedRowCount = TotalCount * weights(index) / weights.sum
      assert(Math.abs(expectedRowCount - count) < expectedRowCount * samplingDeviation)
    }

    // Array has 1 weight, return the underlining DataFrame.
    var parts = df1.randomSplit(Array(0.2))
    assert(parts.size == 1 && parts(0) == df1)

    // Split as 2 parts
    var weights = Array(0.2, 0.8)
    parts = df1.randomSplit(weights)
    assert(parts.size == weights.size)
    partCount0 = parts(0).count()
    partCount1 = parts(1).count()
    assert(partCount0 + partCount1 == rowCount)
    checkPartRowCount(weights, 0, partCount0, rowCount)
    checkPartRowCount(weights, 1, partCount1, rowCount)

    // Split 2 parts and weights needs to be normalized
    weights = Array(0.11111, 0.66666)
    parts = df1.randomSplit(weights)
    assert(parts.size == weights.size)
    partCount0 = parts(0).count()
    partCount1 = parts(1).count()
    assert(partCount0 + partCount1 == rowCount)
    checkPartRowCount(weights, 0, partCount0, rowCount)
    checkPartRowCount(weights, 1, partCount1, rowCount)

    // Split 3 parts and weights needs to be normalized
    weights = Array(0.11111, 0.66666, 1.3)
    parts = df1.randomSplit(weights)
    assert(parts.size == weights.size)
    partCount0 = parts(0).count()
    partCount1 = parts(1).count()
    partCount2 = parts(2).count()
    assert(partCount0 + partCount1 + partCount2 == rowCount)
    checkPartRowCount(weights, 0, partCount0, rowCount)
    checkPartRowCount(weights, 1, partCount1, rowCount)
    checkPartRowCount(weights, 2, partCount2, rowCount)
  }

  test("randomSplit() negative test") {
    val df1 = session.range(10)

    // parameter 'weights' can't be empty
    assertThrows[SnowparkClientException](df1.randomSplit(Array.empty))

    // weight must be positive
    assertThrows[SnowparkClientException](df1.randomSplit(Array(-0.1, -0.2)))
    assertThrows[SnowparkClientException](df1.randomSplit(Array(0.1, 0.0)))
  }

  test("Test toDF()") {
    // toDF(String, String*) with 1 column
    val df1 = Seq(1, 2, 3).toDF("a")
    assert(df1.count() == 3 && df1.schema.size == 1 && df1.schema.head.name.equals("A"))
    df1.show()
    // toDF(Seq[String]) with 1 column
    val df2 = Seq(1, 2, 3).toDF(Seq("a"))
    assert(df2.count() == 3 && df2.schema.size == 1 && df2.schema.head.name.equals("A"))
    df2.show()

    // toDF(String, String*) with 2 column
    val df3 = Seq((1, null), (2, "NotNull"), (3, null)).toDF("a", "b")
    assert(df3.count() == 3 && df3.schema.size == 2)
    assert(df3.schema.head.name.equals("A") && df3.schema.last.name.equals("B"))
    // toDF(Seq[String]) with 2 column
    val df4 = Seq((1, null), (2, "NotNull"), (3, null)).toDF(Seq("a", "b"))
    assert(df4.count() == 3 && df4.schema.size == 2)
    assert(df4.schema.head.name.equals("A") && df4.schema.last.name.equals("B"))

    // toDF(String, String*) with 3 column
    val df5 = Seq((1, null, "a"), (2, "NotNull", "a"), (3, null, "a")).toDF("a", "b", "c")
    assert(df5.count() == 3 && df5.schema.size == 3)
    assert(df5.schema.head.name.equals("A") && df5.schema.last.name.equals("C"))
    // toDF(Seq[String]) with 3 column
    val df6 = Seq((1, null, "a"), (2, "NotNull", "a"), (3, null, "a")).toDF(Seq("a", "b", "c"))
    assert(df6.count() == 3 && df6.schema.size == 3)
    assert(df6.schema.head.name.equals("A") && df6.schema.last.name.equals("C"))
  }

  test("toDF() negative test") {
    val values = Seq((1, null), (2, "NotNull"), (3, null))

    // toDF(String, String*) with invalid args count
    assertThrows[IllegalArgumentException]({ values.toDF("a") })
    assertThrows[IllegalArgumentException]({ values.toDF("a", "b", "c") })

    // toDF(Seq[String]) with invalid args count
    assertThrows[IllegalArgumentException]({ values.toDF(Seq.empty) })
    assertThrows[IllegalArgumentException]({ values.toDF(Seq("a")) })
    assertThrows[IllegalArgumentException]({ values.toDF(Seq("a", "b", "c")) })
  }

  test("test sort()") {
    val df =
      Seq((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3), (3, 1), (3, 2), (3, 3))
        .toDF("a", "b")

    // order ASC with 1 column
    var sortedRows = df.sort(col("a").asc).collect()
    for (i <- 1 until sortedRows.length) {
      assert(sortedRows(i - 1).getLong(0) <= sortedRows(i).getLong(0))
    }
    // order DESC with 1 column
    sortedRows = df.sort(col("a").desc).collect()
    for (i <- 1 until sortedRows.length) {
      assert(sortedRows(i - 1).getLong(0) >= sortedRows(i).getLong(0))
    }

    // order ASC with 2 column
    sortedRows = df.sort(col("a").asc, col("b").asc).collect()
    for (i <- 1 until sortedRows.length) {
      assert(
        sortedRows(i - 1).getLong(0) < sortedRows(i).getLong(0) ||
          (sortedRows(i - 1).getLong(0) == sortedRows(i).getLong(0) &&
            sortedRows(i - 1).getLong(1) <= sortedRows(i).getLong(1)))
    }
    // order DESC with 2 column
    sortedRows = df.sort(col("a").desc, col("b").desc).collect()
    for (i <- 1 until sortedRows.length) {
      assert(
        sortedRows(i - 1).getLong(0) > sortedRows(i).getLong(0) ||
          (sortedRows(i - 1).getLong(0) == sortedRows(i).getLong(0) &&
            sortedRows(i - 1).getLong(1) >= sortedRows(i).getLong(1)))
    }

    // Negative test: sort() needs at least one sort expression.
    assertThrows[SnowparkClientException]({ df.sort(Seq.empty) })
  }

  test("test select()") {
    val df = Seq((1, "a", 10), (2, "b", 20), (3, "c", 30)).toDF("a", "b", "c")

    // select(String, String*) with 1 column
    var expectedResult = Seq(Row(1), Row(2), Row(3))
    checkAnswer(df.select("a"), expectedResult)
    // select(Seq[String]) with 1 column
    checkAnswer(df.select(Seq("a")), expectedResult)
    // select(Column, Column*) with 1 column
    checkAnswer(df.select(col("a")), expectedResult)
    // select(Seq[Column]) with 1 column
    checkAnswer(df.select(Seq(col("a"))), expectedResult)

    expectedResult = Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30))
    // select(String, String*) with 3 columns
    checkAnswer(df.select("a", "b", "c"), expectedResult)
    // select(Seq[String]) with 3 column
    checkAnswer(df.select(Seq("a", "b", "c")), expectedResult)
    // select(Column, Column*) with 3 column
    checkAnswer(df.select(col("a"), col("b"), col("c")), expectedResult)
    // select(Seq[Column]) with 3 column
    checkAnswer(df.select(Seq(col("a"), col("b"), col("c"))), expectedResult)

    // test col("a") + col("c")
    expectedResult = Seq(Row("a", 11), Row("b", 22), Row("c", 33))
    // select(Column, Column*) with col("a") + col("b")
    checkAnswer(df.select(col("b"), col("a") + col("c")), expectedResult)
    // select(Seq[Column]) with col("a") + col("b")
    checkAnswer(df.select(Seq(col("b"), col("a") + col("c"))), expectedResult)
  }

  test("select() negative test") {
    val df = Seq((1, "a", 10), (2, "b", 20), (3, "c", 30)).toDF("a", "b", "c")

    // Select with empty Seq
    assertThrows[IllegalArgumentException]({ df.select(Seq.empty[String]) })
    assertThrows[IllegalArgumentException]({ df.select(Seq.empty[Column]) })

    // select column which doesn't exist.
    assertThrows[SnowflakeSQLException]({ df.select("not_exist_column").collect() })
    assertThrows[SnowflakeSQLException]({ df.select(Seq("not_exist_column")).collect() })
    assertThrows[SnowflakeSQLException]({ df.select(col("not_exist_column")).collect() })
    assertThrows[SnowflakeSQLException]({ df.select(Seq(col("not_exist_column"))).collect() })
  }

  test("drop() and dropColumns()") {
    val df = Seq((1, "a", 10), (2, "b", 20), (3, "c", 30)).toDF("a", "b", "c")

    // drop non-exist-column (do nothing)
    var expectedResult = Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30))
    checkAnswer(df.drop("not_exist_column"), expectedResult)
    checkAnswer(df.drop(Seq("not_exist_column")), expectedResult)
    checkAnswer(df.drop(col("not_exist_column")), expectedResult)
    checkAnswer(df.drop(Seq(col("not_exist_column"))), expectedResult)

    // drop 1st column
    expectedResult = Seq(Row("a", 10), Row("b", 20), Row("c", 30))
    checkAnswer(df.drop("a"), expectedResult)
    checkAnswer(df.drop(Seq("a")), expectedResult)
    checkAnswer(df.drop(col("a")), expectedResult)
    checkAnswer(df.drop(Seq(col("a"))), expectedResult)

    // drop 2nd column
    expectedResult = Seq(Row(1, 10), Row(2, 20), Row(3, 30))
    checkAnswer(df.drop("b"), expectedResult)
    checkAnswer(df.drop(Seq("b")), expectedResult)
    checkAnswer(df.drop(col("b")), expectedResult)
    checkAnswer(df.drop(Seq(col("b"))), expectedResult)

    // drop 2nd and 3rd columns
    expectedResult = Seq(Row(1), Row(2), Row(3))
    checkAnswer(df.drop("b", "c"), expectedResult)
    checkAnswer(df.drop(Seq("c", "b")), expectedResult)
    checkAnswer(df.drop(col("b"), col("c")), expectedResult)
    checkAnswer(df.drop(Seq(col("c"), col("b"))), expectedResult)

    // drop all columns (negative test)
    assertThrows[SnowparkClientException]({ df.drop("a", "b", "c").collect() })
    assertThrows[SnowparkClientException]({ df.drop(Seq("a", "b", "c")).collect() })
    assertThrows[SnowparkClientException]({ df.drop(col("a"), col("b"), col("c")).collect() })
    assertThrows[SnowparkClientException]({ df.drop(Seq(col("a"), col("b"), col("c"))).collect() })
  }

  test("DataFrame.agg()") {
    val df = Seq((1, "One"), (2, "Two"), (3, "Three")).toDF("empid", "name")

    // Agg() on 1 column
    checkAnswer(df.agg(max(col("empid"))), Seq(Row(3)))
    checkAnswer(df.agg(Seq(min(col("empid")))), Seq(Row(1)))
    checkAnswer(df.agg("empid" -> "max"), Seq(Row(3)))
    checkAnswer(df.agg(Seq("empid" -> "avg")), Seq(Row(2.0)))

    // Agg() on 2 columns
    checkAnswer(df.agg(max(col("empid")), max(col("name"))), Seq(Row(3, "Two")))
    checkAnswer(df.agg(Seq(min(col("empid")), min(col("name")))), Seq(Row(1, "One")))
    checkAnswer(df.agg("empid" -> "max", "name" -> "max"), Seq(Row(3, "Two")))
    checkAnswer(df.agg(Seq("empid" -> "min", "name" -> "min")), Seq(Row(1, "One")))
  }

  test("rollup()") {
    val df = Seq(
      ("country A", "state A", 50),
      ("country A", "state A", 50),
      ("country A", "state B", 5),
      ("country A", "state B", 5),
      ("country B", "state A", 100),
      ("country B", "state A", 100),
      ("country B", "state B", 10),
      ("country B", "state B", 10))
      .toDF("country", "state", "value")

    // At least one column needs to be provided ( negative test )
    assertThrows[SnowflakeSQLException]({
      df.rollup(Seq.empty[String]).agg(sum(col("value"))).show()
    })
    assertThrows[SnowflakeSQLException]({
      df.rollup(Seq.empty[Column]).agg(sum(col("value"))).show()
    })

    // rollup() on 1 column
    var expectedResult = Seq(Row("country A", 110), Row("country B", 220), Row(null, 330))
    checkAnswer(df.rollup("country").agg(sum(col("value"))), expectedResult)
    checkAnswer(df.rollup(Seq("country")).agg(sum(col("value"))), expectedResult)
    checkAnswer(df.rollup(col("country")).agg(sum(col("value"))), expectedResult)
    checkAnswer(df.rollup(Seq(col("country"))).agg(sum(col("value"))), expectedResult)

    // rollup() on 2 columns
    expectedResult = Seq(
      Row(null, null, 330),
      Row("country A", null, 110),
      Row("country A", "state A", 100),
      Row("country A", "state B", 10),
      Row("country B", null, 220),
      Row("country B", "state A", 200),
      Row("country B", "state B", 20))
    checkAnswer(
      df.rollup("country", "state")
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
    checkAnswer(
      df.rollup(Seq("country", "state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
    checkAnswer(
      df.rollup(col("country"), col("state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
    checkAnswer(
      df.rollup(Seq(col("country"), col("state")))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
  }

  test("groupBy()") {
    val df = Seq(
      ("country A", "state A", 50),
      ("country A", "state A", 50),
      ("country A", "state B", 5),
      ("country A", "state B", 5),
      ("country B", "state A", 100),
      ("country B", "state A", 100),
      ("country B", "state B", 10),
      ("country B", "state B", 10))
      .toDF("country", "state", "value")

    // groupBy() without column
    checkAnswer(df.groupBy().agg(max(col("value"))), Seq(Row(100)))
    checkAnswer(df.groupBy(Seq.empty[String]).agg(sum(col("value"))), Seq(Row(330)))
    checkAnswer(df.groupBy(Seq.empty[Column]).agg(sum(col("value"))), Seq(Row(330)))

    // groupBy() on 1 column
    var expectedResult = Seq(Row("country A", 110), Row("country B", 220))
    checkAnswer(df.groupBy("country").agg(sum(col("value"))), expectedResult)
    checkAnswer(df.groupBy(Seq("country")).agg(sum(col("value"))), expectedResult)
    checkAnswer(df.groupBy(col("country")).agg(sum(col("value"))), expectedResult)
    checkAnswer(df.groupBy(Seq(col("country"))).agg(sum(col("value"))), expectedResult)

    // groupBy() on 2 columns
    expectedResult = Seq(
      Row("country A", "state A", 100),
      Row("country A", "state B", 10),
      Row("country B", "state A", 200),
      Row("country B", "state B", 20))
    checkAnswer(
      df.groupBy("country", "state")
        .agg(sum(col("value"))),
      expectedResult)
    checkAnswer(
      df.groupBy(Seq("country", "state"))
        .agg(sum(col("value"))),
      expectedResult)
    checkAnswer(
      df.groupBy(col("country"), col("state"))
        .agg(sum(col("value"))),
      expectedResult)
    checkAnswer(
      df.groupBy(Seq(col("country"), col("state")))
        .agg(sum(col("value"))),
      expectedResult)
  }

  test("cube()") {
    val df = Seq(
      ("country A", "state A", 50),
      ("country A", "state A", 50),
      ("country A", "state B", 5),
      ("country A", "state B", 5),
      ("country B", "state A", 100),
      ("country B", "state A", 100),
      ("country B", "state B", 10),
      ("country B", "state B", 10))
      .toDF("country", "state", "value")

    // At least one column needs to be provided ( negative test )
    assertThrows[SnowflakeSQLException]({
      df.cube(Seq.empty[String]).agg(sum(col("value"))).show()
    })
    assertThrows[SnowflakeSQLException]({
      df.cube(Seq.empty[Column]).agg(sum(col("value"))).show()
    })

    // cube() on 1 column
    var expectedResult = Seq(Row("country A", 110), Row("country B", 220), Row(null, 330))
    checkAnswer(df.cube("country").agg(sum(col("value"))), expectedResult)
    checkAnswer(df.cube(Seq("country")).agg(sum(col("value"))), expectedResult)
    checkAnswer(df.cube(col("country")).agg(sum(col("value"))), expectedResult)
    checkAnswer(df.cube(Seq(col("country"))).agg(sum(col("value"))), expectedResult)
    checkAnswer(df.cube(Array(col("country"))).agg(sum(col("value"))), expectedResult)

    // cube() on 2 columns
    expectedResult = Seq(
      Row(null, null, 330),
      Row(null, "state A", 300), // This is an extra row comparing with rollup().
      Row(null, "state B", 30), // This is an extra row comparing with rollup().
      Row("country A", null, 110),
      Row("country A", "state A", 100),
      Row("country A", "state B", 10),
      Row("country B", null, 220),
      Row("country B", "state A", 200),
      Row("country B", "state B", 20))
    checkAnswer(
      df.cube("country", "state")
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
    checkAnswer(
      df.cube(Seq("country", "state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
    checkAnswer(
      df.cube(col("country"), col("state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
    checkAnswer(
      df.cube(Seq(col("country"), col("state")))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      false)
  }

  test("flatten") {
    val table = session.sql("select parse_json(a) as a from values('[1,2]') as T(a)")
    checkAnswer(table.flatten(table("a")).select("value"), Seq(Row("1"), Row("2")))

    // conflict column names
    val table1 = session.sql("select parse_json(value) as value from values('[1,2]') as T(value)")
    val flatten = table1.flatten(table1("value"), "", outer = false, recursive = false, "both")
    checkAnswer(
      flatten.select(table1("value"), flatten("value")),
      Seq(Row("[\n  1,\n  2\n]", "1"), Row("[\n  1,\n  2\n]", "2")),
      sort = false)

    // multiple flatten
    val flatten1 =
      flatten.flatten(table1("value"), "[0]", outer = true, recursive = true, "array")
    checkAnswer(
      flatten1.select(table1("value"), flatten("value"), flatten1("value")),
      Seq(Row("[\n  1,\n  2\n]", "1", "1"), Row("[\n  1,\n  2\n]", "2", "1")),
      sort = false)

    // wrong mode
    assertThrows[SnowparkClientException](
      flatten.flatten(col("value"), "", outer = false, recursive = false, "wrong"))

    // contains multiple query
    val df = session.sql("show tables").limit(1)
    val df1 = df.withColumn("value", lit("[1,2]")).select(parse_json(col("value")).as("value"))
    val flatten2 = df1.flatten(df1("value"))
    checkAnswer(flatten2.select(flatten2("value")), Seq(Row("1"), Row("2")), sort = false)

    // flatten with object traversing
    val table2 = session
      .sql("select * from values('{\"a\":[1,2]}') as T(a)")
      .select(parse_json(col("a")).as("a"))

    val flatten3 = table2.flatten(table2("a")("a"))
    checkAnswer(flatten3.select(flatten3("value")), Seq(Row("1"), Row("2")), sort = false)

    // join
    val df2 = table.flatten(table("a")).select(col("a"), col("value"))
    val df3 = table2.flatten(table2("a")("a")).select(col("a"), col("value"))

    checkAnswer(
      df2.join(df3, df2("value") === df3("value")).select(df3("value")),
      Seq(Row("1"), Row("2")),
      sort = false)

    // union
    checkAnswer(
      df2.union(df3).select(col("value")),
      Seq(Row("1"), Row("2"), Row("1"), Row("2")),
      sort = false)
  }

  test("flatten in session") {
    checkAnswer(
      session.flatten(parse_json(lit("""["a","'"]"""))).select(col("value")),
      Seq(Row("\"a\""), Row("\"'\"")),
      sort = false)

    checkAnswer(
      session
        .flatten(parse_json(lit("""{"a":[1,2]}""")), "a", outer = true, recursive = true, "ARRAY")
        .select("value"),
      Seq(Row("1"), Row("2")))

    assertThrows[SnowparkClientException](
      session.flatten(parse_json(lit("[1]")), "", outer = false, recursive = false, "wrong"))

    val df1 = session.flatten(parse_json(lit("[1,2]")))
    val df2 =
      session.flatten(
        parse_json(lit("""{"a":[1,2]}""")),
        "a",
        outer = false,
        recursive = false,
        "BOTH")

    // union
    checkAnswer(
      df1.union(df2).select("path"),
      Seq(Row("[0]"), Row("[1]"), Row("a[0]"), Row("a[1]")),
      sort = false)

    // join
    checkAnswer(
      df1
        .join(df2, df1("value") === df2("value"))
        .select(df1("path").as("path1"), df2("path").as("path2")),
      Seq(Row("[0]", "a[0]"), Row("[1]", "a[1]")),
      sort = false)

  }

  test("createDataFrame with given schema") {
    val schema = StructType(
      Seq(
        StructField("string", StringType),
        StructField("byte", ByteType),
        StructField("short", ShortType),
        StructField("int", IntegerType),
        StructField("long", LongType),
        StructField("float", FloatType),
        StructField("double", DoubleType),
        StructField("number", DecimalType(10, 3)),
        StructField("boolean", BooleanType),
        StructField("binary", BinaryType),
        StructField("timestamp", TimestampType),
        StructField("date", DateType)))

    val timestamp: Long = 1606179541282L
    val data = Seq(
      Row(
        "a",
        1.toByte,
        2.toShort,
        3,
        4L,
        1.1F,
        1.2,
        BigDecimal(1.2),
        true,
        Array(1.toByte, 2.toByte),
        new Timestamp(timestamp - 100),
        new Date(timestamp - 100)),
      Row(null, null, null, null, null, null, null, null, null, null, null, null))

    val result = session.createDataFrame(data, schema)
    // byte, short, int, long are converted to long
    // float and double are converted to double
    result.schema.printTreeString()
    assert(
      getSchemaString(result.schema) ==
        """root
          | |--STRING: String (nullable = true)
          | |--BYTE: Long (nullable = true)
          | |--SHORT: Long (nullable = true)
          | |--INT: Long (nullable = true)
          | |--LONG: Long (nullable = true)
          | |--FLOAT: Double (nullable = true)
          | |--DOUBLE: Double (nullable = true)
          | |--NUMBER: Decimal(10, 3) (nullable = true)
          | |--BOOLEAN: Boolean (nullable = true)
          | |--BINARY: Binary (nullable = true)
          | |--TIMESTAMP: Timestamp (nullable = true)
          | |--DATE: Date (nullable = true)
          |""".stripMargin)
    checkAnswer(result, data, sort = false)
  }

  // can't correctly collect Time data, todo: fix in SNOW-274402
  test("createDataFrame with given schema: Time") {
    val schema = StructType(Seq(StructField("Time", TimeType)))
    val data = Seq(Row(Time.valueOf("10:11:12")), Row(null))

    val df = session.createDataFrame(data, schema)
    df.schema.printTreeString()
    assert(
      getSchemaString(df.schema) ===
        """root
          | |--TIME: Time (nullable = true)
          |""".stripMargin)
  }

  // In the result, Array, Map and Geography are String data
  test("createDataFrame with given schema: Array, Map, Variant") {
    val schema = StructType(
      Seq(
        StructField("array", ArrayType(null)),
        StructField("map", MapType(null, null)),
        StructField("variant", VariantType),
        StructField("geography", GeographyType)))
    val data = Seq(
      Row(Array("'", 2), Map("'" -> 1), new Variant(1), Geography.fromGeoJSON("POINT(30 10)")),
      Row(null, null, null, null, null))

    val df = session.createDataFrame(data, schema)
    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ARRAY: Array (nullable = true)
          | |--MAP: Map (nullable = true)
          | |--VARIANT: Variant (nullable = true)
          | |--GEOGRAPHY: Geography (nullable = true)
          |""".stripMargin)
    df.show()
    val expected =
      Seq(
        Row(
          "[\n  \"'\",\n  2\n]",
          "{\n  \"'\": 1\n}",
          "1",
          Geography.fromGeoJSON("""{
                                  |  "coordinates": [
                                  |    30,
                                  |    10
                                  |  ],
                                  |  "type": "Point"
                                  |}""".stripMargin)),
        Row(null, null, null, null))
    checkAnswer(df, expected, sort = false)
  }

  test("variant in array and map") {
    val schema = StructType(
      Seq(StructField("array", ArrayType(null)), StructField("map", MapType(null, null))))
    val data = Seq(Row(Array(new Variant(1), new Variant("\"'")), Map("a" -> new Variant("\"'"))))
    val df = session.createDataFrame(data, schema)
    checkAnswer(df, Seq(Row("[\n  1,\n  \"\\\"'\"\n]", "{\n  \"a\": \"\\\"'\"\n}")))
  }

  test("geography in array and map") {
    val schema = StructType(Seq(StructField("array", ArrayType(null))))
    val data = Seq(
      Row(
        Array(
          Geography.fromGeoJSON("point(30 10)"),
          Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"))))
    checkAnswer(
      session.createDataFrame(data, schema),
      Seq(
        Row("[\n  \"point(30 10)\",\n  {\n    \"coordinates\": [\n" +
          "      30,\n      10\n    ],\n    \"type\": \"Point\"\n  }\n]")))

    val schema1 = StructType(Seq(StructField("map", MapType(null, null))))
    val data1 = Seq(
      Row(
        Map(
          "a" -> Geography.fromGeoJSON("point(30 10)"),
          "b" -> Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"))))
    checkAnswer(
      session.createDataFrame(data1, schema1),
      Seq(
        Row("{\n  \"a\": \"point(30 10)\",\n  \"b\": {\n    \"coordinates\": [\n" +
          "      30,\n      10\n    ],\n    \"type\": \"Point\"\n  }\n}")))
  }

  test("escaped character") {
    val df = Seq("'", "\\", "\n").toDF("a")
    checkAnswer(df, Seq(Row("'"), Row("\\"), Row("\n")), sort = false)
  }

  // Exclude this test in Java Stored Proc, because it creates a new session,
  // which is not supported yet.
  test("create or replace temporary view", JavaStoredProcExclude) {
    val viewName = randomName()
    val viewName1 = s""""$viewName%^11""""
    val viewName2 = s""""$viewName""""
    try {
      val df = Seq(1, 2, 3).toDF("a")
      df.createOrReplaceTempView(viewName)
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))

      // test replace
      val df2 = Seq("a", "b", "c").toDF("b")
      df2.createOrReplaceTempView(viewName)
      checkAnswer(session.table(viewName), Seq(Row("a"), Row("b"), Row("c")), sort = false)

      // view name has special char
      df.createOrReplaceTempView(viewName1)
      checkAnswer(session.table(quoteName(viewName1)), Seq(Row(1), Row(2), Row(3)))

      // view name has quote
      df.createOrReplaceTempView(viewName2)
      checkAnswer(session.table(quoteName(viewName2)), Seq(Row(1), Row(2), Row(3)))

      // check if it is session temporary
      val newSession = Session.builder.configFile(defaultProfile).create
      val msg = intercept[SnowflakeSQLException](newSession.table(viewName).show())
      assert(msg.getMessage.contains("does not exist or not authorized"))
    } finally {
      dropView(viewName)
      dropView(viewName1)
      dropView(viewName2)
    }
  }

  test("create DataFrame with Schema Inference") {
    val df1 = Seq(1, 2, 3).toDF("int")
    checkAnswer(df1, Seq(Row(1), Row(2), Row(3)))
    val schema1 = df1.schema
    assert(schema1.length == 1)
    assert(schema1.head.name == "INT")
    assert(schema1.head.dataType == LongType)

    // tuple
    val df2 = Seq((true, "a"), (false, "b")).toDF("boolean", "string")
    checkAnswer(df2, Seq(Row(true, "a"), Row(false, "b")), sort = false)

    // case class
    val df3 =
      session.createDataFrame(Seq(Table1(new Variant(1), Geography.fromGeoJSON("point(10 10)"))))
    df3.schema.printTreeString()
    checkAnswer(
      df3,
      Seq(
        Row(
          "1",
          Geography.fromGeoJSON("""{
                                  |  "coordinates": [
                                  |    10,
                                  |    10
                                  |  ],
                                  |  "type": "Point"
                                  |}""".stripMargin))))
  }

  case class Table1(variant: Variant, geography: Geography)

  test("create nullable dataFrame with schema inference") {
    val df = Seq((1, Some(1), None), (2, Some(3), Some(true)))
      .toDF("a", "b", "c")
    assert(getSchemaString(df.schema) == """root
                                  | |--A: Long (nullable = false)
                                  | |--B: Long (nullable = false)
                                  | |--C: Boolean (nullable = true)
                                  |""".stripMargin)
    checkAnswer(df, Seq(Row(1, 1, null), Row(2, 3, true)), sort = false)
  }

  test("createDataFrame from empty Seq with schema inference") {
    Seq.empty[(Int, Int)].toDF("a", "b").schema.printTreeString()
  }

  test("schema inference binary type") {
    assert(
      getSchemaString(
        session
          .createDataFrame(Seq(
            (Some(Array(1.toByte, 2.toByte)), Array(3.toByte, 4.toByte)),
            (None, Array.empty[Byte])))
          .schema) ==
        """root
          | |--_1: Binary (nullable = true)
          | |--_2: Binary (nullable = false)
          |""".stripMargin)
  }

  test("primitive array") {
    checkAnswer(
      session
        .createDataFrame(
          Seq(Row(Array(1))),
          StructType(Seq(StructField("arr", ArrayType(null))))),
      Seq(Row("[\n  1\n]")))
  }

  test("time, date and timestamp test") {
    assert(
      session.sql("select '00:00:00' :: Time").collect()(0).getTime(0).toString == "00:00:00")
    assert(
      session
        .sql("select '1970-1-1 00:00:00' :: Timestamp")
        .collect()(0)
        .getTimestamp(0)
        .toString == "1970-01-01 00:00:00.0")
    assert(
      session.sql("select '1970-1-1' :: Date").collect()(0).getDate(0).toString == "1970-01-01")
  }

  test("quoted column names") {
    val normalName = "NORMAL_NAME"
    val lowerCaseName = "\"lower_case\""
    val quoteStart = "\"\"\"quote_start\""
    val quoteEnd = "\"quote_end\"\"\""
    val quoteMiddle = "\"quote_\"\"_mid\""
    val quoteAllCases = "\"\"\"quote_\"\"_start\"\"\""

    createTable(
      tableName,
      s"$normalName int, $lowerCaseName int, $quoteStart int," +
        s"$quoteEnd int, $quoteMiddle int, $quoteAllCases int")
    runQuery(s"insert into $tableName values(1, 2, 3, 4, 5, 6)", session)

    // Test select()
    val df1 = session
      .table(tableName)
      .select(normalName, lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)
    val schema1 = df1.schema
    assert(
      schema1.fields.length == 6 &&
        schema1.fields(0).name.equals(normalName) &&
        schema1.fields(1).name.equals(lowerCaseName) &&
        schema1.fields(2).name.equals(quoteStart) &&
        schema1.fields(3).name.equals(quoteEnd) &&
        schema1.fields(4).name.equals(quoteMiddle) &&
        schema1.fields(5).name.equals(quoteAllCases))
    checkAnswer(df1, Seq(Row(1, 2, 3, 4, 5, 6)))

    // Test select() + cacheResult() + select()
    val df2 = session
      .table(tableName)
      .select(normalName, lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)
      .cacheResult()
      .select(normalName, lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)
    val schema2 = df2.schema
    assert(
      schema2.fields.length == 6 &&
        schema2.fields(0).name.equals(normalName) &&
        schema2.fields(1).name.equals(lowerCaseName) &&
        schema2.fields(2).name.equals(quoteStart) &&
        schema2.fields(3).name.equals(quoteEnd) &&
        schema2.fields(4).name.equals(quoteMiddle) &&
        schema2.fields(5).name.equals(quoteAllCases))
    checkAnswer(df2, Seq(Row(1, 2, 3, 4, 5, 6)))

    // Test drop()
    val df3 = session
      .table(tableName)
      .drop(lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)
    val schema3 = df3.schema
    assert(schema3.fields.length == 1 && schema3.fields.head.name.equals(normalName))
    checkAnswer(df3, Seq(Row(1)))

    // Test select() + cacheResult() + drop()
    val df4 = session
      .table(tableName)
      .select(normalName, lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)
      .cacheResult()
      .drop(lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)
    val schema4 = df4.schema
    assert(schema4.fields.length == 1 && schema4.fields.head.name.equals(normalName))
    checkAnswer(df4, Seq(Row(1)))
  }

  test("quoted column names without surrounding quote") {
    val normalName = "NORMAL_NAME"
    val lowerCaseName = "\"lower_case\""
    val quoteStart = "\"\"\"quote_start\""
    val quoteEnd = "\"quote_end\"\"\""
    val quoteMiddle = "\"quote_\"\"_mid\""
    val quoteAllCases = "\"\"\"quote_\"\"_start\"\"\""

    createTable(
      tableName,
      s"$normalName int, $lowerCaseName int, $quoteStart int," +
        s"$quoteEnd int, $quoteMiddle int, $quoteAllCases int")
    runQuery(s"insert into $tableName values(1, 2, 3, 4, 5, 6)", session)

    // Test simplified input format
    val quoteStart2 = "\"quote_start"
    val quoteEnd2 = "quote_end\""
    val quoteMiddle2 = "quote_\"_mid"
    val df1 = session
      .table(tableName)
      .select(quoteStart2, quoteEnd2, quoteMiddle2)
    // Even the input format can be simplified format, the returned column is the same.
    val schema1 = df1.schema
    assert(
      schema1.fields.length == 3 &&
        schema1.fields(0).name.equals(quoteStart) &&
        schema1.fields(1).name.equals(quoteEnd) &&
        schema1.fields(2).name.equals(quoteMiddle))
    checkAnswer(df1, Seq(Row(3, 4, 5)))
  }

  test("negative test for user input invalid quoted name") {
    val df = Seq(1, 2, 3).toDF("a")
    val ex = intercept[SnowparkClientException] {
      df.where(col(s""""A" = "A" --"""") === 2).collect()
    }
    assert(ex.getMessage.contains("Invalid identifier"))
  }

  test("clone with union DataFrame") {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 1),(2, 2)", session)
    val df = session.table(tableName)
    val unionDF = df.union(df)
    val clonedUnionDF = unionDF.clone
    checkAnswer(clonedUnionDF, Seq(Row(1, 1), Row(2, 2)))
  }

  test("clone with unionAll DataFrame") {
    createTable(tableName, s"c1 int, c2 int")
    runQuery(s"insert into $tableName values(1, 1),(2, 2)", session)
    val df = session.table(tableName)
    val unionDF = df.unionAll(df)
    val clonedUnionDF = unionDF.clone
    checkAnswer(clonedUnionDF, Seq(Row(1, 1), Row(1, 1), Row(2, 2), Row(2, 2)))
  }

  test("clone with cache") {
    val df = session.sql("show tables").select("\"created_on\"", "\"name\"").cacheResult()
    val clonedDF = df.clone()
    val result = df.collect()
    clonedDF.show()
    checkAnswer(clonedDF, result, false)
  }

  test("toLocalIterator") {
    val df = Seq(1, 2, 3).toDF("a")

    val array = df.collect()
    val iterator = df.toLocalIterator
    var index = 0
    while (iterator.hasNext) {
      // call it one more time, make sure this function doesn't change any thing
      assert(iterator.hasNext)
      assert(iterator.next() == array(index))
      index += 1
    }
    assert(index == array.length)
  }

  test("toLocalIterator close iterator") {
    import java.io.Closeable
    val df = Seq(1, 2, 3).toDF("a")

    val array = df.collect()
    var iterator = df.toLocalIterator
    // Close the Iterator without consuming any rows
    iterator.asInstanceOf[Closeable].close()
    assert(!iterator.hasNext)

    // Close the Iterator after first calling hasNext
    iterator = df.toLocalIterator
    assert(iterator.hasNext)
    iterator.asInstanceOf[Closeable].close()
    assert(!iterator.hasNext)

    // Close the Iterator after consuming the first row
    iterator = df.toLocalIterator
    assert(iterator.hasNext)
    assert(iterator.next() == array(0))
    iterator.asInstanceOf[Closeable].close()
    assert(!iterator.hasNext)

    // Close the Iterator after consuming the second row
    iterator = df.toLocalIterator
    assert(iterator.hasNext)
    assert(iterator.next() == array(0))
    assert(iterator.hasNext)
    assert(iterator.next() == array(1))
    assert(iterator.hasNext)
    iterator.asInstanceOf[Closeable].close()
    assert(!iterator.hasNext)

    // Close the Iterator after consuming all the rows
    iterator = df.toLocalIterator
    assert(iterator.hasNext)
    assert(iterator.next() == array(0))
    assert(iterator.hasNext)
    assert(iterator.next() == array(1))
    assert(iterator.hasNext)
    assert(iterator.next() == array(2))
    assert(!iterator.hasNext)
    iterator.asInstanceOf[Closeable].close()
    assert(!iterator.hasNext)
  }

  test("DataFrame.show() with new line") {
    val df = Seq("line1\nline1.1\n", "line2", "\n", "line4", "\n\n", null).toDF("a")
    assert(
      getShowString(df, 10) ==
        """-----------
          ||"A"      |
          |-----------
          ||line1    |
          ||line1.1  |
          ||         |
          ||line2    |
          ||         |
          ||         |
          ||line4    |
          ||         |
          ||         |
          ||         |
          ||NULL     |
          |-----------
          |""".stripMargin)

    val df2 =
      Seq(("line1\nline1.1\n", 1), ("line2", 2), ("\n", 3), ("line4", 4), ("\n\n", 5), (null, 6))
        .toDF("a", "b")
    assert(
      getShowString(df2, 10) ==
        """-----------------
          ||"A"      |"B"  |
          |-----------------
          ||line1    |1    |
          ||line1.1  |     |
          ||         |     |
          ||line2    |2    |
          ||         |3    |
          ||         |     |
          ||line4    |4    |
          ||         |5    |
          ||         |     |
          ||         |     |
          ||NULL     |6    |
          |-----------------
          |""".stripMargin)
  }

  test("negative test to input invalid table name for saveAsTable()") {
    val df = Seq((1, null), (2, "NotNull"), (3, null)).toDF("a", "b")
    val ex = intercept[SnowparkClientException] {
      df.write.saveAsTable("negative test invalid table name")
    }
    assert(ex.getMessage.matches(".*The object name .* is invalid."))
  }

  test("negative test to input invalid view name for createOrReplaceView()") {
    val df = Seq((1, null), (2, "NotNull"), (3, null)).toDF("a", "b")
    val ex = intercept[SnowparkClientException] {
      df.createOrReplaceView("negative test invalid table name")
    }
    assert(ex.getMessage.matches(".*The object name .* is invalid."))
  }

  test("toDF with array schema") {
    val df = session.createDataFrame(Seq((1, "a"))).toDF(Array("a", "b"))
    val schema = df.schema
    assert(schema.length == 2)
    assert(schema.head.name == "A")
    assert(schema(1).name == "B")
  }

  test("sort with array arg") {
    val df = Seq((1, 1, 1), (2, 0, 4), (1, 2, 3)).toDF("col1", "col2", "col3")
    val dfSorted = df.sort(Array(col("col1").asc, col("col2").desc, col("col3")))
    checkAnswer(dfSorted, Array(Row(1, 2, 3), Row(1, 1, 1), Row(2, 0, 4)), sort = false)
  }

  test("select with array args") {
    val df = Seq((1, 2)).toDF("col1", "col2")
    val dfSelected = df.select(Array(df.col("col1"), lit("abc"), df.col("col1") + df.col("col2")))
    checkAnswer(dfSelected, Array(Row(1, "abc", 3)))
  }

  test("select(String) with array args") {
    val df = Seq((1, 2, 3)).toDF("col1", "col2", "col3")
    val dfSelected = df.select(Array("col1", "col2"))
    checkAnswer(dfSelected, Array(Row(1, 2)))
  }

  test("drop(String) with array args") {
    val df = Seq((1, 2, 3)).toDF("col1", "col2", "col3")
    checkAnswer(df.drop(Array("col3")), Array(Row(1, 2)))
  }

  test("drop with array args") {
    val df = Seq((1, 2, 3)).toDF("col1", "col2", "col3")
    checkAnswer(df.drop(Array(df("col3"))), Array(Row(1, 2)))
  }

  test("agg with array args") {
    val df = Seq((1, 2), (4, 5)).toDF("col1", "col2")
    checkAnswer(df.agg(Array(max($"col1"), mean($"col2"))), Array(Row(4, 3.5)))
  }

  test("rollup with array args") {
    val df = Seq(
      ("country A", "state A", 50),
      ("country A", "state A", 50),
      ("country A", "state B", 5),
      ("country A", "state B", 5),
      ("country B", "state A", 100),
      ("country B", "state A", 100),
      ("country B", "state B", 10),
      ("country B", "state B", 10))
      .toDF("country", "state", "value")

    val expectedResult = Seq(
      Row(null, null, 330),
      Row("country A", null, 110),
      Row("country A", "state A", 100),
      Row("country A", "state B", 10),
      Row("country B", null, 220),
      Row("country B", "state A", 200),
      Row("country B", "state B", 20))

    checkAnswer(
      df.rollup(Array($"country", $"state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      sort = false)
  }
  test("rollup(String) with array args") {
    val df = Seq(
      ("country A", "state A", 50),
      ("country A", "state A", 50),
      ("country A", "state B", 5),
      ("country A", "state B", 5),
      ("country B", "state A", 100),
      ("country B", "state A", 100),
      ("country B", "state B", 10),
      ("country B", "state B", 10))
      .toDF("country", "state", "value")

    val expectedResult = Seq(
      Row(null, null, 330),
      Row("country A", null, 110),
      Row("country A", "state A", 100),
      Row("country A", "state B", 10),
      Row("country B", null, 220),
      Row("country B", "state A", 200),
      Row("country B", "state B", 20))

    checkAnswer(
      df.rollup(Array("country", "state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
      expectedResult,
      sort = false)
  }

  test("groupBy with array args") {
    val df = Seq(
      ("country A", "state A", 50),
      ("country A", "state A", 50),
      ("country A", "state B", 5),
      ("country A", "state B", 5),
      ("country B", "state A", 100),
      ("country B", "state A", 100),
      ("country B", "state B", 10),
      ("country B", "state B", 10))
      .toDF("country", "state", "value")

    val expectedResult = Seq(
      Row("country A", "state A", 100),
      Row("country A", "state B", 10),
      Row("country B", "state A", 200),
      Row("country B", "state B", 20))

    checkAnswer(
      df.groupBy(Array($"country", $"state"))
        .agg(sum(col("value"))),
      expectedResult)
  }

  test("groupBy(String) with array args") {
    val df = Seq(
      ("country A", "state A", 50),
      ("country A", "state A", 50),
      ("country A", "state B", 5),
      ("country A", "state B", 5),
      ("country B", "state A", 100),
      ("country B", "state A", 100),
      ("country B", "state B", 10),
      ("country B", "state B", 10))
      .toDF("country", "state", "value")

    val expectedResult = Seq(
      Row("country A", "state A", 100),
      Row("country A", "state B", 10),
      Row("country B", "state A", 200),
      Row("country B", "state B", 20))

    checkAnswer(
      df.groupBy(Array("country", "state"))
        .agg(sum(col("value"))),
      expectedResult)
  }

  test("test rename: basic") {
    val df = Seq((1, 2)).toDF("a", "b")
    df.schema.printTreeString()

    // rename column 'b as 'b1
    val df2 = df.rename("b1", col("b"))
    df2.schema.printTreeString()
    assert(
      TestUtils.treeString(df2.schema, 0) ==
        s"""root
           | |--A: Long (nullable = false)
           | |--B1: Long (nullable = false)
           |""".stripMargin)
    checkAnswer(df2, Seq(Row(1, 2)))

    // rename column 'a as 'a1
    val df3 = df2.rename("a1", df("a"))
    df3.schema.printTreeString()
    assert(
      TestUtils.treeString(df3.schema, 0) ==
        s"""root
           | |--A1: Long (nullable = false)
           | |--B1: Long (nullable = false)
           |""".stripMargin)
    checkAnswer(df2, Seq(Row(1, 2)))
  }

  test("test rename: joined DataFrame") {
    val dfLeft = Seq((1, 2)).toDF("a", "b")
    val dfRight = Seq((3, 4)).toDF("a", "c")
    val dfJoin = dfLeft.join(dfRight)

    // rename left df columns including ambiguous columns
    val df1 = dfJoin.rename("left_a", dfLeft("a")).rename("left_b", dfLeft("b"))
    assert(df1.schema.head.name.equals("LEFT_A") && df1.schema(1).name.equals("LEFT_B"))
    checkAnswer(df1, Seq(Row(1, 2, 3, 4)))

    // rename left df columns including ambiguous columns
    val df2 = df1.rename("right_a", dfRight("a")).rename("right_c", dfRight("c"))
    assert(
      TestUtils.treeString(df2.schema, 0) ==
        s"""root
           | |--LEFT_A: Long (nullable = false)
           | |--LEFT_B: Long (nullable = false)
           | |--RIGHT_A: Long (nullable = false)
           | |--RIGHT_C: Long (nullable = false)
           |""".stripMargin)
    checkAnswer(df2, Seq(Row(1, 2, 3, 4)))

    // Get columns for right DF's columns
    val df3 = df2.select(dfRight("a"), dfRight("c"))
    df3.schema.printTreeString()
    assert(
      TestUtils.treeString(df3.schema, 0) ==
        s"""root
           | |--RIGHT_A: Long (nullable = false)
           | |--RIGHT_C: Long (nullable = false)
           |""".stripMargin)
    checkAnswer(df3, Seq(Row(3, 4)))
  }

  test("test rename: toDF() and joined DataFrame") {
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((1, 2)).toDF("a", "b")
    val df3 = df1.toDF("a1", "b1")
    val df4 = df3.join(df2)
    val df5 = df4.rename("a2", df1("a"))
    df5.schema.printTreeString()
    assert(
      TestUtils.treeString(df5.schema, 0) ==
        s"""root
           | |--A2: Long (nullable = false)
           | |--B1: Long (nullable = false)
           | |--A: Long (nullable = false)
           | |--B: Long (nullable = false)
           |""".stripMargin)
    checkAnswer(df5, Seq(Row(1, 2, 1, 2)))
  }

  test("test rename: negative test") {
    val df = Seq((1, 2)).toDF("a", "b")

    // rename un-qualified column
    val ex1 = intercept[SnowparkClientException] {
      df.rename("c", lit("c"))
    }
    assert(ex1.errorCode.equals("0120") &&
      ex1.message.contains(
        "Unable to rename the column Column[Literal(c,Some(String))] as \"C\"" +
          " because this DataFrame doesn't have a column named Column[Literal(c,Some(String))]."))

    // rename un-exist column
    val ex2 = intercept[SnowparkClientException] {
      df.rename("c", col("not_exist_column"))
    }
    assert(
      ex2.errorCode.equals("0120") &&
        ex2.message.contains("Unable to rename the column \"NOT_EXIST_COLUMN\" as \"C\"" +
          " because this DataFrame doesn't have a column named \"NOT_EXIST_COLUMN\"."))

    // rename a column has 3 duplicate names in the DataFrame
    val df2 = session.sql("select 1 as A, 2 as A, 3 as A")
    val ex3 = intercept[SnowparkClientException] {
      df2.rename("c", col("a"))
    }
    assert(
      ex3.errorCode.equals("0121") &&
        ex3.message.contains("Unable to rename the column \"A\" as \"C\" because" +
          " this DataFrame has 3 columns named \"A\""))
  }

  test("with columns keep order", JavaStoredProcExclude) {
    val data = new Variant(
      Map("STARTTIME" -> 0, "ENDTIME" -> 10000, "START_STATION_ID" -> 2, "END_STATION_ID" -> 3))
    val df = Seq((1, data)).toDF("TRIPID", "V")

    val result = df.withColumns(
      Seq("starttime", "endtime", "duration", "start_station_id", "end_station_id"),
      Seq(
        to_timestamp(get(col("V"), lit("STARTTIME"))),
        to_timestamp(get(col("V"), lit("ENDTIME"))),
        datediff("minute", col("STARTTIME"), col("ENDTIME")),
        as_integer(get(col("V"), lit("START_STATION_ID"))),
        as_integer(get(col("V"), lit("END_STATION_ID")))))

    checkAnswer(
      result,
      Seq(
        Row(
          1,
          "{\n  \"ENDTIME\": 10000,\n  \"END_STATION_ID\": 3," +
            "\n  \"STARTTIME\": 0,\n  \"START_STATION_ID\": 2\n}",
          Timestamp.valueOf("1969-12-31 16:00:00.0"),
          Timestamp.valueOf("1969-12-31 18:46:40.0"),
          166,
          2,
          3)))
  }

  test("withColumns input doesn't match each other") {
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    val msg = intercept[SnowparkClientException](df.withColumns(Seq("e", "f"), Seq(lit(1))))
    assert(
      msg.message.contains(
        "The number of column names (2) does not match the number of values (1)."))
  }

  test("withColumns replace exiting") {
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    val replaced = df.withColumns(Seq("b", "d"), Seq(lit(5), lit(6)))
    checkAnswer(replaced, Seq(Row(1, 3, 5, 6)))

    val msg = intercept[SnowparkClientException](
      df.withColumns(Seq("d", "b", "d"), Seq(lit(4), lit(5), lit(6))))
    assert(
      msg.message.contains(
        "The same column name is used multiple times in the colNames parameter."))

    val msg1 = intercept[SnowparkClientException](
      df.withColumns(Seq("d", "b", "D"), Seq(lit(4), lit(5), lit(6))))
    assert(
      msg1.message.contains(
        "The same column name is used multiple times in the colNames parameter."))
  }

  test("dropDuplicates") {
    val df = Seq((1, 1, 1, 1), (1, 1, 1, 2), (1, 1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4))
      .toDF("a", "b", "c", "d")
    checkAnswer(
      df.dropDuplicates(),
      Seq(Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)))

    val result1 = df.dropDuplicates("a")
    assert(result1.count() == 1)
    val row1 = result1.collect().head
    // result is non-deterministic.
    (row1.getInt(0), row1.getInt(1), row1.getInt(2), row1.getInt(3)) match {
      case (1, 1, 1, 1) =>
      case (1, 1, 1, 2) =>
      case (1, 1, 2, 3) =>
      case (1, 2, 3, 4) =>
      case _ => throw new Exception("wrong result")
    }

    val result2 = df.dropDuplicates("a", "b")
    assert(result2.count() == 2)
    checkAnswer(result2.where(col("b") === lit(2)), Seq(Row(1, 2, 3, 4)))
    val row2 = result2.where(col("b") === lit(1)).collect().head
    // result is non-deterministic.
    (row2.getInt(0), row2.getInt(1), row2.getInt(2), row2.getInt(3)) match {
      case (1, 1, 1, 1) =>
      case (1, 1, 1, 2) =>
      case (1, 1, 2, 3) =>
      case _ => throw new Exception("wrong result")
    }

    val result3 = df.dropDuplicates("a", "b", "c")
    assert(result3.count() == 3)
    checkAnswer(result3.where(col("c") === lit(2)), Seq(Row(1, 1, 2, 3)))
    checkAnswer(result3.where(col("c") === lit(3)), Seq(Row(1, 2, 3, 4)))
    val row3 = result3.where(col("c") === lit(1)).collect().head
    // result is non-deterministic.
    (row3.getInt(0), row3.getInt(1), row3.getInt(2), row3.getInt(3)) match {
      case (1, 1, 1, 1) =>
      case (1, 1, 1, 2) =>
      case _ => throw new Exception("wrong result")
    }

    checkAnswer(
      df.dropDuplicates("a", "b", "c", "d"),
      Seq(Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)))

    checkAnswer(
      df.dropDuplicates("a", "b", "c", "d", "d"),
      Seq(Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)))

    // column doesn't exist
    assertThrows[SnowparkClientException](df.dropDuplicates("e").collect())
  }

  test("consecutively dropDuplicates") {
    val df = Seq((1, 1, 1, 1), (1, 1, 1, 2), (1, 1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4))
      .toDF("a", "b", "c", "d")
    val df1 = df
      .dropDuplicates()
      .dropDuplicates("a", "b", "c")
      .dropDuplicates("a", "b")
      .dropDuplicates("a")

    assert(df1.count() == 1)
    val row1 = df1.collect().head
    // result is non-deterministic.
    (row1.getInt(0), row1.getInt(1), row1.getInt(2), row1.getInt(3)) match {
      case (1, 1, 1, 1) =>
      case (1, 1, 1, 2) =>
      case (1, 1, 2, 3) =>
      case (1, 2, 3, 4) =>
      case _ => throw new Exception("wrong result")
    }
  }

  test("negative test to handle JDBC exceptions") {
    val sourceEdges = session
      .createDataFrame(
        Seq(
          (2, 1, 35, "red", 99),
          (7, 2, 24, "red", 99),
          (7, 9, 77, "green", 99),
          (8, 5, 11, "green", 99),
          (8, 4, 14, "blue", 99),
          (8, 3, 21, "red", 99),
          (9, 9, 12, "orange", 99)))
      .toDF("v1", "v2", "length", "color", "unused")

    // Wrapped JDBC exception
    val ex = intercept[SnowparkClientException] {
      sourceEdges.select(col("v2") - col("v1").as("diff)")).show()
    }
    assert(ex.errorCode.equals("0308"))
    val jdbcEx = ex.getCause.asInstanceOf[SnowflakeSQLException]
    assert(jdbcEx.getMessage.contains("SQL compilation error:"))

    // Un-wrapped the JDBC exception
    val ex2 = intercept[SnowflakeSQLException] {
      session.sql("select negative test query").show()
    }
    assert(ex2.getMessage.contains("SQL compilation error:"))
  }

  test("negative test for DataFrame.col(NOT_EXIST_COL)") {
    val df = Seq((1, 2)).toDF("a", "b")
    val ex = intercept[SnowparkClientException] {
      df.select(df.col("NOT_EXIST_COL"))
    }
    assert(ex.errorCode.equals("0108"))
    assert(
      ex.message.contains("The DataFrame does not contain the column named" +
        " 'NOT_EXIST_COL' and the valid names are \"A\", \"B\""))
  }
}

class EagerDataFrameSuite extends DataFrameSuite with EagerSession {
  test("eager analysis") {
    // reports errors
    assertThrows[SnowflakeSQLException](
      session.sql("select something").select("111").filter(col("+++") > "aaa"))

  }
}

class LazyDataFrameSuite extends DataFrameSuite with LazySession {
  test("lazy analysis") {
    // no error reported
    val df = session.sql("select something").select("111").filter(col("+++") > "aaa")

    // reports error when executing
    assertThrows[SnowflakeSQLException](df.schema)
  }
}
