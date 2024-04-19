package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.scalatest.Matchers.the

import java.sql.ResultSet

class DataFrameAggregateSuite extends TestData {
  import session.implicits._
  val absTol = 1e-8
  test("pivot") {
    checkAnswer(
      monthlySales
        .pivot("month", Seq("JAN", "FEB", "MAR", "APR"))
        .agg(sum(col("amount")))
        .sort(col("empid")),
      Seq(Row(1, 10400, 8000, 11000, 18000), Row(2, 39500, 90700, 12000, 5300)),
      sort = false)

    // multiple aggregation isn't supported
    val e = intercept[SnowparkClientException](
      monthlySales
        .pivot("month", Seq("JAN", "FEB", "MAR", "APR"))
        .agg(sum(col("amount")), avg(col("amount")))
        .sort(col("empid"))
        .collect())
    assert(
      e.getMessage.contains("You can apply only one aggregate expression to a" +
        " RelationalGroupedDataFrame returned by the pivot() method."))
  }

  test("join on pivot") {
    val df1 = monthlySales
      .pivot("month", Seq("JAN", "FEB", "MAR", "APR"))
      .agg(sum(col("amount")))
      .sort(col("empid"))

    val df2 = Seq((1, 12345), (2, 67890)).toDF("empid", "may")

    checkAnswer(
      df1.join(df2, "empid"),
      Seq(Row(1, 10400, 8000, 11000, 18000, 12345), Row(2, 39500, 90700, 12000, 5300, 67890)),
      sort = false)
  }

  test("pivot on join") {
    val df = Seq((1, "One"), (2, "Two")).toDF("empid", "name")
    checkAnswer(
      monthlySales
        .join(df, "empid")
        .pivot("month", Seq("JAN", "FEB", "MAR", "APR"))
        .agg(sum(col("amount")))
        .sort(col("name")),
      Seq(Row(1, "One", 10400, 8000, 11000, 18000), Row(2, "Two", 39500, 90700, 12000, 5300)),
      sort = false)
  }

  test("RelationalGroupedDataFrame.agg()") {
    val df = Seq((1, "One"), (2, "Two"), (3, "Three")).toDF("empid", "name").groupBy()

    // Agg() on 1 column
    checkAnswer(df.agg(max(col("empid"))), Seq(Row(3)))
    checkAnswer(df.agg(Seq(min(col("empid")))), Seq(Row(1)))
    checkAnswer(df.agg(Array(min(col("empid")))), Seq(Row(1)))
    checkAnswer(df.agg(col("empid") -> "max"), Seq(Row(3)))
    checkAnswer(df.agg(Seq(col("empid") -> "avg")), Seq(Row(2.0)))

    // Agg() on 2 columns
    checkAnswer(df.agg(max(col("empid")), max(col("name"))), Seq(Row(3, "Two")))
    checkAnswer(df.agg(Seq(min(col("empid")), min(col("name")))), Seq(Row(1, "One")))
    checkAnswer(df.agg(col("empid") -> "max", col("name") -> "max"), Seq(Row(3, "Two")))
    checkAnswer(df.agg(Seq(col("empid") -> "min", col("name") -> "min")), Seq(Row(1, "One")))
  }

  test("Group by grouping set") {
    // GROUP BY GROUPING SETS((a),(b)) is equivalent to GROUP BY a UNION ALL GROUP BY b.
    // GROUP BY a UNION ALL GROUP BY b.
    val result = nurse
      .groupBy("medical_license")
      .agg(count(col("*")).as("count"))
      .withColumn("radio_license", lit(null))
      .select("medical_license", "radio_license", "count")
      .unionAll(
        nurse
          .groupBy("radio_license")
          .agg(count(col("*")).as("count"))
          .withColumn("medical_license", lit(null))
          .select("medical_license", "radio_license", "count"))
      .sort(col("count"))
      .collect()

    // GROUP BY GROUPING SETS((a),(b))
    val groupingSets = nurse
      .groupByGroupingSets(GroupingSets(Set(col("medical_license")), Set(col("radio_license"))))
      .agg(count(col("*")).as("count"))
      .sort(col("count"))

    checkAnswer(groupingSets, result, sort = false)

    checkAnswer(
      groupingSets,
      Seq(
        Row(null, "General", 1),
        Row(null, "Amateur Extra", 1),
        Row("RN", null, 2),
        Row(null, "Technician", 2),
        Row(null, null, 3),
        Row("LVN", null, 5)),
      sort = false)

    // comparing with groupBy
    checkAnswer(
      nurse
        .groupBy("medical_license", "radio_license")
        .agg(count(col("*")).as("count"))
        .sort(col("count"))
        .select("count", "medical_license", "radio_license"),
      Seq(
        Row(1, "LVN", "General"),
        Row(1, "RN", "Amateur Extra"),
        Row(1, "RN", null),
        Row(2, "LVN", "Technician"),
        Row(2, "LVN", null)),
      sort = false)

    // mixed grouping expression
    checkAnswer(
      nurse
        .groupByGroupingSets(
          GroupingSets(Set(col("medical_license"), col("radio_license"))),
          GroupingSets(Set(col("radio_license"))))
        // duplicated column should be removed in the result
        .agg(col("radio_license"))
        .sort(col("radio_license")),
      Seq(
        Row("LVN", null),
        Row("RN", null),
        Row("RN", "Amateur Extra"),
        Row("LVN", "General"),
        Row("LVN", "Technician")),
      sort = false)

    // default constructor
    checkAnswer(
      nurse
        .groupByGroupingSets(
          Seq(
            GroupingSets(Seq(Set(col("medical_license"), col("radio_license")))),
            GroupingSets(Seq(Set(col("radio_license"))))))
        // duplicated column should be removed in the result
        .agg(col("radio_license"))
        .sort(col("radio_license")),
      Seq(
        Row("LVN", null),
        Row("RN", null),
        Row("RN", "Amateur Extra"),
        Row("LVN", "General"),
        Row("LVN", "Technician")),
      sort = false)
  }

  test("RelationalGroupedDataFrame.max()") {
    val df1 = Seq(("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e"))
      .toDF("key", "value1", "value2", "rest")

    // below 3 ways to call max() must return the same result.
    val maxResult = Seq(Row("a", 3, 33), Row("b", 4, 44))
    checkAnswer(df1.groupBy("key").max(col("value1"), col("value2")), maxResult)
    checkAnswer(df1.groupBy("key").agg(max(col("value1")), max(col("value2"))), maxResult)
  }

  test("RelationalGroupedDataFrame.any_value()") {
    val df1 = Seq(("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e"))
      .toDF("key", "value1", "value2", "rest")

    // below 2 ways to call any_value()
    val (values_1_A, values_1_B) = (Seq(1, 3), Seq(2, 4))
    val (values_2_A, values_2_B) = (Seq(11, 33), Seq(22, 44))
    var rows = df1.groupBy("key").any_value(col("value1"), col("value2")).collect()
    assert(rows.length == 2)
    rows.foreach { row =>
      if (row.getString(0).equals("a")) {
        assert(values_1_A.contains(row.getInt(1)) && values_2_A.contains(row.getInt(2)))
      } else {
        assert(values_1_B.contains(row.getInt(1)) && values_2_B.contains(row.getInt(2)))
      }
    }
    rows = df1.groupBy("key").agg(any_value(col("value1")), any_value(col("value2"))).collect()
    rows.foreach { row =>
      if (row.getString(0).equals("a")) {
        assert(values_1_A.contains(row.getInt(1)) && values_2_A.contains(row.getInt(2)))
      } else {
        assert(values_1_B.contains(row.getInt(1)) && values_2_B.contains(row.getInt(2)))
      }
    }
  }

  test("RelationalGroupedDataFrame.avg()/mean()") {
    val df1 = Seq(("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e"))
      .toDF("key", "value1", "value2", "rest")

    val avgResult = Seq(Row("a", 2.0, 22.0), Row("b", 3.0, 33.0))
    // below 3 ways to call avg() must return the same result.
    checkAnswer(df1.groupBy("key").avg(col("value1"), col("value2")), avgResult)
    checkAnswer(df1.groupBy("key").agg(avg(col("value1")), avg(col("value2"))), avgResult)
    // mean() is alias of avg()
    checkAnswer(df1.groupBy("key").mean(col("value1"), col("value2")), avgResult)
    checkAnswer(df1.groupBy("key").agg(mean(col("value1")), mean(col("value2"))), avgResult)
  }

  test("RelationalGroupedDataFrame.median()") {
    val df1 = Seq(
      ("a", 1, 11, "b"),
      ("b", 2, 22, "c"),
      ("a", 3, 33, "d"),
      ("b", 4, 44, "e"),
      ("b", 44, 444, "f"))
      .toDF("key", "value1", "value2", "rest")

    // call median without group-by
    checkAnswer(df1.select(median(col("value1")), median(col("value2"))), Seq(Row(3.0, 33.0)))

    // below 3 ways to call median() must return the same result.
    val medianResult = Seq(Row("a", 2.0, 22.0), Row("b", 4.0, 44.0))
    checkAnswer(df1.groupBy("key").median(col("value1"), col("value2")), medianResult)
    checkAnswer(
      df1.groupBy("key").agg(median(col("value1")), median(col("value2"))),
      medianResult)
  }

  test("builtin functions") {
    val df = Seq((1, 11), (2, 12), (1, 13)).toDF("a", "b")

    // no arguments
    checkAnswer(
      df.groupBy("a").builtin("max")(col("a"), col("b")),
      Seq(Row(1, 1, 13), Row(2, 2, 12)))
    // with arguments
    checkAnswer(df.groupBy("a").builtin("max")(col("b")), Seq(Row(1, 13), Row(2, 12)))
  }

  test("non empty argument functions") {
    var msg = intercept[SnowparkClientException](integer1.groupBy("a").avg())
    assert(msg.getMessage.contains("You must pass a Seq of one or more Columns to function"))

    msg = intercept[SnowparkClientException](integer1.groupBy("a").sum())
    assert(msg.getMessage.contains("You must pass a Seq of one or more Columns to function"))

    msg = intercept[SnowparkClientException](integer1.groupBy("a").median())
    assert(msg.getMessage.contains("You must pass a Seq of one or more Columns to function"))

    msg = intercept[SnowparkClientException](integer1.groupBy("a").min())
    assert(msg.getMessage.contains("You must pass a Seq of one or more Columns to function"))

    msg = intercept[SnowparkClientException](integer1.groupBy("a").max())
    assert(msg.getMessage.contains("You must pass a Seq of one or more Columns to function"))

    msg = intercept[SnowparkClientException](integer1.groupBy("a").any_value())
    assert(msg.getMessage.contains("You must pass a Seq of one or more Columns to function"))
  }

  test("rollup overlapping columns") {
    checkAnswer(
      testData2.rollup($"a" + $"b" as "foo", $"b" as "bar").agg(sum($"a" - $"b") as "foo"),
      Row(2, 1, 0) :: Row(2, null, 0) :: Row(3, 1, 1) :: Row(3, 2, -1) :: Row(3, null, 0)
        :: Row(4, 1, 2) :: Row(4, 2, 0) :: Row(4, null, 2) :: Row(5, 2, 1)
        :: Row(5, null, 1) :: Row(null, null, 3) :: Nil)

    checkAnswer(
      testData2.rollup("a", "b").agg(sum(col("b"))),
      Row(1, 1, 1) :: Row(1, 2, 2) :: Row(1, null, 3) :: Row(2, 1, 1) :: Row(2, 2, 2)
        :: Row(2, null, 3) :: Row(3, 1, 1) :: Row(3, 2, 2) :: Row(3, null, 3)
        :: Row(null, null, 9) :: Nil)
  }

  test("cube overlapping columns") {
    checkAnswer(
      testData2.cube($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      Row(2, 1, 0) :: Row(2, null, 0) :: Row(3, 1, 1) :: Row(3, 2, -1) :: Row(3, null, 0)
        :: Row(4, 1, 2) :: Row(4, 2, 0) :: Row(4, null, 2) :: Row(5, 2, 1)
        :: Row(5, null, 1) :: Row(null, 1, 3) :: Row(null, 2, 0)
        :: Row(null, null, 3) :: Nil)

    checkAnswer(
      testData2.cube("a", "b").agg(sum(col("b"))),
      Row(1, 1, 1) :: Row(1, 2, 2) :: Row(1, null, 3) :: Row(2, 1, 1) :: Row(2, 2, 2)
        :: Row(2, null, 3) :: Row(3, 1, 1) :: Row(3, 2, 2) :: Row(3, null, 3)
        :: Row(null, 1, 3) :: Row(null, 2, 6) :: Row(null, null, 9) :: Nil)
  }

  test("null count") {
    checkAnswer(testData3.groupBy($"a").agg(count($"b")), Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(testData3.groupBy($"a").agg(count($"a" + $"b")), Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(
      testData3
        .agg(count($"a"), count($"b"), count(lit(1)), count_distinct($"a"), count_distinct($"b")),
      Seq(Row(2, 1, 2, 2, 1)))

    checkAnswer(
      testData3.agg(count($"b"), count_distinct($"b"), sum_distinct($"b")), // non-partial
      Seq(Row(1, 1, 2)))
  }

  // Used temporary VIEW which is not supported by owner's mode stored proc yet
  test("Window functions inside aggregate functions", JavaStoredProcExcludeOwner) {
    def checkWindowError(df: => DataFrame): Unit = {
      the[SnowflakeSQLException] thrownBy {
        df.collect()
      }
    }
    checkWindowError(testData2.select(min(avg($"b").over(Window.partitionBy($"a")))))
    checkWindowError(testData2.agg(sum($"b"), max(rank().over(Window.orderBy($"a")))))
    checkWindowError(
      testData2.groupBy($"a").agg(sum($"b"), max(rank().over(Window.orderBy($"b")))))
    checkWindowError(
      testData2
        .groupBy($"a")
        .agg(max(sum(sum($"b")).over(Window.orderBy($"a")))))
    checkWindowError(
      testData2
        .groupBy($"a")
        .agg(
          sum($"b").as("s"),
          max(count(col("*"))
            .over()))
        .where($"s" === 3))
    checkAnswer(
      testData2
        .groupBy($"a")
        .agg(max($"b"), sum($"b").as("s"), count(col("*")).over())
        .where($"s" === 3),
      Row(1, 2, 3, 3) :: Row(2, 2, 3, 3) :: Row(3, 2, 3, 3) :: Nil)

    testData2.createOrReplaceTempView("testData2")
    checkWindowError(session.sql("SELECT MIN(AVG(b) OVER(PARTITION BY a)) FROM testData2"))
    checkWindowError(session.sql("SELECT SUM(b), MAX(RANK() OVER(ORDER BY a)) FROM testData2"))
    checkWindowError(
      session.sql("SELECT SUM(b), MAX(RANK() OVER(ORDER BY b)) FROM testData2 GROUP BY a"))
    checkWindowError(
      session.sql("SELECT MAX(SUM(SUM(b)) OVER(ORDER BY a)) FROM testData2 GROUP BY a"))
    checkWindowError(
      session.sql(
        "SELECT MAX(RANK() OVER(ORDER BY b)) FROM testData2 GROUP BY a HAVING SUM(b) = 3"))
    checkAnswer(
      session.sql(
        "SELECT a, MAX(b), RANK() OVER(ORDER BY a) FROM testData2 GROUP BY a HAVING SUM(b) = 3"),
      Row(1, 2, 1) :: Row(2, 2, 2) :: Row(3, 2, 3) :: Nil)
  }

  test("distinct") {
    val df = Seq((1, "one", 1.0), (2, "one", 2.0), (2, "two", 1.0)).toDF("i", "s", """"i"""")
    checkAnswer(
      df.distinct(),
      Row(1, "one", 1.0) :: Row(2, "one", 2.0) :: Row(2, "two", 1.0) :: Nil)
    checkAnswer(df.select("i").distinct(), Row(1) :: Row(2) :: Nil)
    checkAnswer(df.select(""""i"""").distinct(), Row(1.0) :: Row(2.0) :: Nil)
    checkAnswer(df.select("s").distinct(), Row("one") :: Row("two") :: Nil)
    checkAnswer(
      df.select("i", """"i"""").distinct(),
      Row(1, 1.0) :: Row(2, 1.0) :: Row(2, 2.0) :: Nil)
    checkAnswer(
      df.select("i", """"i"""").distinct(),
      Row(1, 1.0) :: Row(2, 1.0) :: Row(2, 2.0) :: Nil)
    checkAnswer(df.filter($"i" < 0).distinct(), Nil)
  }

  test("distinct and joins") {
    val lhs = Seq((1, "one", 1.0), (2, "one", 2.0)).toDF("i", "s", """"i"""")
    val rhs = Seq((1, "one", 1.0), (2, "one", 2.0)).toDF("i", "s", """"i"""")

    checkAnswer(
      lhs.join(rhs, lhs("i") === rhs("i")).distinct(),
      Row(1, "one", 1.0, 1, "one", 1.0) :: Row(2, "one", 2.0, 2, "one", 2.0) :: Nil)
    val lhsD = lhs.select($"s").distinct()
    checkAnswer(
      lhsD.join(rhs, lhsD("s") === rhs("s")),
      Row("one", 1, "one", 1.0) :: Row("one", 2, "one", 2.0) :: Nil)

    var rhsD = rhs.select($"s")
    checkAnswer(
      lhsD.join(rhsD, lhsD("s") === rhsD("s")),
      Row("one", "one") :: Row("one", "one") :: Nil)

    rhsD = rhs.select($"s").distinct()
    checkAnswer(lhsD.join(rhsD, lhsD("s") === rhsD("s")), Row("one", "one") :: Nil)
  }
  test("SN - groupBy") {
    checkAnswer(testData2.groupBy("a").agg(sum($"b")), Seq(Row(1, 3), Row(2, 3), Row(3, 3)))
    checkAnswer(testData2.groupBy("a").agg(sum($"b").as("totB")).agg(sum($"totB")), Seq(Row(9)))
    checkAnswer(
      testData2.groupBy("a").agg(count($"*")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil)
    checkAnswer(
      testData2.groupBy("a").agg(Map($"*" -> "count")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil)
    checkAnswer(
      testData2.groupBy("a").agg(Map($"b" -> "sum")),
      Row(1, 3) :: Row(2, 3) :: Row(3, 3) :: Nil)

    val df1 = Seq(("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d"))
      .toDF("key", "value1", "value2", "rest")

    checkAnswer(df1.groupBy("key").min($"value2"), Seq(Row("a", 0), Row("b", 4)))

    checkAnswer(
      decimalData.groupBy("a").agg(sum($"b")),
      Seq(
        Row(new java.math.BigDecimal(1), new java.math.BigDecimal(3)),
        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(3)),
        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3))))
  }

  test("SN - agg should be ordering preserving") {
    val df = session.range(2)
    val ret = df.groupBy("id").agg($"id" -> "sum", $"id" -> "count", $"id" -> "min")
    assert(ret.schema.map(_.name) == Seq("ID", "\"SUM(ID)\"", "\"COUNT(ID)\"", "\"MIN(ID)\""))
    checkAnswer(ret, Row(0, 0, 1, 0) :: Row(1, 1, 1, 1) :: Nil)
  }

  test("SN - cube") {
    checkAnswer(
      courseSales.cube("course", "year").sum($"earnings"),
      Row("Java", 2012, 20000.0) ::
        Row("Java", 2013, 30000.0) ::
        Row("Java", null, 50000.0) ::
        Row("dotNET", 2012, 15000.0) ::
        Row("dotNET", 2013, 48000.0) ::
        Row("dotNET", null, 63000.0) ::
        Row(null, 2012, 35000.0) ::
        Row(null, 2013, 78000.0) ::
        Row(null, null, 113000.0) :: Nil)

    val df0 = session.createDataFrame(
      Seq(
        Fact(20151123, 18, 35, "room1", 18.6),
        Fact(20151123, 18, 35, "room2", 22.4),
        Fact(20151123, 18, 36, "room1", 17.4),
        Fact(20151123, 18, 36, "room2", 25.6)))

    val cube0 = df0.cube("date", "hour", "minute", "room_name").agg(Map($"temp" -> "avg"))
    assert(cube0.where(col("date").is_null).count > 0)
  }

  test("SN - grouping and grouping_id") {
    checkAnswer(
      courseSales
        .cube("course", "year")
        .agg(grouping($"course"), grouping($"year"), grouping_id($"course", $"year")),
      Row("Java", 2012, 0, 0, 0) ::
        Row("Java", 2013, 0, 0, 0) ::
        Row("Java", null, 0, 1, 1) ::
        Row("dotNET", 2012, 0, 0, 0) ::
        Row("dotNET", 2013, 0, 0, 0) ::
        Row("dotNET", null, 0, 1, 1) ::
        Row(null, 2012, 1, 0, 2) ::
        Row(null, 2013, 1, 0, 2) ::
        Row(null, null, 1, 1, 3) :: Nil)

    // use column reference in `grouping_id` instead of column name
    checkAnswer(
      courseSales
        .cube("course", "year")
        .agg(grouping_id(courseSales("course"), courseSales("year"))),
      Row("Java", 2012, 0) ::
        Row("Java", 2013, 0) ::
        Row("Java", null, 1) ::
        Row("dotNET", 2012, 0) ::
        Row("dotNET", 2013, 0) ::
        Row("dotNET", null, 1) ::
        Row(null, 2012, 2) ::
        Row(null, 2013, 2) ::
        Row(null, null, 3) :: Nil)

    /* TODO: Add another test with eager analysis
     */
    intercept[SnowflakeSQLException] {
      courseSales.groupBy().agg(grouping($"course")).collect()
    }
    /*
     * TODO: Add another test with eager analysis
     */
    intercept[SnowflakeSQLException] {
      courseSales.groupBy().agg(grouping_id($"course")).collect()
    }
  }

  test("SN - count") {
    checkAnswer(
      testData2.agg(count($"a"), sum_distinct($"a")), // non-partial
      Seq(Row(6, 6.0)))
  }

  test("SN - stddev") {
    val testData2ADev = math.sqrt(4.0 / 5.0)
    checkAnswer(
      testData2.agg(stddev($"a"), stddev_pop($"a"), stddev_samp($"a")),
      Seq(Row(testData2ADev, 0.8164967850518458, testData2ADev)))
  }

  test("SN - moments") {
    // checkAggregatesWithTol require input snDataFrame with doubles, snowflake
    // returns snDataFrame with decimal (BigInt), so cannot use checkAggregatesWithTol

    val varianceVal = testData2.agg(variance($"a"))
    checkAnswer(varianceVal, Seq(Row(0.8)))

    checkAnswer(
      testData2.groupBy($"a").agg(variance($"b")),
      Row(1, 0.50000) :: Row(2, 0.50000) :: Row(3, 0.500000) :: Nil)

    var statement = runQueryReturnStatement(
      "select variance(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b);",
      session)
    val varianceResult = statement.getResultSet

    while (varianceResult.next()) {
      checkAnswer(varianceVal, Seq(Row(varianceResult.getDouble(1))))
    }
    statement.close()

    val variancePop = testData2.agg(var_pop($"a"))
    checkAnswer(variancePop, Seq(Row(0.666667)))

    val varianceSamp = testData2.agg(var_samp($"a"))
    checkAnswer(varianceSamp, Seq(Row(0.80000)))

    val skewness = testData2.agg(skew($"a"))

    // Snowflake returns -1.875000000000 while the correct answer is -1.5
    // Snowflake KURTOSIS returns DOUBLE if the input data type is DOUBLE/FLOAT
    //     and DECIMAL if the input data type is another numeric data type
    val kurtosisVal = testData2.agg(kurtosis($"a"))
    checkAnswer(kurtosisVal, Seq(Row(-1.875000000000)))

    // add sql test
    statement = runQueryReturnStatement(
      "select kurtosis(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b);",
      session)
    val aggKurtosisResult = statement.getResultSet

    while (aggKurtosisResult.next()) {
      checkAnswer(kurtosisVal, Seq(Row(aggKurtosisResult.getDouble(1))))
    }
    statement.close()
  }

  test("SN - zero moments") {
    val input = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      input.agg(
        stddev($"a"),
        stddev_samp($"a"),
        stddev_pop($"a"),
        variance($"a"),
        var_samp($"a"),
        var_pop($"a"),
        skew($"a"),
        kurtosis($"a")),
      Seq(Row(null, null, 0.0, null, 0.000000, null, null)))

    checkAnswer(
      input.agg(
        sqlExpr("stddev(a)"),
        sqlExpr("stddev_samp(a)"),
        sqlExpr("stddev_pop(a)"),
        sqlExpr("variance(a)"),
        sqlExpr("var_samp(a)"),
        sqlExpr("var_pop(a)"),
        sqlExpr("skew(a)"),
        sqlExpr("kurtosis(a)")),
      Row(null, null, 0.0, null, null, 0.0, null, null))
  }

  test("SN - null moments") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData
        .agg(variance($"a"), var_samp($"a"), var_pop($"a"), skew($"a"), kurtosis($"a")),
      Seq(Row(null, null, null, null)))

    checkAnswer(
      emptyTableData.agg(
        sqlExpr("variance(a)"),
        sqlExpr("var_samp(a)"),
        sqlExpr("var_pop(a)"),
        sqlExpr("skew(a)"),
        sqlExpr("kurtosis(a)")),
      Seq(Row(null, null, null, null, null)))
  }

  test("SN - Decimal sum/avg over window should work.") {
    checkAnswer(
      session.sql("select sum(a) over () from values (1.0), (2.0), (3.0) T(a)"),
      Row(6.0) :: Row(6.0) :: Row(6.0) :: Nil)
    checkAnswer(
      session.sql("select avg(a) over () from values (1.0), (2.0), (3.0) T(a)"),
      Row(2.0) :: Row(2.0) :: Row(2.0) :: Nil)
  }

  test("SN - aggregate function in GROUP BY") {
    val e = intercept[SnowflakeSQLException] {
      testData4.groupBy(sum($"key")).count().collect()
    }
    assert(e.getMessage.contains("is not a valid group by expression"))
  }

  test("SN - ints in aggregation expressions are taken as group-by ordinal.") {
    checkAnswer(
      testData2.groupBy(lit(3), lit(4)).agg(lit(6), lit(7), sum($"b")),
      Seq(Row(3, 4, 6, 7, 9)))

    checkAnswer(
      testData2.groupBy(lit(3), lit(4)).agg(lit(6), $"b", sum($"b")),
      Seq(Row(3, 4, 6, 1, 3), Row(3, 4, 6, 2, 6)))

    val testdata2Str: String =
      "(SELECT * FROM VALUES (1,1),(1,2),(2,1),(2,2),(3,1),(3,2) T(a, b) )"
    checkAnswer(
      session.sql(s"SELECT 3, 4, SUM(b) FROM $testdata2Str GROUP BY 1, 2"),
      Seq(Row(3, 4, 9)))
    checkAnswer(
      session.sql(s"SELECT 3 AS c, 4 AS d, SUM(b) FROM $testdata2Str GROUP BY c, d"),
      Seq(Row(3, 4, 9)))
  }

  test("distinct and unions") {
    var lhs = Seq((1, "one", 1.0), (2, "one", 2.0)).toDF("i", "s", """"i"""")
    var rhs = Seq((1, "one", 1.0), (2, "one", 2.0)).toDF("i", "s", """"i"""")
    checkAnswer(lhs.union(rhs).distinct(), Row(1, "one", 1.0) :: Row(2, "one", 2.0) :: Nil)

    val lhsD = lhs.select($"s").distinct()
    rhs = rhs.select($"s")
    checkAnswer(lhsD.union(rhs), Row("one") :: Nil)

    lhs = lhs.select($"s")
    var rhsD = rhs.select($"s").distinct()

    checkAnswer(lhs.union(rhsD), Row("one") :: Nil)
    checkAnswer(lhsD.union(rhsD), Row("one") :: Nil)
  }

  test("distinct and unionAll") {
    var lhs = Seq((1, "one", 1.0), (2, "one", 2.0)).toDF("i", "s", """"i"""")
    var rhs = Seq((1, "one", 1.0), (2, "one", 2.0)).toDF("i", "s", """"i"""")
    checkAnswer(lhs.unionAll(rhs).distinct(), Row(1, "one", 1.0) :: Row(2, "one", 2.0) :: Nil)

    val lhsD = lhs.select($"s").distinct()
    rhs = rhs.select($"s")
    checkAnswer(lhsD.unionAll(rhs), Row("one") :: Row("one") :: Row("one") :: Nil)

    lhs = lhs.select($"s")
    var rhsD = rhs.select($"s").distinct()

    checkAnswer(lhs.unionAll(rhsD), Row("one") :: Row("one") :: Row("one") :: Nil)
    checkAnswer(lhsD.unionAll(rhsD), Row("one") :: Row("one") :: Nil)
  }

  // Used temporary VIEW which is not supported by owner's mode stored proc yet
  test("SN - count_if", JavaStoredProcExcludeOwner) {
    Seq(
      ("a", None),
      ("a", Some(1)),
      ("a", Some(2)),
      ("a", Some(3)),
      ("b", None),
      ("b", Some(4)),
      ("b", Some(5)),
      ("b", Some(6)))
      .toDF("x", "y")
      .createOrReplaceTempView("tempView")

    checkAnswer(
      session.sql(
        "SELECT COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), " +
          "COUNT_IF(y IS NULL) FROM tempView"),
      Seq(Row(0L, 3L, 3L, 2L)))

    checkAnswer(
      session.sql(
        "SELECT x, COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), " +
          "COUNT_IF(y IS NULL) FROM tempView GROUP BY x"),
      Row("a", 0L, 1L, 2L, 1L) :: Row("b", 0L, 2L, 1L, 1L) :: Nil)

    checkAnswer(
      session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 1"),
      Seq(Row("a")))

    checkAnswer(
      session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 2"),
      Seq(Row("b")))

    checkAnswer(
      session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y IS NULL) > 0"),
      Row("a") :: Row("b") :: Nil)

    checkAnswer(session.sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(NULL) > 0"), Nil)

    val error = intercept[SnowflakeSQLException] {
      session.sql("SELECT COUNT_IF(x) FROM tempView").collect()
    }
  }

  test("rollup") {
    checkAnswer(
      courseSales.rollup("course", "year").sum($"earnings"),
      Row("Java", 2012, 20000.0) ::
        Row("Java", 2013, 30000.0) ::
        Row("Java", null, 50000.0) ::
        Row("dotNET", 2012, 15000.0) ::
        Row("dotNET", 2013, 48000.0) ::
        Row("dotNET", null, 63000.0) ::
        Row(null, null, 113000.0) :: Nil)
  }
  test("grouping/grouping_id inside window function") {

    val w = Window.orderBy(sum($"earnings"))
    checkAnswer(
      courseSales
        .cube("course", "year")
        .agg(
          sum($"earnings"),
          grouping_id($"course", $"year"),
          rank().over(
            Window.partitionBy(grouping_id($"course", $"year")).orderBy(sum($"earnings")))),
      Row("Java", 2012, 20000.0, 0, 2) ::
        Row("Java", 2013, 30000.0, 0, 3) ::
        Row("Java", null, 50000.0, 1, 1) ::
        Row("dotNET", 2012, 15000.0, 0, 1) ::
        Row("dotNET", 2013, 48000.0, 0, 4) ::
        Row("dotNET", null, 63000.0, 1, 2) ::
        Row(null, 2012, 35000.0, 2, 1) ::
        Row(null, 2013, 78000.0, 2, 2) ::
        Row(null, null, 113000.0, 3, 1) :: Nil)
  }

  test("References in grouping functions should be indexed with semanticEquals") {
    checkAnswer(
      courseSales
        .cube("course", "year")
        .agg(grouping($"CouRse"), grouping($"year")),
      Row("Java", 2012, 0, 0) ::
        Row("Java", 2013, 0, 0) ::
        Row("Java", null, 0, 1) ::
        Row("dotNET", 2012, 0, 0) ::
        Row("dotNET", 2013, 0, 0) ::
        Row("dotNET", null, 0, 1) ::
        Row(null, 2012, 1, 0) ::
        Row(null, 2013, 1, 0) ::
        Row(null, null, 1, 1) :: Nil)
  }
  test("agg without groups") {
    checkAnswer(testData2.agg(sum($"b")), Row(9))
  }

  test("agg without groups and functions") {
    checkAnswer(testData2.agg(lit(1)), Row(1))
  }

  test("null average") {
    checkAnswer(testData3.agg(avg($"b")), Row(2.0))

    checkAnswer(testData3.agg(avg($"b"), count_distinct($"b")), Row(2.0, 1))

    checkAnswer(
      testData3.agg(avg($"b"), sum_distinct($"b")), // non-partial
      Row(2.0, 2.0))
  }

  test("zero average") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(emptyTableData.agg(avg($"a")), Row(null))

    checkAnswer(
      emptyTableData.agg(avg($"a"), sum_distinct($"b")), // non-partial
      Row(null, null))
  }
  test("multiple column distinct count") {
    val df1 = Seq(
      ("a", "b", "c"),
      ("a", "b", "c"),
      ("a", "b", "d"),
      ("x", "y", "z"),
      ("x", "q", null.asInstanceOf[String]))
      .toDF("key1", "key2", "key3")

    checkAnswer(df1.agg(count_distinct($"key1", $"key2")), Row(3))

    checkAnswer(df1.agg(count_distinct($"key1", $"key2", $"key3")), Row(3))

    checkAnswer(
      df1.groupBy($"key1").agg(count_distinct($"key2", $"key3")),
      Seq(Row("a", 2), Row("x", 1)))
  }

  test("zero count") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(count($"a"), sum_distinct($"a")), // non-partial
      Row(0, null))
  }

  test("zero stddev") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(stddev($"a"), stddev_pop($"a"), stddev_samp($"a")),
      Row(null, null, null))
  }

  test("zero sum") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(emptyTableData.agg(sum($"a")), Row(null))
  }
  test("zero sum distinct") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(emptyTableData.agg(sum_distinct($"a")), Row(null))
  }

  test("limit + aggregates") {
    val df = Seq(("a", 1), ("b", 2), ("c", 1), ("d", 5)).toDF("id", "value")
    val limit2Df = df.limit(2)
    checkAnswer(limit2Df.groupBy("id").count().select($"id"), limit2Df.select($"id"), true)
  }

  test("listagg") {
    val df = Seq(
      (2, 1, 35, "red", 99),
      (7, 2, 24, "red", 99),
      (7, 9, 77, "green", 99),
      (8, 5, 11, "green", 99),
      (8, 4, 14, "blue", 99),
      (8, 3, 21, "red", 99),
      (9, 9, 12, "orange", 99)).toDF("v1", "v2", "length", "color", "unused")

    val result = df.groupBy(df.col("color")).agg(listagg(df.col("length"), ",")).collect()
    // result is unpredictable without within group
    assert(result.length == 4)

    checkAnswer(
      df.groupBy(df.col("color"))
        .agg(listagg(df.col("length"), ",")
          .withinGroup(df.col("length").asc))
        .sort($"color"),
      Seq(Row("blue", "14"), Row("green", "11,77"), Row("orange", "12"), Row("red", "21,24,35")),
      sort = false)
  }
}
