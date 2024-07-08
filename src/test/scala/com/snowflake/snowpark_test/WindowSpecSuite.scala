package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{DataFrame, Row, TestData, Window}
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.scalatest.matchers.should.Matchers.the

import scala.reflect.ClassTag

class WindowSpecSuite extends TestData {
  import session.implicits._

  test("partitionBy, orderBy, rowsBetween") {
    val df = Seq((1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key").rowsBetween(-1, 2)

    checkAnswer(
      df.select($"key", avg($"key").over(window)),
      Row(1, 1.333) :: Row(1, 1.333) :: Row(2, 1.500) :: Row(2, 2.000) ::
        Row(2, 2.000) :: Nil)

    val window2 = Window.rowsBetween(Window.currentRow, 2).orderBy($"key")
    checkAnswer(
      df.select($"key", avg($"key").over(window2)),
      Seq(Row(2, 2.000), Row(2, 2.000), Row(2, 2.000), Row(1, 1.666), Row(1, 1.333)),
      sort = false)
  }

  test("rangeBetween") {
    val df = Seq("non_numeric").toDF("value")
    val window = Window.orderBy($"value")

    checkAnswer(
      df.select(
        $"value",
        min($"value")
          .over(window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      Row("non_numeric", "non_numeric") :: Nil)

    val window2 =
      Window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing).orderBy($"value")
    checkAnswer(
      df.select($"value", min($"value").over(window2)),
      Row("non_numeric", "non_numeric") :: Nil)
  }

  test("window function with aggregates") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2))
      .toDF("key", "value")
    val window = Window.orderBy()
    checkAnswer(
      df.groupBy($"key")
        .agg(sum($"value"), sum(sum($"value")).over(window) - sum($"value")),
      Seq(Row("a", 6, 9), Row("b", 9, 6)))
  }

  test("Window functions inside WHERE and HAVING clauses") {
    def checkAnalysisError[T: ClassTag](df: => DataFrame): Unit = {
      the[T] thrownBy {
        df.collect()
      }
    }

    checkAnalysisError[SnowflakeSQLException](
      testData2.select("a").where(rank().over(Window.orderBy($"b")) === 1))
    checkAnalysisError[SnowflakeSQLException](
      testData2.where($"b" === 2 && rank().over(Window.orderBy($"b")) === 1))
    checkAnalysisError[SnowflakeSQLException](
      testData2
        .groupBy($"a")
        .agg(avg($"b").as("avgb"))
        .where($"a" > $"avgb" && rank().over(Window.orderBy($"a")) === 1))
    checkAnalysisError[SnowflakeSQLException](
      testData2
        .groupBy($"a")
        .agg(max($"b").as("maxb"), sum($"b").as("sumb"))
        .where(rank().over(Window.orderBy($"a")) === 1))
    checkAnalysisError[SnowflakeSQLException](
      testData2
        .groupBy($"a")
        .agg(max($"b").as("maxb"), sum($"b").as("sumb"))
        .where($"sumb" === 5 && rank().over(Window.orderBy($"a")) === 1))

    testData2.createOrReplaceTempView("testData2")
    checkAnalysisError[SnowflakeSQLException](
      session.sql("SELECT a FROM testData2 WHERE RANK() OVER(ORDER BY b) = 1"))
    checkAnalysisError[SnowflakeSQLException](
      session.sql("SELECT * FROM testData2 WHERE b = 2 AND RANK() OVER(ORDER BY b) = 1"))
    checkAnalysisError[SnowflakeSQLException](
      session.sql(
        "SELECT * FROM testData2 GROUP BY a HAVING a > AVG(b) AND RANK() OVER(ORDER BY a) = 1"))
    checkAnalysisError[SnowflakeSQLException](
      session.sql(
        "SELECT a, MAX(b), SUM(b) FROM testData2 GROUP BY a HAVING RANK() OVER(ORDER BY a) = 1"))
    checkAnalysisError[SnowflakeSQLException](session.sql(s"""SELECT a, MAX(b)
                     |FROM testData2
                     |GROUP BY a
                     |HAVING SUM(b) = 5 AND RANK() OVER(ORDER BY a) = 1
                     |""".stripMargin))
    succeed
  }

  test("reuse window partitionBy") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val w = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(lead($"key", 1).over(w), lead($"value", 1).over(w)),
      Row(1, "1") :: Row(2, "2") :: Row(null, null) :: Row(null, null) :: Nil)
  }

  test("reuse window orderBy") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val w = Window.orderBy($"value").partitionBy($"key")

    checkAnswer(
      df.select(lead($"key", 1).over(w), lead($"value", 1).over(w)),
      Row(1, "1") :: Row(2, "2") :: Row(null, null) :: Row(null, null) :: Nil)
  }

  test("rank functions in unspecific window") {
    val df = Seq((1, "1"), (2, "2"), (1, "2"), (2, "2")).toDF("key", "value")
    df.createOrReplaceTempView("window_table")
    checkAnswer(
      df.select(
        $"key",
        max($"key").over(Window.partitionBy($"value").orderBy($"key")),
        min($"key").over(Window.partitionBy($"value").orderBy($"key")),
        mean($"key").over(Window.partitionBy($"value").orderBy($"key")),
        count($"key").over(Window.partitionBy($"value").orderBy($"key")),
        sum($"key").over(Window.partitionBy($"value").orderBy($"key")),
        ntile(lit(2)).over(Window.partitionBy($"value").orderBy($"key")),
        row_number().over(Window.partitionBy($"value").orderBy($"key")),
        dense_rank().over(Window.partitionBy($"value").orderBy($"key")),
        rank().over(Window.partitionBy($"value").orderBy($"key")),
        cume_dist().over(Window.partitionBy($"value").orderBy($"key")),
        percent_rank().over(Window.partitionBy($"value").orderBy($"key"))),
      Row(1, 1, 1, 1.0d, 1, 1, 1, 1, 1, 1, 1.0d / 3.0d, 0.0d) ::
        Row(1, 1, 1, 1.0d, 1, 1, 1, 1, 1, 1, 1.0d, 0.0d) ::
        Row(2, 2, 1, 5.0d / 3.0d, 3, 5, 1, 2, 2, 2, 1.0d, 0.5d) ::
        Row(2, 2, 1, 5.0d / 3.0d, 3, 5, 2, 3, 2, 2, 1.0d, 0.5d) :: Nil)
  }

  test("empty over spec") {

    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("b", 2)).toDF("key", "value")
    df.createOrReplaceTempView("window_table")
    checkAnswer(
      df.select($"key", $"value", sum($"value").over(), avg($"value").over()),
      Seq(Row("a", 1, 6, 1.5), Row("a", 1, 6, 1.5), Row("a", 2, 6, 1.5), Row("b", 2, 6, 1.5)))
    checkAnswer(
      session.sql("select key, value, sum(value) over(), avg(value) over() from window_table"),
      Seq(Row("a", 1, 6, 1.5), Row("a", 1, 6, 1.5), Row("a", 2, 6, 1.5), Row("b", 2, 6, 1.5)))

  }
  test("null inputs") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2))
      .toDF("key", "value")
    val window = Window.orderBy()
    checkAnswer(
      df.select($"key", $"value", avg(lit(null)).over(window), sum(lit(null)).over(window)),
      Seq(
        Row("a", 1, null, null),
        Row("a", 1, null, null),
        Row("a", 2, null, null),
        Row("a", 2, null, null),
        Row("b", 4, null, null),
        Row("b", 3, null, null),
        Row("b", 2, null, null)),
      false)
  }

  test("SN - window function should fail if order by clause is not specified") {
    val df = Seq((1, "1"), (2, "2"), (1, "2"), (2, "2")).toDF("key", "value")
    assertThrows[SnowflakeSQLException](
      // Here we missed .orderBy("key")!
      df.select(row_number().over(Window.partitionBy($"value"))).collect())
  }

  test("SN - corr, covar_pop, stddev_pop functions in specific window") {
    val df = Seq(
      ("a", "p1", 10.0, 20.0),
      ("b", "p1", 20.0, 10.0),
      ("c", "p2", 20.0, 20.0),
      ("d", "p2", 20.0, 20.0),
      ("e", "p3", 0.0, 0.0),
      ("f", "p3", 6.0, 12.0),
      ("g", "p3", 6.0, 12.0),
      ("h", "p3", 8.0, 16.0),
      ("i", "p4", 5.0, 5.0)).toDF("key", "partitionId", "value1", "value2")
    checkAnswer(
      df.select(
        $"key",
        corr($"value1", $"value2").over(
          Window
            .partitionBy($"partitionId")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        covar_pop($"value1", $"value2")
          .over(
            Window
              .partitionBy($"partitionId")
              .orderBy($"key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        var_pop($"value1")
          .over(
            Window
              .partitionBy($"partitionId")
              .orderBy($"key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        stddev_pop($"value1")
          .over(
            Window
              .partitionBy($"partitionId")
              .orderBy($"key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        var_pop($"value2")
          .over(
            Window
              .partitionBy($"partitionId")
              .orderBy($"key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        stddev_pop($"value2")
          .over(
            Window
              .partitionBy($"partitionId")
              .orderBy($"key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      Seq(
        Row("a", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
        Row("b", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
        Row("c", null, 0.0, 0.0, 0.0, 0.0, 0.0),
        Row("d", null, 0.0, 0.0, 0.0, 0.0, 0.0),
        Row("e", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
        Row("f", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
        Row("g", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
        Row("h", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
        Row("i", null, 0.0, 0.0, 0.0, 0.0, 0.0)))
  }

  test("SN - covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window") {
    val df = Seq(
      ("a", "p1", 10.0, 20.0),
      ("b", "p1", 20.0, 10.0),
      ("c", "p2", 20.0, 20.0),
      ("d", "p2", 20.0, 20.0),
      ("e", "p3", 0.0, 0.0),
      ("f", "p3", 6.0, 12.0),
      ("g", "p3", 6.0, 12.0),
      ("h", "p3", 8.0, 16.0),
      ("i", "p4", 5.0, 5.0)).toDF("key", "partitionId", "value1", "value2")
    checkAnswer(
      df.select(
        $"key",
        covar_samp($"value1", $"value2").over(
          Window
            .partitionBy($"partitionId")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        var_samp($"value1").over(
          Window
            .partitionBy($"partitionId")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        variance($"value1").over(
          Window
            .partitionBy($"partitionId")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        stddev_samp($"value1").over(
          Window
            .partitionBy($"partitionId")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        stddev($"value1").over(
          Window
            .partitionBy($"partitionId")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      Seq(
        Row("a", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
        Row("b", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
        Row("c", 0.0, 0.0, 0.0, 0.0, 0.0),
        Row("d", 0.0, 0.0, 0.0, 0.0, 0.0),
        Row("e", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
        Row("f", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
        Row("g", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
        Row("h", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
        Row("i", null, null, null, null, null)))
  }

  test("SN - skewness and kurtosis functions in window") {
    val df = Seq(
      ("a", "p1", 1.0),
      ("b", "p1", 1.0),
      ("c", "p1", 2.0),
      ("d", "p1", 2.0),
      ("e", "p1", 3.0),
      ("f", "p1", 3.0),
      ("g", "p1", 3.0),
      ("h", "p2", 1.0),
      ("i", "p2", 2.0),
      ("j", "p2", 5.0)).toDF("key", "partition", "value")
    checkAnswer(
      df.select(
        $"key",
        skew($"value").over(
          Window
            .partitionBy($"partition")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        kurtosis($"value").over(
          Window
            .partitionBy($"partition")
            .orderBy($"key")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      // results are checked by scipy.stats.skew() and scipy.stats.kurtosis()
      Seq(
        Row("a", -0.353044463087872, -1.8166064369747783),
        Row("b", -0.353044463087872, -1.8166064369747783),
        Row("c", -0.353044463087872, -1.8166064369747783),
        Row("d", -0.353044463087872, -1.8166064369747783),
        Row("e", -0.353044463087872, -1.8166064369747783),
        Row("f", -0.353044463087872, -1.8166064369747783),
        Row("g", -0.353044463087872, -1.8166064369747783),
        Row("h", 1.293342780733395, null),
        Row("i", 1.293342780733395, null),
        Row("j", 1.293342780733395, null)))
  }

  test("SN - aggregation function on invalid column") {
    val df = Seq((1, "1")).toDF("key", "value")
    assertThrows[SnowflakeSQLException](df.select($"key", count($"invalid").over()).collect())
  }

  test("SN - statistical functions") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2))
      .toDF("key", "value")
    val window = Window.partitionBy($"key")
    checkAnswer(
      df.select(
        $"key",
        var_pop($"value").over(window),
        var_samp($"value").over(window),
        approx_count_distinct($"value").over(window)),
      Seq.fill(4)(Row("a", BigDecimal(0.250000), BigDecimal(0.333333), 2))
        ++ Seq.fill(3)(Row("b", BigDecimal(0.666667), BigDecimal(1.000000), 3)))
  }

  test("SN - window functions in multiple selects") {
    val df = Seq(("S1", "P1", 100), ("S1", "P1", 700), ("S2", "P1", 200), ("S2", "P2", 300))
      .toDF("sno", "pno", "qty")

    val w1 = Window.partitionBy($"sno")
    val w2 = Window.partitionBy($"sno", $"pno")

    val select = df
      .select($"sno", $"pno", $"qty", sum($"qty").over(w2).alias("sum_qty_2"))
      .select($"sno", $"pno", $"qty", col("sum_qty_2"), sum($"qty").over(w1).alias("sum_qty_1"))

    checkAnswer(
      select,
      Seq(
        Row("S1", "P1", 100, 800, 800),
        Row("S1", "P1", 700, 800, 800),
        Row("S2", "P1", 200, 200, 500),
        Row("S2", "P2", 300, 300, 500)))

  }

}
