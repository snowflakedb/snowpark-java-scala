package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{Row, SnowparkClientException, TestData, Window}
import net.snowflake.client.jdbc.SnowflakeSQLException

class WindowFramesSuite extends TestData {
  import session.implicits._

  test("lead/lag with empty data frame") {
    val df = Seq.empty[(Int, String)].toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(df.select(lead($"value", 1).over(window), lag($"value", 1).over(window)), Nil)
  }

  test("lead/lag with positive offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select($"key", lead($"value", 1).over(window), lag($"value", 1).over(window)),
      Row(1, "3", null) :: Row(1, null, "1") :: Row(2, "4", null) :: Row(2, null, "2") :: Nil
    )
  }

  test("reverse lead/lag with positive offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value".desc)

    checkAnswer(
      df.select($"key", lead($"value", 1).over(window), lag($"value", 1).over(window)),
      Row(1, "1", null) :: Row(1, null, "3") :: Row(2, "2", null) :: Row(2, null, "4") :: Nil
    )
  }

  test("lead/lag with negative offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select($"key", lead($"value", -1).over(window), lag($"value", -1).over(window)),
      Row(1, "1", null) :: Row(1, null, "3") :: Row(2, "2", null) :: Row(2, null, "4") :: Nil
    )
  }

  test("reverse lead/lag with negative offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value".desc)

    checkAnswer(
      df.select($"key", lead($"value", -1).over(window), lag($"value", -1).over(window)),
      Row(1, "3", null) :: Row(1, null, "1") :: Row(2, "4", null) :: Row(2, null, "2") :: Nil
    )
  }

  test("lead/lag with default value") {
    val default = null
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4"), (2, "5")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        lead($"value", 2).over(window),
        lag($"value", 2).over(window),
        lead($"value", -2).over(window),
        lag($"value", -2).over(window)
      ),
      Row(1, default, default, default, default) :: Row(1, default, default, default, default) ::
        Row(2, "5", default, default, "5") :: Row(2, default, "2", "2", default) ::
        Row(2, default, default, default, default) :: Nil
    )
  }

  test("unbounded rows/range between with aggregation") {
    val df = Seq(("one", 1), ("two", 2), ("one", 3), ("two", 4)).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        sum($"value")
          .over(window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        sum($"value")
          .over(window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))
      ),
      Row("one", 4, 4) :: Row("one", 4, 4) :: Row("two", 6, 6) :: Row("two", 6, 6) :: Nil
    )
  }

  test("SN - rows between should accept int/long values as boundary") {
    val df =
      Seq((1L, "1"), (1L, "1"), (2147483650L, "1"), (3L, "2"), (2L, "1"), (2147483650L, "2"))
        .toDF("key", "value")

    checkAnswer(
      df.select(
        $"key",
        count($"key").over(Window.partitionBy($"value").orderBy($"key").rowsBetween(0, 100))
      ),
      Seq(Row(1, 3), Row(1, 4), Row(2, 2), Row(2147483650L, 1), Row(2147483650L, 1), Row(3, 2))
    )

    val e = intercept[SnowparkClientException](
      df.select(
        $"key",
        count($"key")
          .over(Window.partitionBy($"value").orderBy($"key").rowsBetween(0, 2147483648L))
      )
    )
    assert(
      e.message.contains("The ending point for the window frame is not a valid integer: 2147483648")
    )

  }

  test("SN - range between should accept at most one ORDER BY expression when unbounded") {
    val df = Seq((1, 1)).toDF("key", "value")
    val window = Window.orderBy($"key", $"value")

    checkAnswer(
      df.select(
        $"key",
        min($"key")
          .over(window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))
      ),
      Seq(Row(1, 1))
    )

    intercept[SnowflakeSQLException](
      df.select(min($"key").over(window.rangeBetween(Window.unboundedPreceding, 1))).collect
    )

    intercept[SnowflakeSQLException](
      df.select(min($"key").over(window.rangeBetween(-1, Window.unboundedFollowing))).collect
    )

    intercept[SnowflakeSQLException](
      df.select(min($"key").over(window.rangeBetween(-1, 1))).collect
    )
  }

  test("SN - range between should accept numeric values only when bounded") {
    val df = Seq("non_numeric").toDF("value")
    val window = Window.orderBy($"value")

    checkAnswer(
      df.select(
        $"value",
        min($"value")
          .over(window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))
      ),
      Row("non_numeric", "non_numeric") :: Nil
    )

    // TODO: Add another test with eager mode enabled
    intercept[SnowflakeSQLException](
      df.select(min($"value").over(window.rangeBetween(Window.unboundedPreceding, 1))).collect()
    )

    intercept[SnowflakeSQLException](
      df.select(min($"value").over(window.rangeBetween(-1, Window.unboundedFollowing))).collect()
    )

    intercept[SnowflakeSQLException](
      df.select(min($"value").over(window.rangeBetween(-1, 1))).collect()
    )
  }

  test("SN - sliding rows between with aggregation") {
    val df = Seq((1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key").rowsBetween(-1, 2)

    checkAnswer(
      df.select($"key", avg($"key").over(window)),
      Row(1, 1.333) :: Row(1, 1.333) :: Row(2, 1.500) :: Row(2, 2.000) ::
        Row(2, 2.000) :: Nil
    )
  }

  test("SN - reverse sliding rows between with aggregation") {
    val df = Seq((1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key".desc).rowsBetween(-1, 2)

    checkAnswer(
      df.select($"key", avg($"key").over(window)),
      Row(1, 1.000) :: Row(1, 1.333) :: Row(2, 1.333) :: Row(2, 2.000) ::
        Row(2, 2.000) :: Nil
    )
  }

  test("Window function any_value()") {
    val df1 = Seq(("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e"))
      .toDF("key", "value1", "value2", "rest")

    val (values_1_A, values_1_B) = (Seq(1, 3), Seq(2, 4))
    val (values_2_A, values_2_B) = (Seq(11, 33), Seq(22, 44))
    val rows = df1
      .select(
        $"key",
        any_value($"value1").over(Window.partitionBy($"key")),
        any_value($"value2").over(Window.partitionBy($"key"))
      )
      .collect()
    assert(rows.length == 4)
    rows.foreach { row =>
      if (row.getString(0).equals("a")) {
        assert(values_1_A.contains(row.getInt(1)) && values_2_A.contains(row.getInt(2)))
      } else {
        assert(values_1_B.contains(row.getInt(1)) && values_2_B.contains(row.getInt(2)))
      }
    }
  }
}
