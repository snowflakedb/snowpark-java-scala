package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{Row, SNTestBase, SnowparkClientException}

import scala.math.abs
import scala.util.Random

class DataFrameRangeSuite extends SNTestBase {

  test("range") {
    checkAnswer(session.range(5), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

    checkAnswer(session.range(3, 5), Seq(Row(3), Row(4)))

    checkAnswer(session.range(3, 10, 2), Seq(Row(3), Row(5), Row(7), Row(9)))
  }

  test("negative test") {
    // Step can't be 0
    val ex = intercept[SnowparkClientException]({
      session.range(-3, -5, 0).show()
    })
    assert(ex.errorCode.equals("0119"))
    assert(ex.message.contains("The step for range() cannot be 0."))
  }

  test("test range: empty result, negative start/end/step") {
    assert(session.range(3, 5, -1).count() == 0)
    assert(session.range(-3, -5, 1).count() == 0)

    checkAnswer(session.range(-3, -10, -2), Seq(Row(-3), Row(-5), Row(-7), Row(-9)))
    checkAnswer(session.range(10, 3, -3), Seq(Row(10), Row(7), Row(4)), false)
  }

  test("range api") {
    // We don't have numPartition parameter, so sub-tests 1,2,4,5,6,7,8,9,17 are skipped

    val res3 = session.range(1, -2).select("id")
    assert(res3.count == 0)

    // only end provided as argument
    val res10 = session.range(10).select("id")
    assert(res10.count == 10)
    assert(res10.agg(sum(col("id")).as("sumid")).collect() === Seq(Row(45)))

    val res11 = session.range(-1).select("id")
    assert(res11.count == 0)

    // using the default slice number
    val res12 = session.range(3, 15, 3).select("id")
    assert(res12.count == 4)
    assert(res12.agg(sum(col("id")).as("sumid")).collect() === Seq(Row(30)))

    // difference between range start and end does not fit in a 64-bit integer
    val n = 9L * 1000 * 1000 * 1000 * 1000 * 1000 * 1000
    val res13 = session.range(-n, n, n / 9).select("id")
    assert(res13.count == 18)

    // range with non aggregation operation
    val res14 = session.range(0, 100, 2).toDF("id").filter(col("id") >= 50)
    val len14 = res14.collect().length
    assert(len14 == 25)

    val res15 = session.range(100, -100, -2).toDF("id").filter(col("id") <= 0)
    val len15 = res15.collect().length
    assert(len15 == 50)

    val res16 = session.range(-1500, 1500, 3).toDF("id").filter(col("id") >= 0)
    val len16 = res16.collect().length
    assert(len16 == 500)
  }

  test("range test") {
    assert(session.range(3).select("id").collect().length == 3)
  }

  test("Range with randomized parameters") {
    import session.implicits._
    val MAX_NUM_STEPS = 10L * 1000

    val seed = System.currentTimeMillis()
    val random = new Random(seed)

    def randomBound(): Long = {
      val n = random.nextLong() % (Long.MaxValue / (100 * MAX_NUM_STEPS))
      if (random.nextBoolean()) n else -n
    }

    for (l <- 1 to 10) {
      val start = randomBound()
      val end = randomBound()
      val numSteps = (abs(random.nextLong()) % MAX_NUM_STEPS) + 1
      val stepAbs = (abs(end - start) / numSteps) + 1
      val step = if (start < end) stepAbs else -stepAbs

      val expCount = (start until end by step).size
      val expSum = (start until end by step).sum

      val res = session.range(start, end, step).toDF("id").agg(count($"id"), sum($"id")).collect()

      assert(!res.isEmpty)
      assert(res.head.getLong(0) == expCount)
      if (expCount > 0) {
        assert(res.head.getLong(1) == expSum)
      }
    }
    succeed
  }

  test("Session range with Max and Min") {
    val start = java.lang.Long.MAX_VALUE - 3
    val end = java.lang.Long.MIN_VALUE + 2
    assert(session.range(start, end, 1).collect.length == 0)
    assert(session.range(start, start, 1).collect.length == 0)
  }
}
