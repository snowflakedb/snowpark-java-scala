package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{JavaStoredProcExclude, Row, TestData, TestUtils, UDFTest}

import java.time.{Duration, Period}

/**
 * End-to-end UDF tests for native INTERVAL types.
 *
 * Prerequisites:
 *   - JDBC 3.27.0+ (typed Duration/Period ResultSet reads)
 *
 * Mirrors snowpark-python tests/integ/test_interval_udf_e2e.py
 */
@UDFTest
class IntervalUDFSuite extends TestData {

  private def withIntervalUdfEnabled(body: => Unit): Unit =
    withSessionParameters(Seq(("ENABLE_INTERVAL_TYPES_IN_UDF", "true")), session)(body)

  override def beforeAll: Unit = {
    super.beforeAll()
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session)
    }
  }

  test("UDF Duration input/return (INTERVAL DAY TO SECOND)", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val addDay = udf((x: Duration) => if (x == null) null else x.plusDays(1))
      val df = session.sql("SELECT INTERVAL '5' DAY AS d")
      checkAnswer(df.select(addDay(col("d"))), Seq(Row(Duration.ofDays(6))))
    }
  }

  test("UDF Duration with day-time components", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val triple = udf((x: Duration) => x.multipliedBy(3))
      val df = session.sql("SELECT INTERVAL '2 12:00:00' DAY TO SECOND AS d")
      checkAnswer(df.select(triple(col("d"))), Seq(Row(Duration.ofDays(7).plusHours(12))))
    }
  }

  test("UDF Duration null input", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val identity = udf((x: Duration) => x)
      val df = session.sql("SELECT NULL::INTERVAL DAY TO SECOND AS d")
      checkAnswer(df.select(identity(col("d"))), Seq(Row(null)))
    }
  }

  test("UDF Duration negative interval", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val negate = udf((x: Duration) => x.negated())
      val df = session.sql("SELECT INTERVAL '3' DAY AS d")
      checkAnswer(df.select(negate(col("d"))), Seq(Row(Duration.ofDays(-3))))
    }
  }

  test("UDF Duration to Long (inspect components)", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val getDays = udf((x: Duration) => x.toDays)
      val df = session.sql("SELECT INTERVAL '5 06:30:00' DAY TO SECOND AS d")
      checkAnswer(df.select(getDays(col("d"))), Seq(Row(5L)))
    }
  }

  test("UDF Period input/return (INTERVAL YEAR TO MONTH)", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val addYear = udf((x: Period) => if (x == null) null else x.plusYears(1))
      val df = session.sql("SELECT INTERVAL '1-2' YEAR TO MONTH AS m")
      // 1 year 2 months + 1 year = 2 years 2 months
      checkAnswer(df.select(addYear(col("m"))), Seq(Row(Period.of(2, 2, 0))))
    }
  }

  test("UDF Period null input", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val identity = udf((x: Period) => x)
      val df = session.sql("SELECT NULL::INTERVAL YEAR TO MONTH AS m")
      checkAnswer(df.select(identity(col("m"))), Seq(Row(null)))
    }
  }

  test("registerTemporary Duration UDF", JavaStoredProcExclude) {
    withIntervalUdfEnabled {
      val addDay = session.udf.registerTemporary((x: Duration) => x.plusDays(1))
      val df = session.sql("SELECT INTERVAL '5' DAY AS d")
      checkAnswer(df.select(addDay(col("d"))), Seq(Row(Duration.ofDays(6))))
    }
  }
}
