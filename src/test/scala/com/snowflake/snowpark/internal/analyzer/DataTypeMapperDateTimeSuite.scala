package com.snowflake.snowpark.internal.analyzer

import java.sql.{Date, Timestamp}
import java.util.{Random, TimeZone}

import com.snowflake.snowpark.types.{DateType, TimestampType}
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat
import org.scalatest.funsuite.AnyFunSuite

// Compares DATE / TIMESTAMP literal formatting against the JDBC driver's SnowflakeDateTimeFormat,
// which the literals were formatted with before.
class DataTypeMapperDateTimeSuite extends AnyFunSuite {
  private val MILLIS_PER_DAY = 24 * 3600 * 1000L

  private def oldDate(v: Int): String =
    SnowflakeDateTimeFormat
      .fromSqlFormat("YYYY-MM-DD")
      .format(new Date(v * MILLIS_PER_DAY), TimeZone.getTimeZone("GMT"))

  private def oldTimestamp(v: Long): String =
    SnowflakeDateTimeFormat
      .fromSqlFormat("YYYY-MM-DD HH24:MI:SS.FF")
      .format(new Timestamp(v / 1000L), TimeZone.getDefault, 3)

  private def withDefaultTimeZone(id: String)(body: => Unit): Unit = {
    val saved = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(id))
      body
    } finally {
      TimeZone.setDefault(saved)
    }
  }

  private val timeZones = Seq(
    "UTC",
    "GMT",
    "America/Los_Angeles",
    "America/New_York",
    "Europe/Amsterdam",
    "Europe/London",
    "Asia/Kolkata",
    "Asia/Kathmandu",
    "Asia/Tokyo",
    "Australia/Lord_Howe",
    "Pacific/Kiritimati",
    "Pacific/Pago_Pago")

  private val edgeDays: Seq[Int] = Seq(
    0,
    1,
    -1,
    -719162, // 0001-01-01 in the proleptic Gregorian calendar
    -719163,
    -719528, // 0000-01-01 in the proleptic Gregorian calendar
    -141427, // 1582-10-15, first Gregorian day
    -141428, // 1582-10-04, last Julian day
    -141438,
    -141417,
    -700000,
    -755662,
    -1000000,
    -3000000,
    2932896, // 9999-12-31
    2932897,
    100000000,
    Int.MaxValue,
    Int.MinValue)

  test("DATE literal matches SnowflakeDateTimeFormat for edge values") {
    edgeDays.foreach(v => assert(DataTypeMapper.formatDate(v) == oldDate(v), s"days = $v"))
  }

  test("DATE literal matches SnowflakeDateTimeFormat for every day around calendar changes") {
    // 0001-01-01 .. 1600-12-31 covers all Julian/Gregorian differences and year < 1000
    (-719528 to -140000).foreach { v =>
      assert(DataTypeMapper.formatDate(v) == oldDate(v), s"days = $v")
    }
  }

  test("DATE literal matches SnowflakeDateTimeFormat for random values") {
    val random = new Random(42)
    (1 to 20000).foreach { _ =>
      val v = random.nextInt()
      assert(DataTypeMapper.formatDate(v) == oldDate(v), s"days = $v")
    }
    (1 to 20000).foreach { _ =>
      val v = random.nextInt(2932897 + 800000) - 800000
      assert(DataTypeMapper.formatDate(v) == oldDate(v), s"days = $v")
    }
  }

  test("DATE literal does not depend on the default time zone") {
    timeZones.foreach { tz =>
      withDefaultTimeZone(tz) {
        edgeDays.foreach(v => assert(DataTypeMapper.formatDate(v) == oldDate(v), s"$tz $v"))
      }
    }
  }

  test("DATE literal examples") {
    assert(DataTypeMapper.toSql(0, Some(DateType)) == "DATE '1970-01-01'")
    assert(DataTypeMapper.toSql(-1, Some(DateType)) == "DATE '1969-12-31'")
    assert(DataTypeMapper.toSql(2932896, Some(DateType)) == "DATE '9999-12-31'")
  }

  private val edgeMicros: Seq[Long] = Seq(
    0L,
    1L,
    -1L,
    999L,
    -999L,
    1000L,
    -1000L,
    1001L,
    -1001L,
    1234567L,
    -1234567L,
    1700000000123456L,
    -1700000000123456L,
    -62135596800000000L, // 0001-01-01 in the proleptic Gregorian calendar
    -62135769600000000L,
    -62167219200000000L, // 0000-01-01 in the proleptic Gregorian calendar
    -62167219200000001L,
    -12219292800000000L, // 1582-10-15, first Gregorian day
    -12219292800000001L,
    -12219292800001000L,
    -12220000000000000L,
    -30000000000000000L,
    -70000000000000000L,
    253402300799999999L, // 9999-12-31 23:59:59.999999 UTC
    253402300800000000L,
    253402300799999000L,
    253402300799999000L - 86400L * 1000000L,
    Long.MaxValue,
    Long.MinValue,
    Long.MaxValue - 1,
    Long.MinValue + 1)

  test("TIMESTAMP literal matches SnowflakeDateTimeFormat for edge values in many time zones") {
    timeZones.foreach { tz =>
      withDefaultTimeZone(tz) {
        edgeMicros.foreach { v =>
          assert(DataTypeMapper.formatTimestamp(v) == oldTimestamp(v), s"$tz micros = $v")
        }
      }
    }
  }

  test("TIMESTAMP literal matches SnowflakeDateTimeFormat for random values") {
    val random = new Random(7)
    timeZones.foreach { tz =>
      withDefaultTimeZone(tz) {
        (1 to 3000).foreach { _ =>
          // from year 0001 to year 9999 with sub-millisecond digits
          val v = (random.nextDouble() * 3.2e17).toLong - 6.3e16.toLong
          assert(DataTypeMapper.formatTimestamp(v) == oldTimestamp(v), s"$tz micros = $v")
        }
        (1 to 1000).foreach { _ =>
          val v = random.nextLong()
          assert(DataTypeMapper.formatTimestamp(v) == oldTimestamp(v), s"$tz micros = $v")
        }
      }
    }
  }

  test("TIMESTAMP literal matches SnowflakeDateTimeFormat around zone transitions") {
    // daylight saving transitions in 2023 and the 1582 calendar change
    val starts = Seq(1678608000L, 1699167600L, -12219292800L - 7200L)
    val random = new Random(3)
    Seq("America/Los_Angeles", "Europe/Amsterdam", "Australia/Lord_Howe").foreach { tz =>
      withDefaultTimeZone(tz) {
        starts.foreach { startSecond =>
          (-100000 to 100000 by 997).foreach { offsetSecond =>
            val v = (startSecond + offsetSecond) * 1000000L + random.nextInt(1000000)
            assert(DataTypeMapper.formatTimestamp(v) == oldTimestamp(v), s"$tz micros = $v")
          }
        }
      }
    }
  }

  test("TIMESTAMP literal examples") {
    withDefaultTimeZone("America/Los_Angeles") {
      assert(
        DataTypeMapper.toSql(1700000000123456L, Some(TimestampType)) ==
          "TIMESTAMP '2023-11-14 14:13:20.123'")
      assert(
        DataTypeMapper.toSql(-1L, Some(TimestampType)) == "TIMESTAMP '1969-12-31 16:00:00.000'")
      assert(
        DataTypeMapper.toSql(-1001L, Some(TimestampType)) ==
          "TIMESTAMP '1969-12-31 15:59:59.999'")
    }
  }
}
