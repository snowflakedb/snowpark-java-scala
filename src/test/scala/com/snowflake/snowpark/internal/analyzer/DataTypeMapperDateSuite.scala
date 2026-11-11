package com.snowflake.snowpark.internal.analyzer

import java.sql.Date
import java.time.LocalDate
import java.util.{Random, TimeZone}

import com.snowflake.snowpark.types.DateType
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat
import org.scalatest.funsuite.AnyFunSuite

// DATE literals used to be formatted with the JDBC driver's SnowflakeDateTimeFormat, which uses the
// hybrid Julian calendar and drops the era. They must stay the same from 1582-10-15 on, where the
// hybrid and proleptic Gregorian calendars agree.
class DataTypeMapperDateSuite extends AnyFunSuite {
  private val MILLIS_PER_DAY = 24 * 3600 * 1000L
  // 1582-10-15
  private val FIRST_GREGORIAN_DAY = -141427

  private def oldDate(v: Int): String =
    SnowflakeDateTimeFormat
      .fromSqlFormat("YYYY-MM-DD")
      .format(new Date(v * MILLIS_PER_DAY), TimeZone.getTimeZone("GMT"))

  // Parses yyyy-MM-dd with an optional minus sign back to proleptic Gregorian days since epoch.
  private def parseDays(s: String): Long = {
    val negative = s.startsWith("-")
    val Array(y, m, d) = (if (negative) s.substring(1) else s).split("-")
    assert(y.length >= 4 && m.length == 2 && d.length == 2, s)
    LocalDate.of(if (negative) -y.toInt else y.toInt, m.toInt, d.toInt).toEpochDay
  }

  private def withDefaultTimeZone(id: String)(body: => Unit): Unit = {
    val saved = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(id))
      body
    } finally {
      TimeZone.setDefault(saved)
    }
  }

  private val edgeDays: Seq[Int] = Seq(
    0,
    1,
    -1,
    -719162, // 0001-01-01
    -719163,
    -719528, // 0000-01-01
    FIRST_GREGORIAN_DAY,
    FIRST_GREGORIAN_DAY - 1,
    -700000,
    -1000000,
    2932896, // 9999-12-31
    2932897,
    Int.MaxValue,
    Int.MinValue)

  test("DATE literal is unchanged from 1582-10-15 on") {
    edgeDays.filter(_ >= FIRST_GREGORIAN_DAY).foreach { v =>
      assert(DataTypeMapper.formatDate(v) == oldDate(v), s"days = $v")
    }
    (FIRST_GREGORIAN_DAY to FIRST_GREGORIAN_DAY + 100000).foreach { v =>
      assert(DataTypeMapper.formatDate(v) == oldDate(v), s"days = $v")
    }
    val random = new Random(42)
    (1 to 20000).foreach { _ =>
      val v = FIRST_GREGORIAN_DAY + random.nextInt(Int.MaxValue)
      assert(DataTypeMapper.formatDate(v) == oldDate(v), s"days = $v")
    }
  }

  test("DATE literal round trips to the same proleptic Gregorian day") {
    edgeDays.foreach(v => assert(parseDays(DataTypeMapper.formatDate(v)) == v, s"days = $v"))
    // year -0200 .. 1600 covers all Julian/Gregorian differences, year 0 and BCE years
    (-800000 to -140000).foreach { v =>
      assert(parseDays(DataTypeMapper.formatDate(v)) == v, s"days = $v")
    }
    val random = new Random(43)
    (1 to 20000).foreach { _ =>
      val v = random.nextInt()
      assert(parseDays(DataTypeMapper.formatDate(v)) == v, s"days = $v")
    }
  }

  test("DATE literal keeps the era and the day before 1582-10-15") {
    // epoch days of these literals as read by Snowflake
    Seq(
      -141427 -> "1582-10-15",
      -141438 -> "1582-10-04",
      -171664 -> "1500-01-01",
      -354120 -> "1000-06-15",
      -719162 -> "0001-01-01",
      -719163 -> "0000-12-31",
      -719528 -> "0000-01-01",
      -719529 -> "-0001-12-31",
      -719893 -> "-0001-01-01",
      -735525 -> "-0044-03-15",
      2932897 -> "10000-01-01").foreach { case (v, expected) =>
      assert(DataTypeMapper.toSql(v, Some(DateType)) == s"DATE '$expected'", s"days = $v")
    }
    assert(DataTypeMapper.formatDate(-719528) != DataTypeMapper.formatDate(-719162))
  }

  test("DATE literal of LocalDate and java.sql.Date keeps the date") {
    Seq("1500-01-01", "1582-10-04", "1000-06-15", "0001-01-01", "1970-01-01", "2024-02-29")
      .foreach { s =>
        assert(DataTypeMapper.formatDate(DateTimeUtils.localDateToDays(LocalDate.parse(s))) == s)
        assert(DataTypeMapper.formatDate(DateTimeUtils.javaDateToDays(Date.valueOf(s))) == s)
      }
  }

  test("DATE literal does not depend on the default time zone") {
    val expected = edgeDays.map(DataTypeMapper.formatDate)
    Seq("UTC", "America/Los_Angeles", "Asia/Kathmandu", "Pacific/Kiritimati").foreach { tz =>
      withDefaultTimeZone(tz) {
        assert(edgeDays.map(DataTypeMapper.formatDate) == expected, tz)
      }
    }
  }

  test("DATE literal examples") {
    assert(DataTypeMapper.toSql(0, Some(DateType)) == "DATE '1970-01-01'")
    assert(DataTypeMapper.toSql(-1, Some(DateType)) == "DATE '1969-12-31'")
    assert(DataTypeMapper.toSql(2932896, Some(DateType)) == "DATE '9999-12-31'")
  }
}
