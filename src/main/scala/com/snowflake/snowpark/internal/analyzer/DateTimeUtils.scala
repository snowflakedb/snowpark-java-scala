package com.snowflake.snowpark.internal.analyzer

import java.sql.Timestamp
import java.time.{Instant, LocalDate}
import java.util.concurrent.TimeUnit.NANOSECONDS

object DateTimeUtils {
  // Time Constants
  @inline private final val SECONDS_PER_DAY = 86400L
  @inline private final val MICROS_PER_MILLIS = 1000L
  @inline private final val MICROS_PER_SECOND = 1000000L
  @inline private final val NANOS_PER_MICROS = 1000L

  def instantToMicros(instant: Instant): Long = {
    val us = Math.multiplyExact(instant.getEpochSecond, MICROS_PER_SECOND)
    val result = Math.addExact(us, NANOSECONDS.toMicros(instant.getNano))
    result
  }

  def javaTimestampToMicros(t: Timestamp): Long =
    Math.multiplyExact(t.getTime, MICROS_PER_MILLIS) +
      (t.getNanos / NANOS_PER_MICROS) % MICROS_PER_MILLIS

  def localDateToDays(localDate: LocalDate): Int = Math.toIntExact(localDate.toEpochDay)

  def javaDateToDays(date: java.sql.Date): Int = localDateToDays(date.toLocalDate)
}
