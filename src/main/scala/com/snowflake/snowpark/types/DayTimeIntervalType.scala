package com.snowflake.snowpark.types

/**
 * Day-time interval data type. Mapped to INTERVAL DAY TO SECOND Snowflake data type.
 *
 * At the UDF/sproc boundary, day-time interval values are transmitted as Arrow MonthDayNano
 * ({months=0, days=D, nanoseconds=N}) and surfaced to Java/Scala handlers as
 * [[java.time.Duration]].
 *
 * @since 1.14.0
 */
object DayTimeIntervalType extends AtomicType
