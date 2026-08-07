package com.snowflake.snowpark.types

/**
 * Year-month interval data type. Mapped to INTERVAL YEAR TO MONTH Snowflake data type.
 *
 * At the UDF/sproc boundary, year-month interval values are transmitted as Arrow MonthDayNano
 * ({months=M, days=0, nanoseconds=0}) and surfaced to Java/Scala handlers as [[java.time.Period]].
 *
 * @since 1.14.0
 */
object YearMonthIntervalType extends AtomicType
