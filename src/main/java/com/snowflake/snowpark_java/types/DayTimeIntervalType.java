package com.snowflake.snowpark_java.types;

/**
 * Day-time interval data type. This maps to INTERVAL DAY TO SECOND data type in Snowflake.
 *
 * <p>At the UDF/sproc boundary, values are transmitted as Arrow MonthDayNano and surfaced to Java
 * handlers as {@link java.time.Duration}.
 *
 * @since 1.14.0
 */
public class DayTimeIntervalType extends DataType {
  DayTimeIntervalType() {}
}
