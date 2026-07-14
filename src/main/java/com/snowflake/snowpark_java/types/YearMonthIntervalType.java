package com.snowflake.snowpark_java.types;

/**
 * Year-month interval data type. This maps to INTERVAL YEAR TO MONTH data type in Snowflake.
 *
 * <p>At the UDF/sproc boundary, values are transmitted as Arrow MonthDayNano and surfaced to Java
 * handlers as {@link java.time.Period}.
 *
 * @since 1.14.0
 */
public class YearMonthIntervalType extends DataType {
  YearMonthIntervalType() {}
}
