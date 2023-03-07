package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;

/**
 * Represents an underlying DataFrame with rows that are grouped by common values. Can be used to
 * define aggregations on these grouped DataFrames.
 *
 * @since 0.9.0
 */
public class RelationalGroupedDataFrame {
  private final com.snowflake.snowpark.RelationalGroupedDataFrame rgdf;

  RelationalGroupedDataFrame(com.snowflake.snowpark.RelationalGroupedDataFrame rgdf) {
    this.rgdf = rgdf;
  }

  /**
   * Returns a DataFrame with aggregated computed according to the supplied Column expressions.
   * Functions contains some built-in aggregate functions that can be used.
   *
   * @see com.snowflake.snowpark_java.Functions Functions
   * @param cols The aggregate functions
   * @return The result DataFrame
   * @since 0.9.0
   */
  public DataFrame agg(Column... cols) {
    return new DataFrame(this.rgdf.agg(Column.toScalaColumnArray(cols)));
  }

  /**
   * Return the average for the specified numeric columns.
   *
   * @since 1.1.0
   * @param cols The input column list
   * @return The result DataFrame
   */
  public DataFrame avg(Column... cols) {
    return new DataFrame(
        this.rgdf.avg(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Return the average for the specified numeric columns. Alias of avg
   *
   * @since 1.1.0
   * @param cols The input column list
   * @return The result DataFrame
   */
  public DataFrame mean(Column... cols) {
    return new DataFrame(
        this.rgdf.avg(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Return the sum for the specified numeric columns.
   *
   * @since 1.1.0
   * @param cols The input column list
   * @return The result DataFrame
   */
  public DataFrame sum(Column... cols) {
    return new DataFrame(
        this.rgdf.sum(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Return the median for the specified numeric columns.
   *
   * @since 1.1.0
   * @param cols The input column list
   * @return The result DataFrame
   */
  public DataFrame median(Column... cols) {
    return new DataFrame(
        this.rgdf.median(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Return the min for the specified numeric columns.
   *
   * @since 1.1.0
   * @param cols The input column list
   * @return The result DataFrame
   */
  public DataFrame min(Column... cols) {
    return new DataFrame(
        this.rgdf.min(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Return the max for the specified numeric columns.
   *
   * @since 1.1.0
   * @param cols The input column list
   * @return The result DataFrame
   */
  public DataFrame max(Column... cols) {
    return new DataFrame(
        this.rgdf.max(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns non-deterministic values for the specified columns.
   *
   * @since 1.1.0
   * @param cols The input column list
   * @return The result DataFrame
   */
  public DataFrame any_value(Column... cols) {
    return new DataFrame(
        this.rgdf.any_value(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Return the number of rows for each group.
   *
   * @since 1.1.0
   * @return The result DataFrame
   */
  public DataFrame count() {
    return new DataFrame(this.rgdf.count());
  }

  /**
   * Computes the builtin aggregate 'aggName' over the specified columns. Use this function to
   * invoke any aggregates not explicitly listed in this class.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.groupBy("col1").builtin("max", df.col("col2"));
   * }</pre>
   *
   * @since 1.1.0
   * @param aggName the Name of an aggregate function.
   * @param cols a list of function arguments.
   * @return The result DataFrame
   */
  public DataFrame builtin(String aggName, Column... cols) {
    return new DataFrame(
        this.rgdf.builtin(aggName, JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }
}
