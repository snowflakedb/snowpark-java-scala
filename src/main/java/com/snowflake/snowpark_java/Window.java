package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;

/**
 * Contains functions to form WindowSpec.
 *
 * @since 0.9.0
 */
public final class Window {

  // lock constructor
  private Window() {}

  /**
   * Returns WindowSpec object with partition by clause.
   *
   * @since 0.9.0
   * @param cols The list of column
   * @return The result WindowSpec
   */
  public static WindowSpec partitionBy(Column... cols) {
    return new WindowSpec(
        com.snowflake.snowpark.Window.partitionBy(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns WindowSpec object with order by clause.
   *
   * @since 0.9.0
   * @param cols The list of column
   * @return The result WindowSpec
   */
  public static WindowSpec orderBy(Column... cols) {
    return new WindowSpec(
        com.snowflake.snowpark.Window.orderBy(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns a value representing unbounded preceding.
   *
   * @since 0.9.0
   * @return A long value
   */
  public static long unboundedPreceding() {
    return com.snowflake.snowpark.Window.unboundedPreceding();
  }

  /**
   * Returns a value representing unbounded following.
   *
   * @since 0.9.0
   * @return A long value
   */
  public static long unboundedFollowing() {
    return com.snowflake.snowpark.Window.unboundedFollowing();
  }

  /**
   * Returns a value representing current row.
   *
   * @since 0.9.0
   * @return A long value
   */
  public static long currentRow() {
    return com.snowflake.snowpark.Window.currentRow();
  }

  /**
   * Returns WindowSpec object with row frame clause.
   *
   * @since 0.9.0
   * @param start The start position
   * @param end The end position
   * @return The result WindowSpec
   */
  public static WindowSpec rowsBetween(long start, long end) {
    return new WindowSpec(com.snowflake.snowpark.Window.rowsBetween(start, end));
  }

  /**
   * Returns WindowSpec object with range frame clause.
   *
   * @since 0.9.0
   * @param start The start position
   * @param end The end position
   * @return The result WindowSpec
   */
  public static WindowSpec rangeBetween(long start, long end) {
    return new WindowSpec(com.snowflake.snowpark.Window.rangeBetween(start, end));
  }
}
