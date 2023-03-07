package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;

/**
 * Represents a window frame clause.
 *
 * @since 0.9.0
 */
public class WindowSpec {
  private final com.snowflake.snowpark.WindowSpec scalaWindowSpec;

  WindowSpec(com.snowflake.snowpark.WindowSpec ws) {
    this.scalaWindowSpec = ws;
  }

  /**
   * Returns a new WindowSpec object with the new partition by clause.
   *
   * @since 0.9.0
   * @param cols A list of input column
   * @return The result WindowSpec
   */
  public WindowSpec partitionBy(Column... cols) {
    return new WindowSpec(
        this.scalaWindowSpec.partitionBy(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns a new WindowSpec object with the new order by clause.
   *
   * @since 0.9.0
   * @param cols A list of input column
   * @return The result WindowSpec
   */
  public WindowSpec orderBy(Column... cols) {
    return new WindowSpec(
        this.scalaWindowSpec.orderBy(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns a new WindowSpec object with the new row frame clause.
   *
   * @since 0.9.0
   * @param start The starting index
   * @param end The ending index
   * @return The result WindowSpec
   */
  public WindowSpec rowsBetween(long start, long end) {
    return new WindowSpec(this.scalaWindowSpec.rowsBetween(start, end));
  }

  /**
   * Returns a new WindowSpec object with the new range frame clause.
   *
   * @since 0.9.0
   * @param start The starting index
   * @param end The ending index
   * @return The result WindowSpec
   */
  public WindowSpec rangeBetween(long start, long end) {
    return new WindowSpec(this.scalaWindowSpec.rangeBetween(start, end));
  }

  com.snowflake.snowpark.WindowSpec toScalaWindowSpec() {
    return scalaWindowSpec;
  }
}
