package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer.UnspecifiedFrame

/**
 * Contains functions to form [[WindowSpec]].
 *
 * @since 0.1.0
 */
object Window {

  /**
   * Returns [[WindowSpec]] object with partition by clause.
   * @since 0.1.0
   */
  def partitionBy(cols: Column*): WindowSpec =
    spec.partitionBy(cols: _*)

  /**
   * Returns [[WindowSpec]] object with order by clause.
   * @since 0.1.0
   */
  def orderBy(cols: Column*): WindowSpec =
    spec.orderBy(cols: _*)

  /**
   * Returns a value representing unbounded preceding.
   * @since 0.1.0
   */
  def unboundedPreceding: Long = Long.MinValue

  /**
   * Returns a value representing unbounded following.
   * @since 0.1.0
   */
  def unboundedFollowing: Long = Long.MaxValue

  /**
   * Returns a value representing current row.
   * @since 0.1.0
   */
  def currentRow: Long = 0

  /**
   * Returns [[WindowSpec]] object with row frame clause.
   * @since 0.1.0
   */
  def rowsBetween(start: Long, end: Long): WindowSpec =
    spec.rowsBetween(start, end)

  /**
   * Returns [[WindowSpec]] object with range frame clause.
   * @since 0.1.0
   */
  def rangeBetween(start: Long, end: Long): WindowSpec =
    spec.rangeBetween(start, end)

  private[snowpark] def spec: WindowSpec = {
    new WindowSpec(Seq.empty, Seq.empty, UnspecifiedFrame)
  }
}
