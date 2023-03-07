package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer.GroupingSetsExpression

/**
 * Constructors of GroupingSets object.
 *
 * @since 0.4.0
 */
object GroupingSets {

  /**
   * Creates a GroupingSets object from a list of column/expression sets.
   *
   * @param set a set of DataFrame column, or any expression in the current scope.
   * @param sets a list of arguments except the first one
   * @since 0.4.0
   */
  def apply(set: Set[Column], sets: Set[Column]*): GroupingSets =
    new GroupingSets(set +: sets)
}

/**
 * A Container of grouping sets that you pass to
 * [[DataFrame.groupByGroupingSets(groupingSets* DataFrame.groupByGroupingSets]].
 *
 * @param sets a list of grouping sets
 * @since 0.4.0
 */
case class GroupingSets(sets: Seq[Set[Column]]) {
  private[snowpark] val toExpression = GroupingSetsExpression(sets.map(_.map(_.expr)))
}
