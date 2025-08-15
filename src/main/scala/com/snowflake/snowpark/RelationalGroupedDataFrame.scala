package com.snowflake.snowpark

import java.util.Locale

import com.snowflake.snowpark.RelationalGroupedDataFrame.GroupType
import com.snowflake.snowpark.internal.ErrorMessage
import com.snowflake.snowpark.internal.analyzer._

import scala.reflect.ClassTag

private[snowpark] object RelationalGroupedDataFrame {

  private[snowpark] def apply(
      df: DataFrame,
      groupingExprs: Seq[Expression],
      groupType: GroupType): RelationalGroupedDataFrame =
    new RelationalGroupedDataFrame(df, groupingExprs, groupType)

  sealed trait GroupType {
    override def toString: String = getClass.getSimpleName.stripSuffix("$").stripSuffix("Type")
  }

  object GroupByType extends GroupType

  object GroupByGroupingSetsType extends GroupType

  object CubeType extends GroupType

  object RollupType extends GroupType

  case class PivotType(pivotCol: Expression, values: Seq[Expression]) extends GroupType

}

/**
 * Represents an underlying DataFrame with rows that are grouped by common values. Can be used to
 * define aggregations on these grouped DataFrames.
 *
 * Example:
 * {{{
 *   val groupedDf: RelationalGroupedDataFrame = df.groupBy("dept")
 *   val aggDf: DataFrame = groupedDf.agg(groupedDf("salary") -> "mean")
 * }}}
 *
 * The methods [[DataFrame.groupBy(cols:Array[String* DataFrame.groupBy]],
 * [[DataFrame.cube(cols:Seq[String* DataFrame.cube]] and
 * [[DataFrame.rollup(cols:Array[String* DataFrame.rollup]] return an instance of type
 * [[RelationalGroupedDataFrame]]
 *
 * @since 0.1.0
 */
class RelationalGroupedDataFrame private[snowpark] (
    dataFrame: DataFrame,
    private[snowpark] val groupingExprs: Seq[Expression],
    private[snowpark] val groupType: GroupType) {

  private[this] def toDF(aggExprs: Seq[Expression]): DataFrame = {
    val aliasedAgg = (groupingExprs.flatMap {
      case GroupingSetsExpression(args) =>
        args.flatMap(_.toSeq).distinct // all columns in grouping sets
      case other => Some(other)
    } ++ aggExprs).distinct.map(alias)

    groupType match {
      case RelationalGroupedDataFrame.GroupByType |
          RelationalGroupedDataFrame.GroupByGroupingSetsType =>
        DataFrame(dataFrame.session, Aggregate(groupingExprs, aliasedAgg, dataFrame.plan))
      case RelationalGroupedDataFrame.RollupType =>
        DataFrame(
          dataFrame.session,
          Aggregate(Seq(Rollup(groupingExprs)), aliasedAgg, dataFrame.plan))
      case RelationalGroupedDataFrame.CubeType =>
        DataFrame(
          dataFrame.session,
          Aggregate(Seq(Cube(groupingExprs)), aliasedAgg, dataFrame.plan))
      case RelationalGroupedDataFrame.PivotType(pivotCol, values) =>
        if (aggExprs.size != 1) {
          throw ErrorMessage.DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR()
        }
        DataFrame(dataFrame.session, Pivot(pivotCol, values, aggExprs, dataFrame.plan))
    }
  }

  private[this] def alias(expr: Expression): NamedExpression = expr match {
    case u: UnresolvedAttribute => UnresolvedAlias(u)
    case expr: NamedExpression => expr
    case expr: Expression =>
      Alias(expr, stripInvalidSnowflakeIdentifierChars(expr.sql.toUpperCase(Locale.ROOT)))
  }

  private[this] def stripInvalidSnowflakeIdentifierChars(identifier: String): String = {
    identifier.replaceAll("\"", "").replaceAll("[^\\x20-\\x7E]", "")
  }

  private[this] def strToExpr(expr: String): Expression => Expression = {
    // todo: redesign this logic in SNOW-175294
    val exprToFunc: Expression => Expression = { (inputExpr: Expression) =>
      expr.toLowerCase(Locale.ROOT) match {
        // We special handle a few cases that have alias that are not in function registry.
        case "avg" | "average" | "mean" => functions.avg(Column(inputExpr)).expr
        case "stddev" | "std" => functions.stddev(Column(inputExpr)).expr
        case "count" | "size" => functions.count(Column(inputExpr)).expr
        case name => functions.builtin(name)(inputExpr).expr
      }
    }
    (inputExpr: Expression) => exprToFunc(inputExpr)
  }

  /**
   * Returns a DataFrame with computed aggregates. The first element of the 'expr' pair is the
   * column to aggregate and the second element is the aggregate function to compute. The following
   * example computes the mean of the price column and the sum of the sales column. The name of the
   * aggregate function to compute must be a valid Snowflake
   * [[https://docs.snowflake.com/en/sql-reference/functions-aggregation.html aggregate function]]
   * "average" and "mean" can be used to specify "avg".
   *
   * {{{
   *   import com.snowflake.snowpark.functions.col
   *   df.groupBy("itemType").agg(
   *     col("price") -> "mean",
   *     col("sales") -> "sum")
   * }}}
   *
   * @return
   *   a [[DataFrame]]
   * @since 0.1.0
   */
  def agg(expr: (Column, String), exprs: (Column, String)*): DataFrame = transformation("agg") {
    agg(expr +: exprs)
  }

  /**
   * Returns a DataFrame with computed aggregates. The first element of the 'expr' pair is the
   * column to aggregate and the second element is the aggregate function to compute. The following
   * example computes the mean of the price column and the sum of the sales column. The name of the
   * aggregate function to compute must be a valid Snowflake
   * [[https://docs.snowflake.com/en/sql-reference/functions-aggregation.html aggregate function]]
   * "average" and "mean" can be used to specify "avg".
   *
   * {{{
   *   import com.snowflake.snowpark.functions.col
   *   df.groupBy("itemType").agg(Seq(
   *     col("price") -> "mean",
   *     col("sales") -> "sum"))
   * }}}
   *
   * @return
   *   a [[DataFrame]]
   * @since 0.2.0
   */
  def agg(exprs: Seq[(Column, String)]): DataFrame = transformation("agg") {
    toDF(exprs.map { case (col, expr) => strToExpr(expr)(col.expr) })
  }

  /**
   * Returns a DataFrame with aggregated computed according to the supplied [[Column]] expressions.
   * [[com.snowflake.snowpark.functions]] contains some built-in aggregate functions that can be
   * used.
   *
   * {{{
   *   impoer com.snowflake.snowpark.functions._
   *   df.groupBy("itemType").agg(
   *     mean($"price"),
   *     sum($"sales"))
   * }}}
   *
   * @return
   *   a [[DataFrame]]
   * @since 0.1.0
   */
  def agg(expr: Column, exprs: Column*): DataFrame = transformation("agg") {
    agg(expr +: exprs)
  }

  /**
   * Returns a DataFrame with aggregated computed according to the supplied [[Column]] expressions.
   * [[com.snowflake.snowpark.functions]] contains some built-in aggregate functions that can be
   * used.
   *
   * {{{
   *   impoer com.snowflake.snowpark.functions._
   *   df.groupBy("itemType").agg(Seq(
   *     mean($"price"),
   *     sum($"sales")))
   * }}}
   *
   * @return
   *   a [[DataFrame]]
   * @since 0.2.0
   */
  def agg[T: ClassTag](exprs: Seq[Column]): DataFrame = transformation("agg") {
    toDF(exprs.map(_.expr))
  }

  /**
   * Returns a DataFrame with aggregated computed according to the supplied [[Column]] expressions.
   * [[com.snowflake.snowpark.functions]] contains some built-in aggregate functions that can be
   * used.
   *
   * @return
   *   a [[DataFrame]]
   * @since 0.9.0
   */
  def agg(exprs: Array[Column]): DataFrame = transformation("agg") {
    agg(exprs.toSeq)
  }

  /**
   * Returns a DataFrame with computed aggregates. The first element of the 'expr' pair is the
   * column to aggregate and the second element is the aggregate function to compute. The following
   * example computes the mean of the price column and the sum of the sales column. The name of the
   * aggregate function to compute must be a valid Snowflake
   * [[https://docs.snowflake.com/en/sql-reference/functions-aggregation.html aggregate function]]
   * "average" and "mean" can be used to specify "avg".
   *
   * {{{
   *   import com.snowflake.snowpark.functions.col
   *   df.groupBy("itemType").agg(Map(
   *   col("price") -> "mean",
   *   col("sales") -> "sum"
   *   ))
   * }}}
   *
   * @return
   *   a [[DataFrame]]
   * @since 0.1.0
   */
  def agg(exprs: Map[Column, String]): DataFrame = transformation("agg") {
    toDF(exprs.map { case (col, expr) =>
      strToExpr(expr)(col.expr)
    }.toSeq)
  }

  /**
   * Return the average for the specified numeric columns.
   *
   * @since 0.4.0
   * @return
   *   a [[DataFrame]]
   */
  def avg(cols: Column*): DataFrame = transformation("avg") {
    nonEmptyArgumentFunction("avg", cols)
  }

  /**
   * Return the average for the specified numeric columns. Alias of avg
   *
   * @since 0.4.0
   * @return
   *   a [[DataFrame]]
   */
  def mean(cols: Column*): DataFrame = transformation("mean") {
    avg(cols: _*)
  }

  /**
   * Return the sum for the specified numeric columns.
   *
   * @since 0.1.0
   * @return
   *   a [[DataFrame]]
   */
  def sum(cols: Column*): DataFrame = transformation("sum") {
    nonEmptyArgumentFunction("sum", cols)
  }

  /**
   * Return the median for the specified numeric columns.
   *
   * @since 0.5.0
   * @return
   *   A [[DataFrame]]
   */
  def median(cols: Column*): DataFrame = transformation("median") {
    nonEmptyArgumentFunction("median", cols)
  }

  /**
   * Return the min for the specified numeric columns.
   *
   * @since 0.1.0
   * @return
   *   A [[DataFrame]]
   */
  def min(cols: Column*): DataFrame = transformation("min") {
    nonEmptyArgumentFunction("min", cols)
  }

  /**
   * Return the max for the specified numeric columns.
   *
   * @since 0.4.0
   * @return
   *   A [[DataFrame]]
   */
  def max(cols: Column*): DataFrame = transformation("max") {
    nonEmptyArgumentFunction("max", cols)
  }

  /**
   * Returns non-deterministic values for the specified columns.
   *
   * @since 0.12.0
   * @return
   *   A [[DataFrame]]
   */
  def any_value(cols: Column*): DataFrame = transformation("any_value") {
    nonEmptyArgumentFunction("any_value", cols)
  }

  /**
   * Return the number of rows for each group.
   *
   * @since 0.1.0
   * @return
   *   A [[DataFrame]]
   */
  def count(): DataFrame = transformation("count") {
    toDF(Seq(Alias(functions.builtin("count")(Literal(1)).expr, "count")))
  }

  /**
   * Computes the builtin aggregate 'aggName' over the specified columns. Use this function to
   * invoke any aggregates not explicitly listed in this class.
   *
   * For example:
   * {{{
   *   df.groupBy(col("a")).builtin("max")(col("b"))
   * }}}
   *
   * @since 0.6.0
   * @param aggName
   *   the Name of an aggregate function.
   * @return
   *   A [[DataFrame]]
   */
  def builtin(aggName: String)(cols: Column*): DataFrame = transformation("builtin") {
    toDF(cols.map(_.expr).map(expr => functions.builtin(aggName)(expr).expr))
  }

  private def nonEmptyArgumentFunction(funcName: String, cols: Seq[Column]): DataFrame = {
    if (cols.isEmpty) {
      throw ErrorMessage.DF_FUNCTION_ARGS_CANNOT_BE_EMPTY(funcName)
    } else {
      builtin(funcName)(cols: _*)
    }
  }

  @inline private def transformation(funcName: String)(func: => DataFrame): DataFrame = {
    val typeName = groupType.toString.head.toLower + groupType.toString.tail
    val name = s"$typeName.$funcName"
    DataFrame.buildMethodChain(dataFrame.methodChain, name)(func)
  }

}
