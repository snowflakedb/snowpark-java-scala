package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.internal.ErrorMessage

/** Represents a window frame clause.
  * @since 0.1.0
  */
class WindowSpec private[snowpark] (
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frame: WindowFrame) {

  /** Returns a new [[WindowSpec]] object with the new partition by clause.
    * @since 0.1.0
    */
  def partitionBy(cols: Column*): WindowSpec = new WindowSpec(cols.map(_.expr), orderSpec, frame)

  /** Returns a new [[WindowSpec]] object with the new order by clause.
    * @since 0.1.0
    */
  def orderBy(cols: Column*): WindowSpec = {
    val sortOrder: Seq[SortOrder] = cols.map { col =>
      col.expr match {
        case expr: SortOrder => expr
        case expr: Expression => SortOrder(expr, Ascending)
      }
    }
    new WindowSpec(partitionSpec, sortOrder, frame)
  }

  /** Returns a new [[WindowSpec]] object with the new row frame clause.
    * @since 0.1.0
    */
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => CurrentRow
      case Long.MinValue => UnboundedPreceding
      case x if Int.MinValue <= x && x <= Int.MaxValue => Literal(x.toInt)
      case x => throw ErrorMessage.DF_WINDOW_BOUNDARY_START_INVALID(x)
    }

    val boundaryEnd = end match {
      case 0 => CurrentRow
      case Long.MaxValue => UnboundedFollowing
      case x if Int.MinValue <= x && x <= Int.MaxValue => Literal(x.toInt)
      case x => throw ErrorMessage.DF_WINDOW_BOUNDARY_END_INVALID(x)
    }

    new WindowSpec(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(RowFrame, boundaryStart, boundaryEnd))
  }

  /** Returns a new [[WindowSpec]] object with the new range frame clause.
    * @since 0.1.0
    */
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => CurrentRow
      case Long.MinValue => UnboundedPreceding
      case x => Literal(x)
    }

    val boundaryEnd = end match {
      case 0 => CurrentRow
      case Long.MaxValue => UnboundedFollowing
      case x => Literal(x)
    }

    new WindowSpec(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(RangeFrame, boundaryStart, boundaryEnd))
  }

  private[snowpark] def withAggregate(aggregate: Expression): Column =
    new Column(WindowExpression(aggregate, getWindowSpecDefinition))

  private[snowpark] def getWindowSpecDefinition: WindowSpecDefinition =
    WindowSpecDefinition(partitionSpec, orderSpec, frame)
}
