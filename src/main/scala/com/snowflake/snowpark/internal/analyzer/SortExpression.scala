package com.snowflake.snowpark.internal.analyzer

private[snowpark] abstract sealed class SortDirection {
  def sql: String
  def defaultNullOrdering: NullOrdering
}

private[snowpark] abstract sealed class NullOrdering {
  def sql: String
}

private[snowpark] case object Ascending extends SortDirection {
  override def sql: String = "ASC"
  override def defaultNullOrdering: NullOrdering = NullsFirst
}

private[snowpark] case object Descending extends SortDirection {
  override def sql: String = "DESC"
  override def defaultNullOrdering: NullOrdering = NullsLast
}

private[snowpark] case object NullsFirst extends NullOrdering {
  override def sql: String = "NULLS FIRST"
}

private[snowpark] case object NullsLast extends NullOrdering {
  override def sql: String = "NULLS LAST"
}

private[snowpark] case class SortOrder(
    child: Expression,
    direction: SortDirection,
    nullOrdering: NullOrdering,
    sameOrderExpressions: Set[Expression]
) extends Expression {
  override def children: Seq[Expression] = child +: sameOrderExpressions.toSeq

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    SortOrder(analyzedChildren.head, direction, nullOrdering, analyzedChildren.tail.toSet)
}

private[snowpark] object SortOrder {
  def apply(
      child: Expression,
      direction: SortDirection,
      sameOrderExpressions: Set[Expression] = Set.empty
  ): SortOrder = {
    new SortOrder(child, direction, direction.defaultNullOrdering, sameOrderExpressions)
  }
}
