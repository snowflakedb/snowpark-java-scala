package com.snowflake.snowpark.internal.analyzer

private[snowpark] trait SpecialFrameBoundary extends Expression {
  def sql: String
  override val children: Seq[Expression] = Seq.empty

  // do not use this function, override analyze function directly
  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def analyze(func: Expression => Expression): Expression = func(this)
}

private[snowpark] case object UnboundedPreceding extends SpecialFrameBoundary {
  override def sql: String = "UNBOUNDED PRECEDING"
}

private[snowpark] case object UnboundedFollowing extends SpecialFrameBoundary {
  override def sql: String = "UNBOUNDED FOLLOWING"
}

private[snowpark] case object CurrentRow extends SpecialFrameBoundary {
  override def sql: String = "CURRENT ROW"
}

private[snowpark] sealed trait FrameType {
  def sql: String
}

private[snowpark] case object RowFrame extends FrameType {
  override def sql: String = "ROWS"
}

private[snowpark] case object RangeFrame extends FrameType {
  override def sql: String = "RANGE"
}

private[snowpark] trait WindowFrame extends Expression

private[snowpark] case object UnspecifiedFrame extends WindowFrame {
  override def children: Seq[Expression] = Seq.empty

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    this
}

private[snowpark] case class SpecifiedWindowFrame(
    frameType: FrameType,
    lower: Expression,
    upper: Expression)
    extends WindowFrame {
  override def children: Seq[Expression] = Seq(lower, upper)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    SpecifiedWindowFrame(frameType, analyzedChildren.head, analyzedChildren(1))
}

private[snowpark] case class WindowExpression(
    windowFunction: Expression,
    windowSpec: WindowSpecDefinition)
    extends Expression {
  override def children: Seq[Expression] = Seq(windowFunction, windowSpec)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    WindowExpression(analyzedChildren.head, analyzedChildren(1).asInstanceOf[WindowSpecDefinition])
}

private[snowpark] case class WindowSpecDefinition(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frameSpecification: WindowFrame)
    extends Expression {
  override def children: Seq[Expression] =
    partitionSpec ++ orderSpec :+ frameSpecification

  // do not use this function, override analyze function directly
  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def analyze(func: Expression => Expression): Expression = {
    val analyzedPartitionSpec = partitionSpec.map(_.analyze(func))
    val analyzedOrderSpec = orderSpec.map(_.analyze(func))
    val analyzedFrameSpecification = frameSpecification.analyze(func)
    if (analyzedOrderSpec == orderSpec &&
      analyzedPartitionSpec == partitionSpec &&
      analyzedFrameSpecification == frameSpecification) {
      func(this)
    } else {
      func(
        WindowSpecDefinition(
          analyzedPartitionSpec,
          analyzedOrderSpec.map(_.asInstanceOf[SortOrder]),
          analyzedFrameSpecification.asInstanceOf[WindowFrame]))
    }
  }
}
