package com.snowflake.snowpark.internal.analyzer

import java.util.Locale

import com.snowflake.snowpark.internal.ErrorMessage

private[snowpark] abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan
  def sql: String
  override def children: Seq[LogicalPlan] = Seq(left, right)

  lazy protected val analyzedLeft: LogicalPlan = left.analyzed
  lazy protected val analyzedRight: LogicalPlan = right.analyzed

  lazy override protected val analyzer: ExpressionAnalyzer =
    ExpressionAnalyzer(left.aliasMap, right.aliasMap, dfAliasMap)

  addToDataframeAliasMap(left.dfAliasMap)
  addToDataframeAliasMap(right.dfAliasMap)
  override def analyze: LogicalPlan =
    createFromAnalyzedChildren(analyzedLeft, analyzedRight)

  protected def createFromAnalyzedChildren: (LogicalPlan, LogicalPlan) => LogicalPlan

  override def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan = {
    val newLeft = func(left)
    val newRight = func(right)
    if (left == newLeft && right == newRight) {
      this
    } else {
      updateChildren(newLeft, newRight)
    }
  }

  protected val updateChildren: (LogicalPlan, LogicalPlan) => LogicalPlan

  lazy override val internalRenamedColumns: Map[String, String] =
    left.internalRenamedColumns ++ right.internalRenamedColumns

}

// used when checking if it is set operation in result scan
private[snowpark] abstract class SetOperation extends BinaryNode

private[snowpark] case class Except(left: LogicalPlan, right: LogicalPlan) extends SetOperation {
  override def sql: String = "EXCEPT"

  override protected def createFromAnalyzedChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    Except

  override protected val updateChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    Except
}

private[snowpark] case class Intersect(left: LogicalPlan, right: LogicalPlan)
    extends SetOperation {
  override def sql: String = "INTERSECT"

  override protected def createFromAnalyzedChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    Intersect

  override protected val updateChildren: (LogicalPlan, LogicalPlan) => LogicalPlan = Intersect
}

private[snowpark] case class Union(left: LogicalPlan, right: LogicalPlan) extends SetOperation {
  override def sql: String = "UNION"

  override protected def createFromAnalyzedChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    Union

  override protected val updateChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    Union
}

private[snowpark] case class UnionAll(left: LogicalPlan, right: LogicalPlan)
    extends SetOperation {
  override def sql: String = "UNION ALL"

  override protected def createFromAnalyzedChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    UnionAll

  override protected val updateChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    UnionAll
}

private[snowpark] object JoinType {
  def apply(joinType: String): JoinType =
    joinType.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "inner" => Inner
      case "outer" | "full" | "fullouter" => FullOuter
      case "leftouter" | "left" => LeftOuter
      case "rightouter" | "right" => RightOuter
      case "leftsemi" | "semi" => LeftSemi
      case "leftanti" | "anti" => LeftAnti
      case "cross" => Cross
      case _ =>
        val supported = Seq(
          "inner",
          "outer",
          "full",
          "fullouter",
          "full_outer",
          "leftouter",
          "left",
          "left_outer",
          "rightouter",
          "right",
          "right_outer",
          "leftsemi",
          "left_semi",
          "semi",
          "leftanti",
          "left_anti",
          "anti",
          "cross")

        throw ErrorMessage.DF_JOIN_INVALID_JOIN_TYPE(joinType, supported.mkString(", "))
    }
}

private[snowpark] trait JoinType {
  def sql: String
}

private[snowpark] case object Inner extends JoinType {
  override def sql: String = "INNER"
}

private[snowpark] case object Cross extends JoinType {
  override def sql: String = "CROSS"
}

private[snowpark] case object LeftOuter extends JoinType {
  override def sql: String = "LEFT OUTER"
}

private[snowpark] case object RightOuter extends JoinType {
  override def sql: String = "RIGHT OUTER"
}

private[snowpark] case object FullOuter extends JoinType {
  override def sql: String = "FULL OUTER"
}

private[snowpark] case object LeftSemi extends JoinType {
  override def sql: String = "LEFT SEMI"
}

private[snowpark] case object LeftAnti extends JoinType {
  override def sql: String = "LEFT ANTI"
}

private[snowpark] case class NaturalJoin(joinType: JoinType) extends JoinType {
  val naturalJoins = Seq(Inner, LeftOuter, RightOuter, FullOuter)
  if (!naturalJoins.contains(joinType)) {
    throw ErrorMessage.DF_JOIN_INVALID_NATURAL_JOIN_TYPE(s"$joinType")
  }
  override def sql: String = "NATURAL " + joinType.sql
}

private[snowpark] case class UsingJoin(joinType: JoinType, usingColumns: Seq[String])
    extends JoinType {
  val usingJoins = Seq(Inner, LeftOuter, LeftSemi, RightOuter, FullOuter, LeftAnti)
  if (!usingJoins.contains(joinType)) {
    throw ErrorMessage.DF_JOIN_INVALID_USING_JOIN_TYPE(s"$joinType")
  }
  override def sql: String = "USING " + joinType.sql
}

private[snowpark] case class Join(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression])
    extends BinaryNode {
  override def sql: String = joinType.sql

  override protected def createFromAnalyzedChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    Join(_, _, joinType, condition.map(_.analyze(analyzer.analyze)))

  override protected val updateChildren: (LogicalPlan, LogicalPlan) => LogicalPlan =
    Join(_, _, joinType, condition)
}
