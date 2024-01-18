package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.internal.Utils

private[snowpark] trait MultiChildrenNode extends LogicalPlan {
  override def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan = {
    val newChildren: Seq[LogicalPlan] = children.map(func)
    val updated = !newChildren.zip(children).forall {
      case (plan, plan1) => plan == plan1
    }
    if (updated) updateChildren(newChildren) else this
  }

  protected def updateChildren(newChildren: Seq[LogicalPlan]): MultiChildrenNode

  override lazy val dfAliasMap: Map[String, Seq[Attribute]] =
    children.foldLeft(Map.empty[String, Seq[Attribute]]) {
      case (map, child) => Utils.addToDataframeAliasMap(map, child)
    }

  override protected def analyze: LogicalPlan =
    createFromAnalyzedChildren(children.map(_.analyzed))

  protected def createFromAnalyzedChildren: Seq[LogicalPlan] => MultiChildrenNode

  override protected def analyzer: ExpressionAnalyzer =
    ExpressionAnalyzer(children.map(_.aliasMap), dfAliasMap)

  lazy override val internalRenamedColumns: Map[String, String] =
    children.map(_.internalRenamedColumns).reduce(_ ++ _)
}

private[snowpark] case class SimplifiedUnion(override val children: Seq[LogicalPlan])
    extends MultiChildrenNode {
  val sql: String = "UNION"

  override protected def updateChildren(newChildren: Seq[LogicalPlan]): MultiChildrenNode =
    SimplifiedUnion(newChildren)

  override protected def createFromAnalyzedChildren: Seq[LogicalPlan] => MultiChildrenNode =
    SimplifiedUnion
}

private[snowpark] case class SimplifiedUnionAll(override val children: Seq[LogicalPlan])
    extends MultiChildrenNode {
  val sql: String = "UNION ALL"

  override protected def updateChildren(newChildren: Seq[LogicalPlan]): MultiChildrenNode =
    SimplifiedUnionAll(newChildren)

  override protected def createFromAnalyzedChildren: Seq[LogicalPlan] => MultiChildrenNode =
    SimplifiedUnionAll
}
