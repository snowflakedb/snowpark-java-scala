package com.snowflake.snowpark.internal.analyzer

case class TableDelete(
    tableName: String,
    condition: Option[Expression],
    sourceData: Option[LogicalPlan]
) extends LogicalPlan {
  override def children: Seq[LogicalPlan] =
    if (sourceData.isDefined) {
      Seq(sourceData.get)
    } else Seq.empty

  override protected def analyze: LogicalPlan =
    TableDelete(tableName, condition.map(_.analyze(analyzer.analyze)), sourceData.map(_.analyzed))

  override protected def analyzer: ExpressionAnalyzer =
    ExpressionAnalyzer(sourceData.map(_.aliasMap).getOrElse(Map.empty), dfAliasMap)

  override def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan = {
    val newSource = sourceData.map(func)
    if (sourceData == newSource) this else TableDelete(tableName, condition, newSource)
  }

  lazy override val internalRenamedColumns: Map[String, String] =
    sourceData.map(_.internalRenamedColumns).getOrElse(Map.empty)
}
