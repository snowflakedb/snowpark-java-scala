package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.SaveMode

case class SnowflakeCreateTable(tableName: String, mode: SaveMode, query: Option[LogicalPlan])
    extends LogicalPlan {
  override def children: Seq[LogicalPlan] = query.toSeq

  override protected def analyze: LogicalPlan =
    SnowflakeCreateTable(tableName, mode, query.map(_.analyzed))

  override protected val analyzer: ExpressionAnalyzer =
    ExpressionAnalyzer(query.map(_.aliasMap).getOrElse(Map.empty), dfAliasMap)

  override def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan = {
    val newQuery = query.map(func)
    if (newQuery == query) this else SnowflakeCreateTable(tableName, mode, newQuery)
  }

  lazy override val internalRenamedColumns: Map[String, String] =
    query.map(_.internalRenamedColumns).getOrElse(Map.empty)
}

object SnowflakeCreateTable {
  def apply(tableName: String, mode: SaveMode, query: Option[LogicalPlan]): SnowflakeCreateTable =
    new SnowflakeCreateTable(tableName, mode, query)
}
