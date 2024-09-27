package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.internal.{ErrorMessage, Utils}
import com.snowflake.snowpark.Row

private[snowpark] trait LogicalPlan {
  def children: Seq[LogicalPlan] = Seq.empty

  // create a new plan by updating children
  // used by simplifier
  def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan

  // analyze functions
  // aliasMap is updated during analyzing, so have to return these two together
  lazy val (analyzed, aliasMap): (LogicalPlan, Map[ExprId, String]) = {
    val analyzedPlan: LogicalPlan = analyze
    analyzedPlan.setSourcePlan(this)
    (analyzedPlan, analyzer.getAliasMap)
  }

  lazy val dfAliasMap: Map[String, Seq[Attribute]] = Map.empty

  protected def analyze: LogicalPlan
  protected def analyzer: ExpressionAnalyzer

  // cache the SnowflakePlan once a plan node has been converted to it.
  // use SnowflakePlan to analyze result schema
  private var snowflakePlan: Option[SnowflakePlan] = None

  // The staging plan nodes of analyzer and optimizer may be converted from Raw plan tree.
  // keep a reference to the source node where this plan node was converted from.
  // then cache the SnowflakePlan in the source node instead of this staging plan,
  // since all the staging nodes are one time use and will be purged soon.
  private var sourcePlan: Option[LogicalPlan] = None

  def setSourcePlan(plan: LogicalPlan): Unit = {
    if (!plan.eq(this) && sourcePlan.isEmpty) {
      this.sourcePlan = Option(plan)
    }
  }

  def setSnowflakePlan(plan: SnowflakePlan): Unit = sourcePlan match {
    case Some(sp) => sp.setSnowflakePlan(plan)
    case _ => snowflakePlan = Option(plan)
  }

  def getSnowflakePlan: Option[SnowflakePlan] = sourcePlan match {
    case Some(sp) => sp.getSnowflakePlan
    case _ => snowflakePlan
  }

  def getOrUpdateSnowflakePlan(func: => SnowflakePlan): SnowflakePlan =
    getSnowflakePlan.getOrElse {
      setSnowflakePlan(func)
      getSnowflakePlan.get
    }

  protected lazy val planName: String =
    this.getClass.getSimpleName.stripSuffix("$")

  // generate a String to summarize the content of this logical plan
  // the result string is used by telemetry to analyze the performance
  // of simplifier. This summary contains the name of logical plan,
  // doesn't have any user data like values and column names.
  private[snowpark] def summarize: String =
    children.map(_.summarize).mkString(s"$planName(", ",", ")")

  // Cash all internal renames. New name to original name mapping
  val internalRenamedColumns: Map[String, String]
}

private[snowpark] trait LeafNode extends LogicalPlan {
  // create ExpressionAnalyzer with empty alias map
  override protected val analyzer: ExpressionAnalyzer = ExpressionAnalyzer()

  // leaf node doesn't have child
  override def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan = this

  override val internalRenamedColumns: Map[String, String] = Map.empty
}

case class TableFunctionRelation(tableFunction: TableFunctionExpression) extends LeafNode {
  override protected def analyze: LogicalPlan =
    TableFunctionRelation(
      tableFunction.analyze(analyzer.analyze).asInstanceOf[TableFunctionExpression])
}

private[snowpark] case class Range(start: Long, end: Long, step: Long) extends LeafNode {
  if (step == 0) {
    throw ErrorMessage.DF_RANGE_STEP_CANNOT_BE_ZERO()
  }

  override protected def analyze: LogicalPlan = this
}

private[snowpark] case class Generator(exprs: Seq[Expression], rowCount: Long) extends LeafNode {
  if (exprs.isEmpty) {
    throw ErrorMessage.DF_COLUMN_LIST_OF_GENERATOR_CANNOT_BE_EMPTY()
  }

  override protected def analyze: LogicalPlan =
    Generator(exprs.map(_.analyze(analyzer.analyze)), rowCount)
}

private[snowpark] case class UnresolvedRelation(name: String) extends LeafNode {
  override protected def analyze: LogicalPlan = this
}

private[snowpark] case class StoredProcedureRelation(spName: String, args: Seq[Expression])
    extends LeafNode {
  override protected def analyze: LogicalPlan = this
}

private[snowpark] case class SnowflakeValues(output: Seq[Attribute], data: Seq[Row])
    extends LeafNode {
  override protected def analyze: LogicalPlan =
    SnowflakeValues(output.map(_.analyze(analyzer.analyze).asInstanceOf[Attribute]), data)
}

private[snowpark] case class CopyIntoNode(
    tableName: String,
    columnNames: Seq[String],
    transformations: Seq[Expression],
    options: Map[String, Any],
    stagedFileReader: StagedFileReader)
    extends LeafNode {
  override protected def analyze: LogicalPlan =
    CopyIntoNode(
      tableName,
      columnNames,
      transformations.map(_.analyze(analyzer.analyze)),
      options,
      stagedFileReader)
}

private[snowpark] trait UnaryNode extends LogicalPlan {
  def child: LogicalPlan
  override final def children: Seq[LogicalPlan] = child :: Nil

  lazy protected val analyzedChild: LogicalPlan = child.analyzed
  // create expression analyzer from child's alias map
  lazy override protected val analyzer: ExpressionAnalyzer =
    ExpressionAnalyzer(child.aliasMap, dfAliasMap)

  override lazy val dfAliasMap: Map[String, Seq[Attribute]] = child.dfAliasMap
  override protected def analyze: LogicalPlan = createFromAnalyzedChild(analyzedChild)

  protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan

  override def updateChildren(func: LogicalPlan => LogicalPlan): LogicalPlan = {
    val newChild = func(child)
    // only create new plan when child updated,
    // otherwise optimizer will be dead locked.
    if (newChild != child) updateChild(newChild) else this
  }

  // compared to createFromAnalyzedChild,
  // this function only updates child but not modifies
  // any expression.
  protected def updateChild: LogicalPlan => LogicalPlan

  override val internalRenamedColumns: Map[String, String] = child.internalRenamedColumns
}

/**
 * Plan Node to sample some rows from a DataFrame. Either a fraction or a row number needs to be
 * specified.
 *
 * @param probabilityFraction
 *   the sampling fraction(0.0 - 1.0)
 * @param rowCount
 *   the sampling row count
 * @param child
 *   the LogicalPlan
 */
private[snowpark] case class SnowflakeSampleNode(
    probabilityFraction: Option[Double],
    rowCount: Option[Long],
    child: LogicalPlan)
    extends UnaryNode {
  if ((probabilityFraction.isEmpty && rowCount.isEmpty) ||
    (probabilityFraction.isDefined && rowCount.isDefined)) {
    throw ErrorMessage.PLAN_SAMPLING_NEED_ONE_PARAMETER()
  }

  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    SnowflakeSampleNode(probabilityFraction, rowCount, _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    createFromAnalyzedChild
}

private[snowpark] case class Sort(order: Seq[SortOrder], child: LogicalPlan) extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    Sort(order.map(_.analyze(analyzer.analyze).asInstanceOf[SortOrder]), _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    Sort(order, _)
}

// requires childOutput when creating,
// since child's SnowflakePlan can be empty
private[snowpark] case class DataframeAlias(
    alias: String,
    child: LogicalPlan,
    childOutput: Seq[Attribute])
    extends UnaryNode {

  override lazy val dfAliasMap: Map[String, Seq[Attribute]] =
    Utils.addToDataframeAliasMap(Map(alias -> childOutput), child)
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    DataframeAlias(alias, _, childOutput)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    createFromAnalyzedChild
}

private[snowpark] case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    Aggregate(
      groupingExpressions.map(_.analyze(analyzer.analyze)),
      aggregateExpressions.map(_.analyze(analyzer.analyze).asInstanceOf[NamedExpression]),
      _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    Aggregate(groupingExpressions, aggregateExpressions, _)
}

private[snowpark] case class Pivot(
    pivotColumn: Expression,
    pivotValues: Seq[Expression],
    aggregates: Seq[Expression],
    child: LogicalPlan)
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    Pivot(
      pivotColumn.analyze(analyzer.analyze),
      pivotValues.map(_.analyze(analyzer.analyze)),
      aggregates.map(_.analyze(analyzer.analyze)),
      _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    Pivot(pivotColumn, pivotValues, aggregates, _)
}

private[snowpark] case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    Filter(condition.analyze(analyzer.analyze), _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    Filter(condition, _)
}

private[snowpark] case class Project(
    projectList: Seq[NamedExpression],
    child: LogicalPlan,
    override val internalRenamedColumns: Map[String, String])
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    Project(
      projectList.map(_.analyze(analyzer.analyze).asInstanceOf[NamedExpression]),
      _,
      internalRenamedColumns)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    Project(projectList, _, internalRenamedColumns)
}

private[snowpark] object Project {
  def apply(projectList: Seq[NamedExpression], child: LogicalPlan): Project = {
    val renamedColumns: Map[String, String] = {
      projectList.flatMap {
        case Alias(child: Attribute, name, true) => Some(name -> child.name)
        case _ => None
      }.toMap ++ child.internalRenamedColumns
    }
    Project(projectList, child, renamedColumns)
  }
}

private[snowpark] case class ProjectAndFilter(
    projectList: Seq[NamedExpression],
    condition: Expression,
    child: LogicalPlan)
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    ProjectAndFilter(
      projectList.map(_.analyze(analyzer.analyze).asInstanceOf[NamedExpression]),
      condition.analyze(analyzer.analyze),
      _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    ProjectAndFilter(projectList, condition, _)
}

private[snowpark] case class CopyIntoLocation(
    stagedFileWriter: StagedFileWriter,
    child: LogicalPlan)
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    CopyIntoLocation(stagedFileWriter, _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    createFromAnalyzedChild
}

private[snowpark] trait ViewType
private[snowpark] case object LocalTempView extends ViewType
private[snowpark] case object PersistedView extends ViewType
private[snowpark] case class CreateViewCommand(name: String, child: LogicalPlan, viewType: ViewType)
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    CreateViewCommand(name, _, viewType)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    createFromAnalyzedChild
}

case class Lateral(child: LogicalPlan, tableFunction: TableFunctionExpression) extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    Lateral(_, tableFunction.analyze(analyzer.analyze).asInstanceOf[TableFunctionExpression])

  override protected def updateChild: LogicalPlan => LogicalPlan =
    Lateral(_, tableFunction)
}
case class Limit(limitExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    Limit(limitExpr.analyze(analyzer.analyze), _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    Limit(limitExpr, _)
}

case class LimitOnSort(child: LogicalPlan, limitExpr: Expression, order: Seq[SortOrder])
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    LimitOnSort(
      _,
      limitExpr.analyze(analyzer.analyze),
      order.map(_.analyze(analyzer.analyze).asInstanceOf[SortOrder]))

  override protected def updateChild: LogicalPlan => LogicalPlan =
    LimitOnSort(_, limitExpr, order)
}

case class TableFunctionJoin(
    child: LogicalPlan,
    tableFunction: TableFunctionExpression,
    over: Option[WindowSpecDefinition])
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    TableFunctionJoin(
      _,
      tableFunction.analyze(analyzer.analyze).asInstanceOf[TableFunctionExpression],
      over)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    TableFunctionJoin(_, tableFunction, over)
}

case class TableMerge(
    tableName: String,
    child: LogicalPlan,
    joinExpr: Expression,
    clauses: Seq[MergeExpression])
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    TableMerge(
      tableName,
      _,
      joinExpr.analyze(analyzer.analyze),
      clauses.map(_.analyze(analyzer.analyze).asInstanceOf[MergeExpression]))

  override protected def updateChild: LogicalPlan => LogicalPlan =
    TableMerge(tableName, _, joinExpr, clauses)
}

// all WithColumns plan should be converted to Project plan by simplifier.
// So no need to be handled by SqlGenerator.
private[snowpark] case class WithColumns(newCols: Seq[NamedExpression], child: LogicalPlan)
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    WithColumns(newCols.map(_.analyze(analyzer.analyze).asInstanceOf[NamedExpression]), _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    WithColumns(newCols, _)
}

// all DropColumns plan should be converted to Project plan by simplifier.
// So no need to be handled by SqlGenerator.
private[snowpark] case class DropColumns(columns: Seq[NamedExpression], child: LogicalPlan)
    extends UnaryNode {
  override protected def createFromAnalyzedChild: LogicalPlan => LogicalPlan =
    DropColumns(columns.map(_.analyze(analyzer.analyze).asInstanceOf[NamedExpression]), _)

  override protected def updateChild: LogicalPlan => LogicalPlan =
    DropColumns(columns, _)
}
