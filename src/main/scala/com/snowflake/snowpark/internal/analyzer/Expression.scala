package com.snowflake.snowpark.internal.analyzer

import java.util.{Locale, UUID}

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.internal.analyzer
import com.snowflake.snowpark.types.DataType

// all Snowpark Expressions should inherit this Expression
private[snowpark] trait Expression {
  def nullable: Boolean = true
  def prettyName: String = getClass.getSimpleName.replaceAll("$$", "").toLowerCase(Locale.ROOT)
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  // a list of all sub-expressions.
  def children: Seq[Expression]

  // return a set of names of columns which invoked by this expression or
  // sub-expressions. If this expression or any sub-expression can't be
  // analyzed, return None.
  // `None` is not equivalent to `Some(Set.empty)`.
  // Some expressions such as `lit` may not have dependent column
  // but still can be analyzed, then they report `Some(Set.empty)` rather than `None`.
  // it is used by optimizer.
  lazy val dependentColumnNames: Option[Set[String]] =
    if (children.forall(_.dependentColumnNames.isDefined)) {
      // if all children can be analyzed
      Some(children.flatMap(_.dependentColumnNames.get).toSet)
    } else {
      None
    }

  // analyze this expression by given analyzing function.
  // used by Analyzer.
  def analyze(func: Expression => Expression): Expression = {
    val analyzedChildren = children.map(_.analyze(func))
    // if no child has been updated, use this expression directly.
    if (children == analyzedChildren) {
      func(this)
    } else {
      // otherwise create a new instance from analyzed children
      func(createAnalyzedExpression(analyzedChildren))
    }
  }

  // create a new instance from the given analyzed children expressions
  protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression

  // Default implementation for the source df of an expression
  // Used by session.tableFunction to support df columns
  val sourceDFs: Seq[DataFrame] = Seq.empty
}

// expr LIKE pattern
private[snowpark] case class Like(expr: Expression, pattern: Expression) extends Expression {
  override def children: Seq[Expression] = Seq(expr, pattern)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    Like(analyzedChildren.head, analyzedChildren(1))
}

private[snowpark] case class RegExp(expr: Expression, pattern: Expression) extends Expression {
  override def children: Seq[Expression] = Seq(expr, pattern)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    RegExp(analyzedChildren.head, analyzedChildren(1))
}

private[snowpark] case class Collate(expr: Expression, collationSpec: String) extends Expression {
  override def children: Seq[Expression] = Seq(expr)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    Collate(analyzedChildren.head, collationSpec)
}

private[snowpark] case class SubfieldString(expr: Expression, field: String) extends Expression {
  override def children: Seq[Expression] = Seq(expr)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    SubfieldString(analyzedChildren.head, field)
}

private[snowpark] case class SubfieldInt(expr: Expression, field: Int) extends Expression {
  override def children: Seq[Expression] = Seq(expr)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    SubfieldInt(analyzedChildren.head, field)
}

case class FunctionExpression(name: String, arguments: Seq[Expression], isDistinct: Boolean)
    extends Expression {
  override val sourceDFs: Seq[DataFrame] = children.flatMap(_.sourceDFs)
  override def prettyName: String = name
  override def children: Seq[Expression] = arguments
  override def sql: String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    FunctionExpression(name, analyzedChildren, isDistinct)
}

private[snowpark] trait TableFunctionExpression extends Expression

private[snowpark] case class FlattenFunction(
    input: Expression,
    path: String,
    outer: Boolean,
    recursive: Boolean,
    mode: String)
    extends TableFunctionExpression {
  override def children: Seq[Expression] = Seq(input)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    FlattenFunction(analyzedChildren.head, path, outer, recursive, mode)
}

private[snowpark] case class TableFunction(funcName: String, args: Seq[Expression])
    extends TableFunctionExpression {
  override def children: Seq[Expression] = args

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    TableFunction(funcName, analyzedChildren)
}

private[snowpark] case class NamedArgumentsTableFunction(
    funcName: String,
    args: Map[String, Expression])
    extends TableFunctionExpression {
  override def children: Seq[Expression] = args.values.toSeq

  // do not use this function, override analyze function directly
  override protected def createAnalyzedExpression(
      analyzedChildren: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def analyze(func: Expression => Expression): Expression = {
    val analyzedArgs = args.map {
      case (key, value) => key -> value.analyze(func)
    }
    if (analyzedArgs == args) {
      func(this)
    } else {
      func(NamedArgumentsTableFunction(funcName, analyzedArgs))
    }
  }
}

private[snowpark] case class GroupingSetsExpression(args: Seq[Set[Expression]])
    extends Expression {
  override def children: Seq[Expression] = args.flatten

  // do not use this function, override analyze function directly
  override protected def createAnalyzedExpression(
      analyzedChildren: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def analyze(func: Expression => Expression): Expression = {
    val analyzedArgs: Seq[Set[Expression]] =
      args.map(_.map(_.analyze(func)))

    if (args == analyzedArgs) {
      func(this)
    } else {
      func(GroupingSetsExpression(analyzedArgs))
    }
  }
}

private[snowpark] case class WithinGroup(expr: Expression, orderByCols: Seq[Expression])
    extends Expression {
  override def children: Seq[Expression] = expr +: orderByCols

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    WithinGroup(analyzedChildren.head, analyzedChildren.tail)
}

private[snowpark] abstract class MergeExpression(condition: Option[Expression]) extends Expression

private[snowpark] case class UpdateMergeExpression(
    condition: Option[Expression],
    assignments: Map[Expression, Expression])
    extends MergeExpression(condition) {
  override def children: Seq[Expression] =
    Seq(condition.toSeq, assignments.keys, assignments.values).flatten

  // do not use this function, override analyze function directly
  override protected def createAnalyzedExpression(
      analyzedChildren: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def analyze(func: Expression => Expression): Expression = {
    val analyzedCondition = condition.map(_.analyze(func))
    val analyzedAssignments = assignments.map {
      case (key, value) => key.analyze(func) -> value.analyze(func)
    }
    if (analyzedAssignments == assignments && analyzedCondition == condition) {
      func(this)
    } else {
      func(UpdateMergeExpression(analyzedCondition, analyzedAssignments))
    }
  }
}

private[snowpark] case class DeleteMergeExpression(condition: Option[Expression])
    extends MergeExpression(condition) {
  override def children: Seq[Expression] = condition.toSeq

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    if (analyzedChildren.isEmpty) this else DeleteMergeExpression(Option(analyzedChildren.head))
}

private[snowpark] case class InsertMergeExpression(
    condition: Option[Expression],
    keys: Seq[Expression],
    values: Seq[Expression])
    extends MergeExpression(condition) {
  override def children: Seq[Expression] =
    condition.toSeq ++ keys ++ values

  // do not use this function, override analyze function directly
  override protected def createAnalyzedExpression(
      analyzedChildren: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def analyze(func: Expression => Expression): Expression = {
    val analyzedCondition = condition.map(_.analyze(func))
    val analyzedKeys = keys.map(_.analyze(func))
    val analyzedValues = values.map(_.analyze(func))
    if (analyzedKeys == keys && analyzedValues == values && analyzedCondition == condition) {
      func(this)
    } else {
      func(InsertMergeExpression(analyzedCondition, analyzedKeys, analyzedValues))
    }
  }
}

trait GroupingSet extends Expression {
  def groupByExprs: Seq[Expression]
  override def children: Seq[Expression] = groupByExprs
}

case class Cube(groupByExprs: Seq[Expression]) extends GroupingSet {
  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    Cube(analyzedChildren)
}

case class Rollup(groupByExprs: Seq[Expression]) extends GroupingSet {
  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    Rollup(analyzedChildren)
}

private[snowpark] case class ScalarSubquery(plan: SnowflakePlan) extends Expression {
  override def children: Seq[Expression] = Seq.empty

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    this
}

private[snowpark] case class CaseWhen(
    branches: Seq[(Expression, Expression)],
    elseValue: Option[Expression] = None)
    extends Expression {
  override def children: Seq[Expression] =
    branches.flatMap(x => Seq(x._1, x._2)) ++ elseValue.toSeq

  // do not use this function, override analyze function directly
  override protected def createAnalyzedExpression(
      analyzedChildren: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def analyze(func: Expression => Expression): Expression = {
    val analyzedBranches = branches.map {
      case (key, value) => key.analyze(func) -> value.analyze(func)
    }
    val analyzedElseValue = elseValue.map(_.analyze(func))
    if (branches == analyzedBranches && elseValue == analyzedElseValue) {
      func(this)
    } else {
      func(CaseWhen(analyzedBranches, analyzedElseValue))
    }
  }
}

// map multiple Expressions as one Expression.
private[snowpark] case class MultipleExpression(expressions: Seq[Expression]) extends Expression {
  override def children: Seq[Expression] = expressions

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    MultipleExpression(analyzedChildren)
}

private[snowpark] case class InExpression(columns: Expression, values: Seq[Expression])
    extends Expression {
  override def children: Seq[Expression] = columns +: values

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    InExpression(analyzedChildren.head, analyzedChildren.tail)
}

private[snowpark] case class Star(expressions: Seq[NamedExpression]) extends Expression {
  override def children: Seq[Expression] = expressions

  override lazy val dependentColumnNames: Option[Set[String]] =
    if (expressions.isEmpty) {
      None // can't analyze
    } else if (expressions.forall(_.dependentColumnNames.isDefined)) {
      Some(expressions.flatMap(_.dependentColumnNames.get).toSet)
    } else {
      None // any sub-query can't be analyzed
    }

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    Star(analyzedChildren.map(_.asInstanceOf[NamedExpression]))
}

private[snowpark] case class ExprId private (id: Long)

private[snowpark] trait NamedExpression extends Expression {
  def name: String
  val exprId: ExprId = NamedExpression.newExprId
}

private[snowpark] object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  def newExprId: ExprId = ExprId(curId.getAndIncrement())
}

private[snowpark] class Attribute private (
    override val name: String,
    val dataType: DataType,
    override val nullable: Boolean,
    override val exprId: ExprId = NamedExpression.newExprId,
    override val sourceDFs: Seq[DataFrame] = Seq.empty)
    extends Expression
    with NamedExpression {
  def withName(newName: String): Attribute = {
    if (name == newName) {
      this
    } else {
      Attribute(analyzer.quoteName(newName), dataType, nullable)
    }
  }
  def withSourceDF(df: DataFrame): Attribute =
    new Attribute(name, dataType, nullable, exprId, Seq(df))
  override def sql: String = name
  override def toString: String = s"'$name"

  override def children: Seq[Expression] = Seq.empty

  override lazy val dependentColumnNames: Option[Set[String]] = Some(Set(quoteName(name)))

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    this
}

private[snowpark] object Attribute {
  def apply(name: String, dataType: DataType, nullable: Boolean): Attribute =
    new Attribute(quoteName(name), dataType, nullable)
  def apply(name: String, dataType: DataType): Attribute =
    new Attribute(quoteName(name), dataType, true)

  def apply(name: String, dataType: DataType, nullable: Boolean, exprId: ExprId): Attribute =
    new Attribute(name, dataType, nullable, exprId)

  def unapply(att: Attribute): Option[(String, DataType, Boolean)] =
    Option(att.name, att.dataType, att.nullable)
}

private[snowpark] case class UnresolvedAttribute(override val name: String)
    extends Expression
    with NamedExpression {
  override def sql: String = name

  override def children: Seq[Expression] = Seq.empty

  // can't analyze
  override lazy val dependentColumnNames: Option[Set[String]] = None

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    this
}

private[snowpark] case class UnresolvedDFAliasAttribute(override val name: String)
  extends Expression with NamedExpression {
  override def sql: String = ""

  override def children: Seq[Expression] = Seq.empty

  // can't analyze
  override lazy val dependentColumnNames: Option[Set[String]] = None

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    this
}

private[snowpark] case class ListAgg(col: Expression, delimiter: String, isDistinct: Boolean)
    extends Expression {
  override def children: Seq[Expression] = Seq(col)

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    ListAgg(analyzedChildren.head, delimiter, isDistinct)
}
