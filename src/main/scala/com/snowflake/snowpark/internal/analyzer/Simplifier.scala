package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.Session
import com.snowflake.snowpark.internal.ErrorMessage

class Simplifier(session: Session) {
  val policies: Seq[SimplificationPolicy] =
    Seq(
      FilterPlusFilterPolicy,
      UnionPlusUnionPolicy,
      SortPlusLimitPolicy,
      WithColumnPolicy(session),
      DropColumnPolicy(session),
      ProjectPlusFilterPolicy
    )
  val default: PartialFunction[LogicalPlan, LogicalPlan] = { case p =>
    p.updateChildren(simplify)
  }
  val policy: PartialFunction[LogicalPlan, LogicalPlan] =
    policies.foldRight(default) { case (prev, curr) =>
      prev.rule orElse curr
    }
  def simplify(plan: LogicalPlan): LogicalPlan = {
    var changed = true
    var simplified = plan
    while (changed) {
      val result = policy(simplified)
      changed = simplified != result
      simplified = result
      if (changed) {
        simplified.setSourcePlan(plan)
      }
    }
    simplified
  }
}

trait SimplificationPolicy {
  val rule: PartialFunction[LogicalPlan, LogicalPlan]
}

object ProjectPlusFilterPolicy extends SimplificationPolicy {
  override val rule: PartialFunction[LogicalPlan, LogicalPlan] = {
    case Filter(condition, Project(projectList, child, _)) if canMerge(projectList, condition) =>
      ProjectAndFilter(projectList, condition, child)
    case Filter(condition2, ProjectAndFilter(projectList, condition1, child))
        if canMerge(projectList, condition2) =>
      ProjectAndFilter(projectList, And(condition1, condition2), child)
  }

  def canMerge(projectList: Seq[NamedExpression], condition: Expression): Boolean = {
    val canAnalyzeProject: Boolean = projectList.forall {
      case _: UnresolvedAttribute => false
      case _: UnresolvedAlias     => false
      case _                      => true
    }
    val canAnalyzeCondition: Boolean = condition.dependentColumnNames.isDefined
    // don't merge if can't analyze
    if (canAnalyzeCondition && canAnalyzeProject) {
      val newProjectColumns: Set[String] = projectList.flatMap {
        case Alias(_, name, _) => Some(quoteName(name))
        case _                 => None
      }.toSet
      val conditionDependencies = condition.dependentColumnNames.get
      // merge if no intersection
      (newProjectColumns & conditionDependencies).isEmpty
    } else {
      false
    }
  }
}

object UnionPlusUnionPolicy extends SimplificationPolicy {
  override val rule: PartialFunction[LogicalPlan, LogicalPlan] = {
    case plan @ (_: Union | _: UnionAll) => process(plan)
  }

  private def process(plan: LogicalPlan): LogicalPlan =
    plan match {
      case Union(left, right) =>
        val newChildren: Seq[LogicalPlan] = Seq(process(left), process(right)).flatMap {
          case SimplifiedUnion(children) => children
          case other                     => Seq(other)
        }
        SimplifiedUnion(newChildren)

      case UnionAll(left, right) =>
        val newChildren: Seq[LogicalPlan] = Seq(process(left), process(right)).flatMap {
          case SimplifiedUnionAll(children) => children
          case other                        => Seq(other)
        }
        SimplifiedUnionAll(newChildren)

      case _ => plan
    }
}
object FilterPlusFilterPolicy extends SimplificationPolicy {
  override val rule: PartialFunction[LogicalPlan, LogicalPlan] = {
    case Filter(condition, child: Filter) =>
      Filter(And(child.condition, condition), child.child)
  }
}

object SortPlusLimitPolicy extends SimplificationPolicy {
  override val rule: PartialFunction[LogicalPlan, LogicalPlan] = {
    case Limit(limitExpr, Sort(order, child)) =>
      LimitOnSort(child, limitExpr, order)
  }
}

case class DropColumnPolicy(session: Session) extends SimplificationPolicy {
  override val rule: PartialFunction[LogicalPlan, LogicalPlan] = { case plan: DropColumns =>
    val (cols, leaf) = process(plan)
    if (cols.isEmpty) {
      throw ErrorMessage.DF_CANNOT_DROP_ALL_COLUMNS()
    }
    Project(cols, leaf)
  }
  // return remaining columns and leaf node
  private def process(plan: DropColumns): (Seq[NamedExpression], LogicalPlan) = {
    val dropColumns = plan.columns.map(c => quoteName(c.name)).toSet
    val (child_output, child) = plan.child match {
      case columns: DropColumns =>
        process(columns)
      case _ =>
        val output = session.analyzer.resolve(plan.child).output
        (output, plan.child)
    }
    val remainingColumns = child_output.filter(c => !dropColumns.contains(c.name))
    (remainingColumns, child)
  }
}

case class WithColumnPolicy(session: Session) extends SimplificationPolicy {
  override val rule: PartialFunction[LogicalPlan, LogicalPlan] = { case plan: WithColumns =>
    val (leaf, _, newCols) = process(plan)
    if (newCols.isEmpty) {
      leaf
    } else {
      Project(UnresolvedAttribute("*") +: newCols, leaf)
    }
  }

  /*
   * return
   * leaf plan: non WithColumns plan,
   * leaf output/schema
   * new columns
   */
  private def process(
      plan: WithColumns
  ): (LogicalPlan, Seq[NamedExpression], Seq[NamedExpression]) =
    plan match {
      case WithColumns(newCols, child: WithColumns) =>
        val (leaf, l_output, c_columns) = process(child)
        merge(leaf, l_output, c_columns, newCols)
      case WithColumns(newCols, child) =>
        val output = session.analyzer.resolve(child).output
        merge(child, output, Seq.empty, newCols)
    }

  private def merge(
      leaf: LogicalPlan,
      l_output: Seq[NamedExpression], // leaf schema
      c_columns: Seq[NamedExpression], // staging new columns
      newCols: Seq[NamedExpression]
  ): // new columns
  (LogicalPlan, Seq[NamedExpression], Seq[NamedExpression]) = {
    val childrenNames = (l_output ++ c_columns).map(_.name).toSet
    val canAnalyze = newCols.forall(_.dependentColumnNames.isDefined)
    val newNames = newCols.map(_.name).toSet
    val dependentColNames = if (canAnalyze) {
      newCols.flatMap(_.dependentColumnNames.get).toSet
    } else Set.empty[String]
    val invokeExistingColumns = (childrenNames & dependentColNames).nonEmpty
    val common = childrenNames & newNames
    // four cases:
    // 1, new columns don't replace any existing column,
    // and don't depend on any existing column.
    // For this case, simplifier directly puts the new columns
    // on the new column list.
    // 2, new columns replace existing columns,
    // and also depend on existing columns.
    // For this case, the simplifier creates two sub-queries:
    // project all columns (handle dependency) and
    // replace the existing columns (handle replacement).
    // 3, new columns replace existing columns but not depend on existing columns.
    // For this case, simplifier creates a new sub-query to
    // replace the existing columns.
    // 4, new columns don't replace existing columns but depend on them.
    // For this case, simplifier creates a new sub-query to project all existing columns
    if (common.isEmpty && canAnalyze && !invokeExistingColumns) {
      (leaf, l_output, c_columns ++ newCols)
    } else if (common.nonEmpty && (!canAnalyze || invokeExistingColumns)) {
      // in case of refer and replace at same time
      // for example, df has column a
      // df.withColumn("a", col("a") + 1)
      // it should have two sub queries
      // 1, project all column
      // 2, replace the old column by new one
      val newLeaf = Project(l_output ++ c_columns, leaf)
      val newProjectList = (l_output ++ c_columns)
        .filter(x => !common.contains(x.name))
        .map(x => UnresolvedAttribute(x.name)) ++ newCols
      val newOutput = newProjectList.map(x => UnresolvedAttribute(x.name))
      (Project(newProjectList, newLeaf), newOutput, Seq.empty)
    } else if (common.nonEmpty) {
      val projectList = (l_output ++ c_columns)
        .filter(x => !common.contains(x.name)) ++ newCols
      // can't directly use projectList, it may contains function then
      // result a duplicated function call. for example.
      // select a + 1 as a from (select a + 1 as a from (table(a)))
      val newOutput = projectList.map(x => UnresolvedAttribute(x.name))
      (Project(projectList, leaf), newOutput, Seq.empty)
    } else {
      val projectList = UnresolvedAttribute("*") +: c_columns
      val newOutput = (l_output ++ c_columns).map(x => UnresolvedAttribute(x.name))
      (Project(projectList, leaf), newOutput, newCols)
    }
  }
}
