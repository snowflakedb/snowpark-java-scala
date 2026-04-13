package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.Session
import com.snowflake.snowpark.internal.{ErrorMessage, Logging, SnowflakeUDF}
import com.snowflake.snowpark.types._

import scala.collection.mutable.ArrayBuffer
import scala.util.DynamicVariable

private object SqlGenerator extends Logging {

  // Sub-query is an expression, so the SnowflakePlan is not accessible when analyzing the sub-query
  // expression. If there are multiple queries for the sub-query plan, the queries except the last
  // one needs to be added to the parent SnowflakePlan. So 'subqueryPlans' is introduced to cache
  // the sub queries plan when analyzing. And the queries in the plans are added to the final plan.
  val subqueryPlans =
    new DynamicVariable[ArrayBuffer[SnowflakePlan]](ArrayBuffer.empty[SnowflakePlan])

  def generateSqlQuery(plan: LogicalPlan, session: Session): SnowflakePlan =
    subqueryPlans.withValue(ArrayBuffer.empty) {
      var result = generate(plan, session)
      if (subqueryPlans.value.nonEmpty) {
        result = result.withSubqueries(subqueryPlans.value.toArray)
      }
      result
    }

  private def generate(plan: LogicalPlan, session: Session): SnowflakePlan = {
    import session.plans._
    def resolveChild(plan: LogicalPlan): SnowflakePlan = {
      plan.getOrUpdateSnowflakePlan(generateSqlQuery(plan, session))
    }

    plan match {
      case p: SnowflakePlan => p
      case TableFunctionJoin(child, tableFunction, over) =>
        joinTableFunction(
          expressionToSql(tableFunction),
          resolveChild(child),
          over.map(expressionToSql),
          Some(plan))
      case TableFunctionRelation(tableFunction) =>
        fromTableFunction(expressionToSql(tableFunction))
      case StoredProcedureRelation(spName, args) =>
        fromStoredProcedure(spName, args.map(expressionToSql))
      case Lateral(child, tableFunction) =>
        lateral(expressionToSql(tableFunction), resolveChild(child), Some(plan))
      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        aggregate(
          groupingExpressions.map(toSqlAvoidOffset),
          aggregateExpressions.map(expressionToSql),
          resolveChild(child),
          Some(plan))
      case Project(projectList, child, _) =>
        project(projectList.map(expressionToSql), resolveChild(child), Some(plan))
      case Filter(condition, child) =>
        filter(expressionToSql(condition), resolveChild(child), Some(plan))
      case ProjectAndFilter(projectList, condition, child) =>
        projectAndFilter(
          projectList.map(expressionToSql),
          expressionToSql(condition),
          resolveChild(child),
          Some(plan))
      case SnowflakeSampleNode(probabilityFraction, rowCount, child) =>
        sample(probabilityFraction, rowCount, resolveChild(child), Some(plan))
      case Sort(order, child) =>
        sort(order.map(expressionToSql), resolveChild(child), Some(plan))
      // Binary Nodes
      case op @ Intersect(left, right) =>
        setOperator(resolveChild(left), resolveChild(right), op.sql, Some(plan))
      case op @ Except(left, right) =>
        setOperator(resolveChild(left), resolveChild(right), op.sql, Some(plan))
      case op @ SimplifiedUnion(children) =>
        setOperator(children.map(resolveChild), op.sql, Some(plan))
      case op @ SimplifiedUnionAll(children) =>
        setOperator(children.map(resolveChild), op.sql, Some(plan))
      case Join(left, right, joinType, condition) =>
        join(
          resolveChild(left),
          resolveChild(right),
          joinType,
          condition.map(expressionToSql),
          Some(plan))
      // relations
      case Range(start, end, step) =>
        // The column name id lower-case is hard-coded as the output
        // schema of Range. Since this corresponds to the Snowflake column "id"
        // (quoted lower-case) it's a little hard for users. So we switch it to
        // the column name "ID" == id == Id
        query(rangeStatement(start, end, step, "id"), Some(plan))
      case Generator(exprs, rowCount) =>
        query(generator(exprs.map(expressionToSql), rowCount), Some(plan))
      case SnowflakeValues(output, data) if data.isEmpty =>
        query(emptyValuesStatement(output), Some(plan))
      case SnowflakeValues(output, data) =>
        // Use SELECT VALUES statement for small values,
        // otherwise, CREATE TEMP table, INSERT data and SELECT from temp table
        if (output.size * data.size < ARRAY_BIND_THRESHOLD) {
          query(valuesStatement(output, data), Some(plan))
        } else {
          largeLocalRelationPlan(output, data, Some(plan))
        }
      // table
      case UnresolvedRelation(tableName) => table(tableName)
      // SaveAsTable
      case SnowflakeCreateTable(tableName, mode, Some(child), tempType) =>
        saveAsTable(tableName, mode, resolveChild(child), tempType)
      case CopyIntoLocation(stagedFileWriter, child) =>
        copyIntoLocation(stagedFileWriter, resolveChild(child))
      // limits
      case Limit(offset, child) =>
        limit(toSqlAvoidOffset(offset), resolveChild(child), Some(plan))
      case Offset(offsetExpr, child) =>
        offset(toSqlAvoidOffset(offsetExpr), resolveChild(child), Some(plan))
      case LimitOnSort(child, offset, order) =>
        limitOnSort(
          resolveChild(child),
          toSqlAvoidOffset(offset),
          order.map(expressionToSql),
          Some(plan))
      // update
      case TableUpdate(tableName, assignments, condition, sourceData) =>
        update(
          tableName,
          assignments.map { case (k, v) =>
            (expressionToSql(k), expressionToSql(v))
          },
          condition.map(expressionToSql),
          sourceData.map(resolveChild))
      // delete
      case TableDelete(tableName, condition, sourceData) =>
        delete(tableName, condition.map(expressionToSql), sourceData.map(resolveChild))
      // merge
      case TableMerge(tableName, source, joinExpr, clauses) =>
        merge(
          tableName,
          resolveChild(source),
          expressionToSql(joinExpr),
          clauses.map(expressionToSql))

      case Pivot(pivotColumn, pivotValues, aggregates, child) =>
        require(aggregates.size == 1, "Only one aggregate is supported with pivot")
        pivot(
          expressionToSql(pivotColumn),
          pivotValues.map(expressionToSql),
          expressionToSql(aggregates.head), // only support single aggregation function
          resolveChild(child),
          Some(plan))

      case CreateViewCommand(name, child, viewType) =>
        val isTemp = viewType match {
          case PersistedView => false
          case LocalTempView => true
          case _ =>
            throw ErrorMessage.PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE(viewType.toString())
        }
        createOrReplaceView(name, resolveChild(child), isTemp)

      case CopyIntoNode(name, columnNames, transformations, options, stagedFileReader) =>
        stagedFileReader
          .table(name)
          .columnNames(columnNames)
          .transformations(transformations)
          .options(options)
          .createSnowflakePlan()
      case DataframeAlias(_, child, _) => resolveChild(child)
    }
  }

  def expressionToSql(expr: Expression): String =
    expr match {
      case GroupingSetsExpression(args) => groupingSetExpression(args.map(_.map(expressionToSql)))
      case TableFunctionExpressionExtractor(str) => str
      case SubfieldString(expr, field) => subfieldExpression(expressionToSql(expr), field)
      case SubfieldInt(expr, field) => subfieldExpression(expressionToSql(expr), field)
      case Like(expr, pattern) => likeExpression(expressionToSql(expr), expressionToSql(pattern))
      case RegExp(expr, pattern) =>
        regexpExpression(expressionToSql(expr), expressionToSql(pattern))
      case Collate(expr, collationSpec) => collateExpression(expressionToSql(expr), collationSpec)
      case SnowflakeUDF(name, children, _, _, _) =>
        functionExpression(name, children.map(expressionToSql), isDistinct = false)
      case CaseWhen(branches, elseValue) =>
        // translated to
        // CASE WHEN condition1 THEN value1 WHEN condition2 THEN value2 ELSE value3 END
        caseWhenExpression(
          branches.map { case (condition, value) =>
            (expressionToSql(condition), expressionToSql(value))
          },
          elseValue match {
            case Some(value) => expressionToSql(value)
            case _ => "NULL"
          })
      case MultipleExpression(expressions) => blockExpression(expressions.map(expressionToSql))
      case InExpression(column, values) =>
        inExpression(expressionToSql(column), values.map(expressionToSql))
      // aggregate
      case GroupingExtractor(str) => str

      // window
      case WindowExpression(windowFunction, windowSpec) =>
        windowExpression(expressionToSql(windowFunction), expressionToSql(windowSpec))
      case WindowSpecDefinition(partitionSpec, orderSpec, frameSpecification) =>
        windowSpecExpressions(
          partitionSpec.map(toSqlAvoidOffset),
          orderSpec.map(toSqlAvoidOffset),
          expressionToSql(frameSpecification))
      case SpecifiedWindowFrame(frameType, lower, upper) =>
        specifiedWindowFrameExpression(
          frameType.sql,
          windowFrameBoundary(toSqlAvoidOffset(lower)),
          windowFrameBoundary(toSqlAvoidOffset(upper)))
      case UnspecifiedFrame => ""
      case SpecialFrameBoundaryExtractor(str) => str

      case Literal(value, dataType) =>
        DataTypeMapper.toSql(value, dataType)
      case attr: Attribute => quoteName(attr.name)
      // unresolved expression
      case UnresolvedAttribute(name) => name
      case FunctionExpression(name, children, isDistinct) =>
        functionExpression(name, children.map(toSqlAvoidOffset), isDistinct)
      case Star(columns) =>
        if (columns.isEmpty) {
          "*"
        } else {
          columns.map(expressionToSql).mkString(",")
        }
      case BinaryOperatorExtractor(str) => str
      case SortOrder(child, direction, nullOrdering, _) =>
        orderExpression(expressionToSql(child), direction.sql, nullOrdering.sql)
      case UnaryExpressionExtractor(str) => str
      case ScalarSubquery(plan: SnowflakePlan) =>
        subqueryPlans.value.append(plan)
        subqueryExpression(plan.queries.last.sql)
      case WithinGroup(expr, orderByExprs) =>
        withinGroupExpression(expressionToSql(expr), orderByExprs.map(expressionToSql))
      case InsertMergeExpression(condition, keys, values) =>
        insertMergeStatement(
          condition.map(expressionToSql),
          keys.map(expressionToSql),
          values.map(expressionToSql))
      case UpdateMergeExpression(condition, assignments) =>
        updateMergeStatement(
          condition.map(expressionToSql),
          assignments.map { case (k, v) =>
            (expressionToSql(k), expressionToSql(v))
          })
      case DeleteMergeExpression(condition) =>
        deleteMergeStatement(condition.map(expressionToSql))
      case ListAgg(expr, delimiter, isDistinct) =>
        listAgg(expressionToSql(expr), DataTypeMapper.stringToSql(delimiter), isDistinct)

      // unsupported expressions
      case _ =>
        throw new UnsupportedOperationException(s"Expression ${expr.toString} is not supported.")

    }

  object TableFunctionExpressionExtractor {
    def unapply(expr: TableFunctionExpression): Option[String] =
      Option(expr match {
        case FlattenFunction(input, path, outer, recursive, mode) =>
          flattenExpression(expressionToSql(input), path, outer, recursive, mode)
        case TableFunctionEx(functionName, args) =>
          functionExpression(functionName, args.map(expressionToSql), isDistinct = false)
        case NamedArgumentsTableFunction(funcName, args) =>
          namedArgumentsFunction(
            funcName,
            args.map { case (str, expression) =>
              str -> expressionToSql(expression)
            })
      })
  }

  object UnaryExpressionExtractor {
    def unapply(expr: UnaryExpression): Option[String] =
      Option(expr match {
        case Alias(child: Attribute, name, _) =>
          aliasExpression(expressionToSql(child), quoteName(name))
        case Alias(child, name, _) => aliasExpression(expressionToSql(child), quoteName(name))
        case UnresolvedAlias(child, _) => expressionToSql(child)
        case Cast(child, dataType) => castExpression(expressionToSql(child), dataType)
        case _ =>
          unaryExpression(expressionToSql(expr.child), expr.sqlOperator, expr.operatorFirst)
      })
  }

  object SpecialFrameBoundaryExtractor {
    def unapply(expr: SpecialFrameBoundary): Option[String] =
      expr match {
        case UnboundedPreceding | UnboundedFollowing | CurrentRow => Some(expr.sql)
      }
  }

  object BinaryOperatorExtractor {
    def unapply(expr: BinaryExpression): Option[String] =
      Option(expr match {
        case _: BinaryArithmeticExpression =>
          binaryArithmeticExpression(
            expr.sqlOperator,
            expressionToSql(expr.left),
            expressionToSql(expr.right))
        case _ =>
          functionExpression(
            expr.sqlOperator,
            Seq(expressionToSql(expr.left), expressionToSql(expr.right)),
            isDistinct = false)
      })
  }

  object GroupingExtractor {
    def unapply(expr: Expression): Option[String] =
      expr match {
        case _: Cube | _: Rollup =>
          Some(
            expressionToSql(
              FunctionExpression(
                // scalastyle:off caselocale
                expr.prettyName.toUpperCase,
                // scalastyle:on caselocale

                /*
                remove alias in grouping functions.
                for example.
                 Session.table("t1").rollup($"a" as "col1").agg(sum(b))
                 should be translated to
                 select a as col1, sum(b) from t1 group by rollup a.
                 but not
                 select a as col1, sum(b) from t1 group by rollup a as col1
                 Alias in group-by expression is forbidden and useless.
                 */
                expr.children.map {
                  case Alias(child, _, _) => child
                  case child => child
                },
                isDistinct = false)))
        case _ => None
      }
  }

  def windowFrameBoundary(offset: String): String = {
    try {
      val num: Int = offset.toInt
      windowFrameBoundaryExpression(num.abs.toString, num >= 0)
    } catch {
      case _: Throwable => offset
    }
  }

  // if expression is integral literal, return the number without casing,
  // otherwise process as normal
  def toSqlAvoidOffset(exp: Expression): String =
    exp match {
      case Literal(value, Some(dataType)) if dataType.isInstanceOf[IntegralType] =>
        DataTypeMapper.toSqlWithoutCast(value, dataType)
      case _ => expressionToSql(exp)
    }

  @inline private final val ARRAY_BIND_THRESHOLD = 512

}
