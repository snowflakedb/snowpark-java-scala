package com.snowflake.snowpark.internal

import com.snowflake.snowpark.FileOperationCommand._
import com.snowflake.snowpark.Row
import com.snowflake.snowpark.internal.Utils.{TempObjectType, randomNameForTempObject}
import com.snowflake.snowpark.types.{DataType, convertToSFType}

package object analyzer {
  // constant string
  @inline private final val _LeftParenthesis: String = "("
  @inline private final val _RightParenthesis: String = ")"
  @inline private final val _LeftBracket: String = "["
  @inline private final val _RightBracket: String = "]"
  @inline private final val _As: String = " AS "
  @inline private final val _And: String = " AND "
  @inline private final val _Or: String = " OR "
  @inline private final val _Not: String = " NOT "
  @inline private final val _Dot: String = "."
  @inline private final val _Star: String = " * "
  @inline private final val _EmptyString: String = ""
  @inline private final val _Space: String = " "
  @inline private final val _DoubleQuote: String = "\""
  @inline private final val _SingleQuote: String = "'"
  @inline private final val _Comma: String = ", "
  @inline private final val _Minus: String = " - "
  @inline private final val _Plus: String = " + "
  @inline private final val _Semicolon: String = ";"
  @inline private final val _ColumnNamePrefix: String = " COL_"
  @inline private final val _Distinct: String = " DISTINCT "
  @inline private final val _BitAnd: String = " BITAND "
  @inline private final val _BitOr: String = " BITOR "
  @inline private final val _BitXor: String = " BITXOR "
  @inline private final val _BitNot: String = " BITNOT "
  @inline private final val _BitShiftLeft: String = " BITSHIFTLEFT "
  @inline private final val _BitShiftRight: String = " BITSHIFTRIGHT "
  @inline private final val _Like: String = " LIKE "
  @inline private final val _Cast: String = " CAST "
  @inline private final val _Iff: String = " IFF "
  @inline private final val _In: String = " IN "
  @inline private final val _ToDecimal: String = " TO_DECIMAL "
  @inline private final val _Asc: String = " ASC "
  @inline private final val _Desc: String = " DESC "
  @inline private final val _Pow: String = " POW "
  @inline private final val _GroupBy: String = " GROUP BY "
  @inline private final val _PartitionBy: String = " PARTITION BY "
  @inline private final val _OrderBy: String = " ORDER BY "
  @inline private final val _Over: String = " OVER "
  @inline private final val _Power: String = " POWER "
  @inline private final val _Round: String = " ROUND "
  @inline private final val _Concat: String = " CONCAT "
  @inline private final val _Select: String = " SELECT "
  @inline private final val _From: String = " FROM "
  @inline private final val _Where: String = " WHERE "
  @inline private final val _Limit: String = " LIMIT "
  @inline private final val _Pivot: String = " PIVOT "
  @inline private final val _For: String = " FOR "
  @inline private final val _On: String = " ON "
  @inline private final val _Using: String = " USING "
  @inline private final val _Join: String = " JOIN "
  @inline private final val _Natural: String = " NATURAL "
  @inline private final val _InnerJoin: String = " INNER JOIN "
  @inline private final val _LeftOuterJoin: String = " LEFT OUTER JOIN "
  @inline private final val _RightOuterJoin: String = " RIGHT OUTER JOIN "
  @inline private final val _FullOuterJoin: String = " FULL OUTER JOIN "
  @inline private final val _Exists: String = " EXISTS "
  @inline private final val _UnionAll: String = " UNION ALL "
  @inline private final val _Create: String = " CREATE "
  @inline private final val _Table: String = " TABLE "
  @inline private final val _Replace: String = " REPLACE "
  @inline private final val _View: String = " VIEW "
  @inline private final val _Temporary: String = " TEMPORARY "
  @inline private final val _ScopedTemporary: String = " SCOPED TEMPORARY "
  @inline private final val _If: String = " If "
  @inline private final val _Insert: String = " INSERT "
  @inline private final val _Into: String = " INTO "
  @inline private final val _Values: String = " VALUES "
  @inline private final val _InlineTable: String = " INLINE_TABLE "
  @inline private final val _Seq8: String = " SEQ8() "
  @inline private final val _RowNumber: String = " ROW_NUMBER() "
  @inline private final val _One: String = " 1 "
  @inline private final val _Generator: String = "GENERATOR"
  @inline private final val _RowCount: String = "ROWCOUNT"
  @inline private final val _RightArrow: String = " => "
  @inline private final val _LessThanOrEqual: String = " <= "
  @inline private final val _Number: String = " NUMBER "
  @inline private final val _UnsatFilter: String = " 1 = 0 "
  @inline private final val _Is: String = " IS "
  @inline private final val _Null: String = " NULL "
  @inline private final val _Between: String = " BETWEEN "
  @inline private final val _Following: String = " FOLLOWING "
  @inline private final val _Preceding: String = " PRECEDING "
  @inline private final val _Dollar: String = "$"
  @inline private final val _DoubleColon: String = "::"
  @inline private final val _Drop: String = " DROP "
  @inline private final val _EqualNull: String = " EQUAL_NULL "
  @inline private final val _IsNaN: String = " = 'NaN'"
  @inline private final val _File: String = " FILE "
  @inline private final val _Format: String = " FORMAT "
  @inline private final val _Type: String = " TYPE "
  @inline private final val _Equals: String = " = "
  @inline private final val _FileFormat: String = " FILE_FORMAT "
  @inline private final val _Copy: String = " COPY "
  @inline private final val _RegExp: String = " REGEXP "
  @inline private final val _Collate: String = " COLLATE "
  @inline private final val _ResultScan: String = " RESULT_SCAN"
  @inline private final val _Sample: String = " SAMPLE "
  @inline private final val _Rows: String = " ROWS "
  @inline private final val _Case: String = " CASE "
  @inline private final val _When: String = " WHEN "
  @inline private final val _Then: String = " THEN "
  @inline private final val _Else: String = " ELSE "
  @inline private final val _End: String = " END "
  @inline private final val _Flatten: String = " FLATTEN "
  @inline private final val _Input: String = " INPUT "
  @inline private final val _Path: String = " PATH "
  @inline private final val _Outer: String = " OUTER "
  @inline private final val _Recursive: String = " RECURSIVE "
  @inline private final val _Mode: String = " MODE "
  @inline private final val _Lateral: String = " LATERAL "
  @inline private final val _Put: String = " PUT "
  @inline private final val _Get: String = " GET "
  @inline private final val _GroupingSets: String = " GROUPING SETS "
  @inline private final val _QuestionMark: String = "?"
  @inline private final val _Pattern: String = " PATTERN "
  @inline private final val _Update: String = " UPDATE "
  @inline private final val _Set: String = " SET "
  @inline private final val _Delete: String = " DELETE "
  @inline private final val _WithinGroup: String = " WITHIN GROUP "
  @inline private final val _Merge: String = " MERGE "
  @inline private final val _Matched: String = " MATCHED "
  @inline private final val _ListAgg: String = " LISTAGG "
  @inline private final val _Call: String = " CALL "

  private[snowpark] sealed trait TempType

  private[snowpark] object TempType {
    case object Permanent extends TempType {
      override def toString: String = _EmptyString
    }
    case object Temporary extends TempType {
      override def toString: String = _Temporary
    }
    case object ScopedTemporary extends TempType {
      override def toString: String = _ScopedTemporary
    }
  }

  private[analyzer] def flattenExpression(
      input: String,
      path: String,
      outer: Boolean,
      recursive: Boolean,
      mode: String
  ): String = {
    // flatten(input => , path => , outer => , recursive => , mode =>)
    _Flatten + _LeftParenthesis + _Input + _RightArrow + input + _Comma + _Path +
      _RightArrow + _SingleQuote + path + _SingleQuote + _Comma + _Outer +
      _RightArrow + outer.toString + _Comma + _Recursive + _RightArrow +
      recursive.toString + _Comma + _Mode + _RightArrow + _SingleQuote + mode +
      _SingleQuote + _RightParenthesis
  }

  private[analyzer] def joinTableFunctionStatement(
      func: String,
      child: String,
      over: Option[String]
  ): String =
    _Select + _Star + _From + _LeftParenthesis + child + _RightParenthesis + _Join +
      table(func, over)

  private[analyzer] def lateralStatement(lateralExpression: String, child: String): String =
    _Select + _Star + _From + _LeftParenthesis + child + _RightParenthesis + _Comma +
      _Lateral + lateralExpression

  private[analyzer] def storedProcedureStatement(name: String, args: Seq[String]): String =
    _Call + name + args.mkString(_LeftParenthesis, _Comma, _RightParenthesis)

  private[analyzer] def tableFunctionStatement(func: String): String =
    projectStatement(Seq.empty, table(func))

  private[analyzer] def caseWhenExpression(
      branches: Seq[(String, String)],
      elseValue: String
  ): String =
    _Case + branches.map { case (condition, value) =>
      _When + condition + _Then + value
    }.mkString + _Else + elseValue + _End

  private[analyzer] def resultScanStatement(uuidPlaceHolder: String): String =
    _Select + _Star + _From + _Table + _LeftParenthesis + _ResultScan + _LeftParenthesis +
      _SingleQuote + uuidPlaceHolder + _SingleQuote + _RightParenthesis + _RightParenthesis

  private[analyzer] def collateExpression(expr: String, collationSpec: String): String =
    expr + _Collate + singleQuote(collationSpec)

  private[analyzer] def regexpExpression(expr: String, pattern: String): String =
    expr + _RegExp + pattern

  private[analyzer] def likeExpression(expr: String, pattern: String): String =
    expr + _Like + pattern

  private[analyzer] def blockExpression(expressions: Seq[String]): String =
    expressions.mkString(_LeftParenthesis, _Comma, _RightParenthesis)

  private[analyzer] def inExpression(column: String, values: Seq[String]): String =
    column + _In + blockExpression(values)

  private[analyzer] def subfieldExpression(expr: String, field: String): String =
    expr + _LeftBracket + _SingleQuote + field + _SingleQuote + _RightBracket

  private[analyzer] def subfieldExpression(expr: String, field: Int): String =
    expr + _LeftBracket + field + _RightBracket

  private[analyzer] def functionExpression(
      name: String,
      children: Seq[String],
      isDistinct: Boolean
  ): String =
    name + _LeftParenthesis + (if (isDistinct) _Distinct else _EmptyString) + children.mkString(
      _Comma
    ) +
      _RightParenthesis

  private[analyzer] def namedArgumentsFunction(name: String, args: Map[String, String]): String =
    name + args
      .map { case (key, value) =>
        key + _RightArrow + value
      }
      .mkString(_LeftParenthesis, _Comma, _RightParenthesis)

  private[analyzer] def equalNullSafe(left: String, right: String): String =
    _EqualNull + _LeftParenthesis + left + _Comma + right + _RightParenthesis

  private[analyzer] def binaryComparison(left: String, right: String, symbol: String): String = {
    left + _Space + symbol + _Space + right
  }

  private[analyzer] def unaryExpression(
      child: String,
      sqlOperator: String,
      operatorFirst: Boolean
  ): String =
    if (operatorFirst) {
      sqlOperator + _Space + child
    } else {
      child + _Space + sqlOperator
    }

  private[analyzer] def subqueryExpression(child: String): String = {
    _LeftParenthesis + child + _RightParenthesis
  }

  private[analyzer] def windowExpression(windowFunction: String, windowSpec: String): String =
    windowFunction + _Over + _LeftParenthesis + windowSpec + _RightParenthesis

  private[analyzer] def windowSpecExpressions(
      partitionSpec: Seq[String],
      orderSpec: Seq[String],
      frameSpec: String
  ): String =
    (if (partitionSpec.nonEmpty) _PartitionBy + partitionSpec.mkString(_Comma) else _EmptyString) +
      (if (orderSpec.nonEmpty) _OrderBy + orderSpec.mkString(_Comma) else _EmptyString) + frameSpec

  private[analyzer] def windowOffsetFunctionExpression(
      input: String,
      offset: String,
      default: String,
      op: String
  ): String =
    op + _LeftParenthesis + input + _Comma + offset + _Comma + default + _RightParenthesis

  private[analyzer] def specifiedWindowFrameExpression(
      frameType: String,
      lower: String,
      upper: String
  ): String =
    _Space + frameType + _Between + lower + _And + upper + _Space

  private[analyzer] def windowFrameBoundaryExpression(
      offset: String,
      isFollowing: Boolean
  ): String =
    offset + (if (isFollowing) _Following else _Preceding)

  private[analyzer] def castExpression(child: String, dataType: DataType): String =
    _Cast + _LeftParenthesis + child + _As + convertToSFType(dataType) +
      _RightParenthesis

  private[analyzer] def orderExpression(
      name: String,
      direction: String,
      nullOrdering: String
  ): String =
    name + _Space + direction + _Space + nullOrdering

  private[analyzer] def aliasExpression(origin: String, alias: String): String =
    origin + _As + alias

  private[analyzer] def withinGroupExpression(column: String, orderByCols: Seq[String]): String =
    column + _WithinGroup + _LeftParenthesis + _OrderBy + orderByCols.mkString(_Comma) +
      _RightParenthesis

  private[analyzer] def binaryArithmeticExpression(
      op: String,
      left: String,
      right: String
  ): String =
    _LeftParenthesis + left + _Space + op + _Space + right + _RightParenthesis

  private[analyzer] def limitExpression(num: Int): String =
    _Limit + num

  private[analyzer] def groupingSetExpression(args: Seq[Set[String]]): String =
    _GroupingSets + args
      .map(_.mkString(_LeftParenthesis, _Comma, _RightParenthesis))
      .mkString(_LeftParenthesis, _Comma, _RightParenthesis)
  // for read from table
  private[analyzer] def projectStatement(
      project: Seq[String],
      child: String,
      isDistinct: Boolean = false
  ): String =
    _Select + (if (isDistinct) _Distinct else _EmptyString) +
      (if (project.isEmpty) _Star else project.mkString(_Comma)) +
      _From + _LeftParenthesis + child + _RightParenthesis

  private[analyzer] def filterStatement(condition: String, child: String): String =
    projectStatement(Seq.empty, child) + _Where + condition

  private[analyzer] def projectAndFilterStatement(
      project: Seq[String],
      condition: String,
      child: String
  ): String =
    _Select + (if (project.isEmpty) _Star else project.mkString(_Comma)) + _From +
      _LeftParenthesis + child + _RightParenthesis + _Where + condition

  private[analyzer] def updateStatement(
      tableName: String,
      assignments: Map[String, String],
      condition: Option[String],
      sourceData: Option[String]
  ): String = {
    _Update + tableName +
      _Set + assignments.toSeq.map { case (k, v) => k + _Equals + v }.mkString(_Comma) +
      (if (sourceData.isDefined) {
         _From + _LeftParenthesis + sourceData.get + _RightParenthesis
       } else _EmptyString) +
      (if (condition.isDefined) _Where + condition.get else _EmptyString)
  }

  private[analyzer] def deleteStatement(
      tableName: String,
      condition: Option[String],
      sourceData: Option[String]
  ): String = {
    _Delete + _From + tableName +
      (if (sourceData.isDefined) {
         _Using + _LeftParenthesis + sourceData.get + _RightParenthesis
       } else _EmptyString) +
      (if (condition.isDefined) _Where + condition.get else _EmptyString)
  }

  private[analyzer] def insertMergeStatement(
      condition: Option[String],
      keys: Seq[String],
      values: Seq[String]
  ): String =
    _When + _Not + _Matched +
      (if (condition.isDefined) _And + condition.get else _EmptyString) +
      _Then + _Insert +
      (if (keys.nonEmpty) {
         _LeftParenthesis + keys.mkString(_Comma) + _RightParenthesis
       } else _EmptyString) +
      _Values + _LeftParenthesis + values.mkString(_Comma) + _RightParenthesis

  private[analyzer] def updateMergeStatement(
      condition: Option[String],
      assignments: Map[String, String]
  ) =
    _When + _Matched + (if (condition.isDefined) _And + condition.get else _EmptyString) +
      _Then + _Update + _Set + assignments.toSeq
        .map { case (k, v) =>
          k + _Equals + v
        }
        .mkString(_Comma)

  private[analyzer] def deleteMergeStatement(condition: Option[String]) =
    _When + _Matched + (if (condition.isDefined) _And + condition.get else _EmptyString) +
      _Then + _Delete

  private[analyzer] def mergeStatement(
      tableName: String,
      source: String,
      joinExpr: String,
      clauses: Seq[String]
  ): String = {
    _Merge + _Into + tableName + _Using + _LeftParenthesis + source + _RightParenthesis +
      _On + joinExpr + clauses.mkString(_EmptyString)
  }

  private[analyzer] def sampleStatement(
      probabilityFraction: Option[Double],
      rowCount: Option[Long],
      child: String
  ): String =
    if (probabilityFraction.isDefined) {
      // Snowflake uses percentage as probability
      projectStatement(Seq.empty, child) + _Sample +
        _LeftParenthesis + (probabilityFraction.get * 100) + _RightParenthesis
    } else if (rowCount.isDefined) {
      projectStatement(Seq.empty, child) + _Sample +
        _LeftParenthesis + rowCount.get + _Rows + _RightParenthesis
    } else {
      throw ErrorMessage.PLAN_SAMPLING_NEED_ONE_PARAMETER()
    }

  private[analyzer] def aggregateStatement(
      groupingExpressions: Seq[String],
      aggregatedExpressions: Seq[String],
      child: String
  ): String =
    projectStatement(aggregatedExpressions, child) +
      // add limit 1 because user may aggregate on non-aggregate function in a scalar aggregation
      // for example, df.agg(lit(1))
      (if (groupingExpressions.isEmpty) limitExpression(1)
       else _GroupBy + groupingExpressions.mkString(_Comma))

  private[analyzer] def sortStatement(order: Seq[String], child: String): String =
    projectStatement(Seq.empty, child) + _OrderBy + order.mkString(_Comma)

  private[analyzer] def generator(columns: Seq[String], rowCount: Long): String =
    projectStatement(columns, table(generator(rowCount)))

  private[analyzer] def rangeStatement(
      start: Long,
      end: Long,
      step: Long,
      columnName: String
  ): String = {
    // use BigInt for extreme case Long.Min to Long.Max
    val range = BigInt(end) - BigInt(start)
    val count =
      if (range * step <= 0) {
        0
      } else {
        (range / BigInt(step)).toLong +
          (if (
             range % BigInt(step) != 0 // ceil
             && range * step > 0 // has result
           ) {
             1
           } else {
             0
           })
      }

    projectStatement(
      Seq(
        _LeftParenthesis + _RowNumber + _Over + _LeftParenthesis + _OrderBy + _Seq8 +
          _RightParenthesis + _Minus + _One + _RightParenthesis + _Star + _LeftParenthesis +
          step + _RightParenthesis + _Plus + _LeftParenthesis + start + _RightParenthesis +
          _As + columnName
      ),
      table(generator(if (count < 0) 0 else count))
    )
  }

  private[analyzer] def valuesStatement(output: Seq[Attribute], data: Seq[Row]): String = {
    val tableName = randomNameForTempObject(TempObjectType.Table)
    val types = output.map(_.dataType)
    val rows = data.map { row =>
      val cells = row.toSeq.zip(types).map { case (v, dType) =>
        DataTypeMapper.toSql(v, Option(dType))
      }
      cells.mkString(_LeftParenthesis, _Comma, _RightParenthesis)
    }

    val querySource = _Values + rows.mkString(_Comma) +
      _As + tableName +
      output.map(_.name).map(quoteName).mkString(_LeftParenthesis, _Comma, _RightParenthesis)

    projectStatement(Seq.empty, querySource)
  }

  private[analyzer] def emptyValuesStatement(output: Seq[Attribute]): String = {
    // Create a Row with null value in each column
    val data = Seq(Row.fromSeq(output.map(_ => null)))
    filterStatement(_UnsatFilter, valuesStatement(output, data))
  }

  private[analyzer] def setOperatorStatement(
      left: String,
      right: String,
      operator: String
  ): String = {
    _LeftParenthesis + left + _RightParenthesis + _Space + operator + _Space +
      _LeftParenthesis + right + _RightParenthesis
  }

  private[analyzer] def setOperatorStatement(children: Seq[String], operator: String): String =
    children
      .map(c => _LeftParenthesis + c + _RightParenthesis)
      .mkString(_Space + operator + _Space)

  private[analyzer] def unionStatement(children: Seq[String]): String = {
    children.reduceLeft((left, right) => setOperatorStatement(left, right, _UnionAll))
  }

  private def leftSemiOrAntiJoinStatement(
      left: String,
      right: String,
      joinType: JoinType,
      condition: Option[String]
  ): String = {

    val leftAlias = randomNameForTempObject(TempObjectType.Table)
    val rightAlias = randomNameForTempObject(TempObjectType.Table)

    val whereCondition = if (joinType == LeftSemi) {
      _Where + _Exists
    } else if (joinType == LeftAnti) {
      _Where + _Not + _Exists
    }

    // This generates sql like "Where a = b"
    val joinCondition = condition.map(_Where + _)

    val result = _Select + _Star +
      _From + _LeftParenthesis + left + _RightParenthesis + _As + leftAlias +
      whereCondition +
      _LeftParenthesis +
      _Select + _Star + _From + _LeftParenthesis + right + _RightParenthesis + _As + rightAlias +
      joinCondition.getOrElse(_EmptyString) +
      _RightParenthesis

    result
  }

  private def snowflakeSupportedJoinsStatement(
      left: String,
      right: String,
      joinType: JoinType,
      condition: Option[String]
  ): String = {

    val leftAlias = randomNameForTempObject(TempObjectType.Table)
    val rightAlias = randomNameForTempObject(TempObjectType.Table)
    val joinSql = joinType match {
      case UsingJoin(tpe, _) => tpe.sql
      case NaturalJoin(tpe) =>
        _Natural + tpe.sql
      case a @ _ => a.sql
    }

    // This generates sql like "USING(a, b)"
    val usingCondition: Option[String] = joinType match {
      case UsingJoin(_, cols) if (!cols.isEmpty) =>
        Some(_Using + _LeftParenthesis + cols.mkString(_Comma) + _RightParenthesis)
      case _ => None
    }

    // This generates sql like "ON a = b"
    val joinCondition = condition.map(_On + _)

    if (usingCondition.isDefined && joinCondition.isDefined) {
      throw ErrorMessage.PLAN_JOIN_NEED_USING_CLAUSE_OR_JOIN_CONDITION()
    }

    val source =
      _LeftParenthesis + left + _RightParenthesis + _As + leftAlias +
        _Space + joinSql + _Join +
        _LeftParenthesis + right + _RightParenthesis + _As + rightAlias +
        usingCondition.getOrElse(_EmptyString) + joinCondition.getOrElse(_EmptyString)

    /*
     * The final join sql looks like this when the left and the right are tables t1 and t2
     *
     * SELECT  *  FROM (
     *          ( SELECT  *  FROM (T1)) AS SN_TEMP_OBJECT_58316588
     *          INNER JOIN
     *           ( SELECT  *  FROM (T2)) AS SN_TEMP_OBJECT_1551097495
     *           USING (a)
     *         )
     */
    projectStatement(Seq.empty, source, false)

  }

  private[analyzer] def joinStatement(
      left: String,
      right: String,
      joinType: JoinType,
      condition: Option[String]
  ): String = {

    joinType match {
      case LeftSemi =>
        leftSemiOrAntiJoinStatement(left, right, LeftSemi, condition)
      case LeftAnti =>
        leftSemiOrAntiJoinStatement(left, right, LeftAnti, condition)
      case UsingJoin(LeftSemi, usingCols) =>
        throw ErrorMessage.PLAN_LEFT_SEMI_JOIN_NOT_SUPPORT_USING_CLAUSE()
      case UsingJoin(LeftAnti, usingCols) =>
        throw ErrorMessage.PLAN_LEFT_ANTI_JOIN_NOT_SUPPORT_USING_CLAUSE()
      case _ => snowflakeSupportedJoinsStatement(left, right, joinType, condition)
    }
  }

  private[analyzer] def subQueryAliasStatement(name: String, child: String): String =
    projectStatement(Seq.empty, child) + _As + name

  private[analyzer] def createTableStatement(
      tableName: String,
      schema: String,
      replace: Boolean = false,
      error: Boolean = true,
      tempType: TempType = TempType.Permanent
  ): String =
    _Create + (if (replace) _Or + _Replace else _EmptyString) + tempType + _Table + tableName +
      (if (!replace && !error) _If + _Not + _Exists else _EmptyString) + _LeftParenthesis +
      schema + _RightParenthesis

  private[analyzer] def insertIntoStatement(tableName: String, child: String): String =
    _Insert + _Into + tableName + projectStatement(Seq.empty, child)

  private[analyzer] def batchInsertIntoStatement(
      tableName: String,
      columnNames: Seq[String]
  ): String = {
    val columns = columnNames.mkString(_Comma)
    val questionMarks = columnNames
      .map { _ =>
        _QuestionMark
      }
      .mkString(_Comma)
    _Insert + _Into + tableName + _LeftParenthesis + columns + _RightParenthesis +
      _Values + _LeftParenthesis + questionMarks + _RightParenthesis
  }

  private[analyzer] def createTableAsSelectStatement(
      tableName: String,
      child: String,
      replace: Boolean = false,
      error: Boolean = true
  ): String =
    _Create + (if (replace) _Or + _Replace else _EmptyString) + _Table +
      (if (!replace && !error) _If + _Not + _Exists else _EmptyString) + tableName + _As +
      projectStatement(Seq.empty, child)

  private[analyzer] def limitOnSortStatement(
      child: String,
      rowCount: String,
      order: Seq[String]
  ): String =
    projectStatement(Seq.empty, child) + _OrderBy + order.mkString(_Comma) + _Limit + rowCount

  private[analyzer] def limitStatement(rowCount: String, child: String): String =
    projectStatement(Seq.empty, child) + _Limit + rowCount

  private[analyzer] def schemaCastSeq(schema: Seq[Attribute]): Seq[String] = {
    schema.zipWithIndex.map { case (attr, index) =>
      val name = _Dollar + (index + 1) + _DoubleColon +
        convertToSFType(attr.dataType)
      name + _As + quoteName(attr.name)
    }
  }

  private[analyzer] def createFileFormatStatement(
      formatName: String,
      fileType: String,
      options: Map[String, String],
      tempType: TempType,
      ifNotExist: Boolean = false
  ): String = {
    val optionsStr = _Type + _Equals + fileType + getOptionsStatement(options)
    _Create + tempType + _File + _Format +
      (if (ifNotExist) _If + _Not + _Exists else "") + formatName + optionsStr
  }

  private[analyzer] def fileOperationStatement(
      command: FileOperationCommand,
      fileName: String,
      stageLocation: String,
      options: Map[String, String]
  ): String =
    command match {
      case PutCommand =>
        _Put + fileName + _Space + stageLocation + _Space + getOptionsStatement(options)
      case GetCommand =>
        _Get + stageLocation + _Space + fileName + _Space + getOptionsStatement(options)
      case _ =>
        throw ErrorMessage.PLAN_UNSUPPORTED_FILE_OPERATION_TYPE()
    }

  private def getOptionsStatement(options: Map[String, String]): String =
    options
      .map { case (k, v) => k + _Space + _Equals + _Space + v }
      .mkString(_Space, _Space, _Space)

  private[analyzer] def dropFileFormatIfExistStatement(formatName: String): String = {
    _Drop + _File + _Format + _If + _Exists + formatName
  }

  private[analyzer] def selectFromPathWithFormatStatement(
      project: Seq[String],
      path: String,
      formatName: Option[String],
      pattern: Option[String]
  ): String = {
    val selectStatement = _Select +
      (if (project.isEmpty) _Star else project.mkString(_Comma)) + _From + path
    val formatStatement = formatName.map(name => _FileFormat + _RightArrow + singleQuote(name))
    val patternStatement = pattern.map(expr => _Pattern + _RightArrow + singleQuote(expr))
    selectStatement +
      (if (formatStatement.nonEmpty || patternStatement.nonEmpty) {
         _LeftParenthesis + formatStatement.getOrElse(_EmptyString) +
           (if (formatStatement.nonEmpty && patternStatement.nonEmpty) _Comma else _EmptyString) +
           patternStatement.getOrElse(_EmptyString) + _RightParenthesis
       } else _EmptyString)
  }
  private[analyzer] def createOrReplaceViewStatement(
      name: String,
      child: String,
      tempType: TempType
  ): String =
    _Create + _Or + _Replace + tempType +
      _View + name + _As + projectStatement(Seq.empty, child)

  private[analyzer] def pivotStatement(
      pivotColumn: String,
      pivotValues: Seq[String],
      aggregate: String,
      child: String
  ): String =
    _Select + _Star + _From + _LeftParenthesis + child + _RightParenthesis + _Pivot +
      _LeftParenthesis + aggregate + _For + pivotColumn + _In +
      pivotValues.mkString(_LeftParenthesis, _Comma, _RightParenthesis) + _RightParenthesis

  /** copy into <tableName> from <filePath> file_format = (type = <format> <formatTypeOptions>)
    * <copyOptions>
    */
  private[snowpark] def copyIntoTable(
      tableName: String,
      filePath: String,
      format: String,
      formatTypeOptions: Map[String, String],
      copyOptions: Map[String, String],
      pattern: Option[String],
      columnNames: Seq[String],
      transformations: Seq[String]
  ): String = {
    _Copy + _Into + tableName +
      (if (columnNames.nonEmpty) {
         columnNames.mkString(_LeftParenthesis, _Comma, _RightParenthesis)
       } else {
         _EmptyString
       }) + _From +
      (if (transformations.nonEmpty) {
         _LeftParenthesis + _Select + transformations.mkString(_Comma) +
           _From + filePath + _RightParenthesis
       } else {
         filePath
       }) +
      pattern.map(expr => _Pattern + _Equals + singleQuote(expr)).getOrElse(_EmptyString) +
      _FileFormat + _Equals + _LeftParenthesis + _Type + _Equals + format +
      (if (formatTypeOptions.nonEmpty) {
         formatTypeOptions
           .map { case (k, v) =>
             s"$k = $v"
           }
           .mkString(_Space, _Space, _Space)
       } else "") +
      _RightParenthesis +
      (if (copyOptions.nonEmpty) {
         copyOptions
           .map { case (k, v) =>
             s"$k = $v"
           }
           .mkString(_Space, _Space, _Space)
       } else "")
  }

  private[snowpark] def dropTableIfExistsStatement(tableName: String): String =
    _Drop + _Table + _If + _Exists + tableName

  // utils
  private[analyzer] def attributeToSchemaString(attributes: Seq[Attribute]): String =
    attributes
      .map(attr => quoteName(attr.name) + _Space + convertToSFType(attr.dataType))
      .mkString(_Comma)

  // use values to represent schema
  private[snowpark] def schemaValueStatement(output: Seq[Attribute]): String =
    _Select + output
      .map(attr =>
        DataTypeMapper.schemaExpression(attr.dataType, attr.nullable) +
          _As + quoteName(attr.name)
      )
      .mkString(_Comma)

  private[snowpark] def listAgg(col: String, delimiter: String, isDistinct: Boolean): String =
    _ListAgg + _LeftParenthesis + (if (isDistinct) _Distinct else "") + col +
      _Comma + delimiter + _RightParenthesis

  private def generator(rowCount: Long): String =
    _Generator + _LeftParenthesis + _RowCount + _RightArrow + rowCount + _RightParenthesis

  private def table(content: String, over: Option[String] = None): String =
    _Table + _LeftParenthesis + content +
      over
        .map(window => _Over + _LeftParenthesis + window + _RightParenthesis)
        .getOrElse(_EmptyString) + _RightParenthesis

  def singleQuote(value: String): String = {
    if (value.startsWith(_SingleQuote) && value.endsWith(_SingleQuote)) {
      value
    } else {
      _SingleQuote + value + _SingleQuote
    }
  }

  /** Use this function to normalize all user input and client generated names
    *
    * Rule: Name with quote: Do nothing Without quote: Starts with _A-Za-z or and only contains
    * _A-Za-z0-9$, upper case all letters and quote otherwise, quote without upper casing
    */
  def quoteName(name: String): String = {
    val alreadyQuoted = "^(\".+\")$".r
    val unquotedCaseInsenstive = "^([_A-Za-z]+[_A-Za-z0-9$]*)$".r
    name.trim match {
      case alreadyQuoted(n)          => validateQuotedName(n)
      case unquotedCaseInsenstive(n) =>
        // scalastyle:off caselocale
        _DoubleQuote + escapeQuotes(n.toUpperCase) + _DoubleQuote
      // scalastyle:on caselocale
      case n => _DoubleQuote + escapeQuotes(n) + _DoubleQuote
    }
  }

  private def validateQuotedName(name: String): String = {
    // Snowflake uses 2 double-quote to escape double-quote,
    // if there are any double-quote in the object name, it needs to be 2 double-quote.
    if (name.substring(1, name.length - 1).replaceAll("\"\"", "").contains("\"")) {
      throw ErrorMessage.PLAN_ANALYZER_INVALID_IDENTIFIER(name)
    } else {
      name
    }
  }

  /** Quotes name without upper casing if not quoted NOTE: All characters in name are DATA so "c1"
    * will be converted to """c1""".
    */
  def quoteNameWithoutUpperCasing(name: String): String =
    _DoubleQuote + escapeQuotes(name) + _DoubleQuote

  private def escapeQuotes(unescaped: String): String = {
    unescaped.replaceAll("\"", "\"\"")
  }

  /*
   * Most integer types map to number(38,0)
   * https://docs.snowflake.com/en/sql-reference/
   * data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
   */
  private[analyzer] def number(precision: Integer = 38, scale: Integer = 0): String = {
    _Number + _LeftParenthesis + precision + _Comma + scale + _RightParenthesis
  }

}
