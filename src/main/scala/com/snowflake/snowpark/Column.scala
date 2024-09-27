package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.internal.{ErrorMessage, Logging}
import com.snowflake.snowpark.types.DataType
import com.snowflake.snowpark.functions.lit

// scalastyle:off
/**
 * Represents a column or an expression in a DataFrame.
 *
 * To create a Column object to refer to a column in a DataFrame, you can:
 *
 *   - Use the [[com.snowflake.snowpark.functions.col(colName* functions.col]] function.
 *   - Use the [[com.snowflake.snowpark.DataFrame.col DataFrame.col]] method.
 *   - Use the shorthand for the [[com.snowflake.snowpark.DataFrame.apply(colName* DataFrame.apply]]
 *     method (`<dataframe>`("<col_name>")`).
 *
 * For example:
 *
 * {{{
 *   import com.snowflake.snowpark.functions.col
 *   df.select(col("name"))
 *   df.select(df.col("name"))
 *   dfLeft.select(dfRight, dfLeft("name") === dfRight("name"))
 * }}}
 *
 * This class also defines utility functions for constructing expressions with Columns.
 *
 * The following examples demonstrate how to use Column objects in expressions:
 * {{{
 *   df
 *    .filter(col("id") === 20)
 *    .filter((col("a") + col("b")) < 10)
 *    .select((col("b") * 10) as "c")
 * }}}
 *
 * @groupname utl Utility Functions
 * @groupname op Expression Operation Functions
 * @since 0.1.0
 */
// scalastyle:on
case class Column private[snowpark] (private[snowpark] val expr: Expression) extends Logging {
  private[snowpark] def named: NamedExpression = expr match {
    case expr: NamedExpression => expr
    // we don't need alias, just make it NamedExpression
    case _ => UnresolvedAlias(expr)
  }

  /**
   * Returns a conditional expression that you can pass to the filter or where method to perform the
   * equivalent of a WHERE ... IN query with a specified list of values.
   *
   * The expression evaluates to true if the value in the column is one of the values in a specified
   * sequence.
   *
   * For example, the following code returns a DataFrame that contains the rows where the column "a"
   * contains the value 1, 2, or 3. This is equivalent to SELECT * FROM table WHERE a IN (1, 2, 3).
   * {{{
   *    df.filter(df("a").in(Seq(1, 2, 3)))
   * }}}
   * @group op
   * @since 0.10.0
   */
  def in(values: Seq[Any]): Column = {
    val columnCount = expr match {
      case me: MultipleExpression => me.expressions.size
      case _ => 1
    }
    val valueExpressions = values.map {
      case tuple: Seq[_] =>
        if (tuple.size == columnCount) {
          MultipleExpression(tuple.map(toExpr))
        } else {
          throw ErrorMessage.PLAN_IN_EXPRESSION_INVALID_VALUE_COUNT(tuple.size, columnCount)
        }
      case df: DataFrame =>
        if (df.schema.size == columnCount) {
          ScalarSubquery(df.snowflakePlan)
        } else {
          throw ErrorMessage.PLAN_IN_EXPRESSION_INVALID_VALUE_COUNT(df.schema.size, columnCount)
        }
      case v => toExpr(v)
    }
    if (values.size != 1 || !valueExpressions.head.isInstanceOf[ScalarSubquery]) {
      // If users use sub-query, only 1 sub-query is allowed.
      // Otherwise, all values must be constant values.
      // Snowflake SQL does support to use Column as IN values, but Snowpark disables it because
      // it is kind of confusing. They may be enabled if users request it in the future.
      def validateValue(valueExpr: Expression): Unit = {
        valueExpr match {
          case _: Literal =>
          case me: MultipleExpression => me.expressions.foreach(validateValue)
          case _ => throw ErrorMessage.PLAN_IN_EXPRESSION_UNSUPPORTED_VALUE(valueExpr.toString)
        }
      }
      valueExpressions.foreach(validateValue)
    }
    withExpr(InExpression(expr, valueExpressions))
  }

  /**
   * Returns a conditional expression that you can pass to the filter or where method to perform a
   * WHERE ... IN query with a specified subquery.
   *
   * The expression evaluates to true if the value in the column is one of the values in the column
   * of the same name in a specified DataFrame.
   *
   * For example, the following code returns a DataFrame that contains the rows where the column "a"
   * of `df2` contains one of the values from column "a" in `df1`. This is equivalent to SELECT *
   * FROM table2 WHERE a IN (SELECT a FROM table1).
   * {{{
   *    val df1 = session.table(table1)
   *    val df2 = session.table(table2)
   *    df2.filter(col("a").in(df1))
   * }}}
   *
   * @group op
   * @since 0.10.0
   */
  def in(df: DataFrame): Column = in(Seq(df))

  // scalastyle:off
  /**
   * Returns the specified element (field) in a column that contains
   * [[https://docs.snowflake.com/en/user-guide/semistructured-concepts.html semi-structured data]].
   *
   * The method applies case-sensitive matching to the names of the specified elements.
   *
   * This is equivalent to using
   * [[https://docs.snowflake.com/en/user-guide/querying-semistructured.html#bracket-notation bracket notation in SQL]]
   * (`column['element']`).
   *
   *   - If the column is an OBJECT value, this function extracts the VARIANT value of the element
   *     with the specified name from the OBJECT value.
   *
   *   - If the element is not found, the method returns NULL.
   *
   *   - You must not specify an empty string for the element name.
   *
   *   - If the column is a VARIANT value, this function first checks if the VARIANT value contains
   *     an OBJECT value.
   *
   *   - If the VARIANT value does not contain an OBJECT value, the method returns NULL.
   *
   *   - Otherwise, the method works as described above.
   *
   * For example:
   * {{{
   *   import com.snowflake.snowpark.functions.col
   *   df.select(col("src")("salesperson")("emails")(0))
   * }}}
   *
   * @param field
   *   field name of the subfield to be extracted. You cannot specify a path.
   * @group op
   * @since 0.2.0
   */
  // scalastyle:on
  def apply(field: String): Column = withExpr(SubfieldString(expr, field))

  // scalastyle:off
  /**
   * Returns the element (field) at the specified index in a column that contains
   * [[https://docs.snowflake.com/en/user-guide/semistructured-concepts.html semi-structured data]].
   *
   * The method applies case-sensitive matching to the names of the specified elements.
   *
   * This is equivalent to using
   * [[https://docs.snowflake.com/en/user-guide/querying-semistructured.html#bracket-notation bracket notation in SQL]]
   * (`column[index]`).
   *
   *   - If the column is an ARRAY value, this function extracts the VARIANT value of the array
   *     element at the specified index.
   *
   *   - If the index points outside of the array boundaries or if an element does not exist at the
   *     specified index (e.g. if the array is sparsely populated), the method returns NULL.
   *
   *   - If the column is a VARIANT value, this function first checks if the VARIANT value contains
   *     an ARRAY value.
   *
   *   - If the VARIANT value does not contain an ARRAY value, the method returns NULL.
   *
   *   - Otherwise, the method works as described above.
   *
   * For example:
   * {{{
   *   import com.snowflake.snowpark.functions.col
   *   df.select(col("src")(1)(0)("name")(0))
   * }}}
   *
   * @param idx
   *   index of the subfield to be extracted
   * @group op
   * @since 0.2.0
   */
  // scalastyle:on
  def apply(idx: Int): Column = withExpr(SubfieldInt(expr, idx))

  /**
   * Returns the column name (if the column has a name).
   * @group utl
   * @since 0.2.0
   */
  def getName: Option[String] =
    expr match {
      case namedExpr: NamedExpression => Option(namedExpr.name)
      case _ => None
    }

  /**
   * Returns a string representation of the expression corresponding to this Column instance.
   * @since 0.1.0
   * @group utl
   */
  override def toString: String = s"Column[${expr.toString()}]"

  /**
   * Returns a new renamed Column. Alias for [[name]].
   * @group op
   * @since 0.1.0
   */
  def as(alias: String): Column = name(alias)

  /**
   * Returns a new renamed Column. Alias for [[name]].
   * @group op
   * @since 0.1.0
   */
  def alias(alias: String): Column = name(alias)

  // used by join when column name conflict
  private[snowpark] def internalAlias(alias: String): Column =
    withExpr(Alias(expr, quoteName(alias), isInternal = true))

  /**
   * Returns a new renamed Column.
   * @group op
   * @since 0.1.0
   */
  def name(alias: String): Column = withExpr(Alias(expr, quoteName(alias)))

  /**
   * Unary minus.
   *
   * @group op
   * @since 0.1.0
   */
  def unary_- : Column = withExpr(UnaryMinus(expr))

  /**
   * Unary not.
   * @group op
   * @since 0.1.0
   */
  def unary_! : Column = withExpr(Not(expr))

  /**
   * Equal to. Alias for [[equal_to]]. Use this instead of `==` to perform an equality check in an
   * expression. For example:
   * {{{
   *   lhs.filter(col("a") === 10).join(rhs, rhs("id") === lhs("id"))
   * }}}
   *
   * @group op
   * @since 0.1.0
   */
  def ===(other: Any): Column = withExpr {
    val right = toExpr(other)
    if (this.expr == right) {
      logWarning(
        s"Constructing trivially true equals predicate, '${this.expr} = $right'. '" +
          "Perhaps need to use aliases.")
    }
    EqualTo(expr, right)
  }

  /**
   * Equal to. Same as `===`.
   * @group op
   * @since 0.1.0
   */
  def equal_to(other: Column): Column = this === other

  /**
   * Not equal to. Alias for [[not_equal]].
   *
   * @group op
   * @since 0.1.0
   */
  def =!=(other: Any): Column = withExpr(NotEqualTo(expr, toExpr(other)))

  /**
   * Not equal to.
   * @group op
   * @since 0.1.0
   */
  def not_equal(other: Column): Column = this =!= other

  /**
   * Greater than. Alias for [[gt]].
   * @group op
   * @since 0.1.0
   */
  def >(other: Any): Column = withExpr(GreaterThan(expr, toExpr(other)))

  /**
   * Greater than.
   * @group op
   * @since 0.1.0
   */
  def gt(other: Column): Column = this > other

  /**
   * Less than. Alias for [[lt]].
   * @group op
   * @since 0.1.0
   */
  def <(other: Any): Column = withExpr(LessThan(expr, toExpr(other)))

  /**
   * Less than.
   * @group op
   * @since 0.1.0
   */
  def lt(other: Column): Column = this < other

  /**
   * Less than or equal to. Alias for [[leq]].
   * @group op
   * @since 0.1.0
   */
  def <=(other: Any): Column = withExpr(LessThanOrEqual(expr, toExpr(other)))

  /**
   * Less than or equal to.
   * @group op
   * @since 0.1.0
   */
  def leq(other: Column): Column = this <= other

  /**
   * Greater than or equal to. Alias for [[geq]].
   * @group op
   * @since 0.1.0
   */
  def >=(other: Any): Column = withExpr(GreaterThanOrEqual(expr, toExpr(other)))

  /**
   * Greater than or equal to.
   * @group op
   * @since 0.1.0
   */
  def geq(other: Column): Column = this >= other

  /**
   * Equal to. You can use this for comparisons against a null value. Alias for [[equal_null]].
   *
   * @group op
   * @since 0.1.0
   */
  def <=>(other: Any): Column = withExpr {
    val right = toExpr(other)
    if (this.expr == right) {
      logWarning(
        s"Constructing trivially true equals predicate, '${this.expr} <=> $right'. " +
          "Perhaps need to use aliases.")
    }
    EqualNullSafe(expr, right)
  }

  /**
   * Equal to. You can use this for comparisons against a null value.
   * @group op
   * @since 0.1.0
   */
  def equal_null(other: Column): Column = this <=> other

  /**
   * Is NaN.
   * @group op
   * @since 0.1.0
   */
  def equal_nan: Column = withExpr(IsNaN(expr))

  /**
   * Is null.
   * @group op
   * @since 0.1.0
   */
  def is_null: Column = withExpr(IsNull(expr))

  /**
   * Wrapper for is_null function.
   *
   * @group op
   * @since 1.10.0
   */
  def isNull: Column = is_null

  /**
   * Is not null.
   * @group op
   * @since 0.1.0
   */
  def is_not_null: Column = withExpr(IsNotNull(expr))

  /**
   * Or. Alias for [[or]].
   * @group op
   * @since 0.1.0
   */
  def ||(other: Any): Column = withExpr(Or(expr, toExpr(other)))

  /**
   * Or.
   * @group op
   * @since 0.1.0
   */
  def or(other: Column): Column = this || other

  /**
   * And. Alias for [[and]].
   * @group op
   * @since 0.1.0
   */
  def &&(other: Any): Column = withExpr(And(expr, toExpr(other)))

  /**
   * And.
   * @group op
   * @since 0.1.0
   */
  def and(other: Column): Column = this && other

  /**
   * Between lower bound and upper bound.
   * @group op
   * @since 0.1.0
   */
  def between(lowerBound: Column, upperBound: Column): Column = {
    (this >= lowerBound) && (this <= upperBound)
  }

  /**
   * Plus. Alias for [[plus]].
   * @group op
   * @since 0.1.0
   */
  def +(other: Any): Column = withExpr(Add(expr, toExpr(other)))

  /**
   * Plus.
   * @group op
   * @since 0.1.0
   */
  def plus(other: Column): Column = this + other

  /**
   * Minus. Alias for [[minus]].
   * @group op
   * @since 0.1.0
   */
  def -(other: Any): Column = withExpr(Subtract(expr, toExpr(other)))

  /**
   * Minus.
   * @group op
   * @since 0.1.0
   */
  def minus(other: Column): Column = this - other

  /**
   * Multiply. Alias for [[multiply]].
   * @group op
   * @since 0.1.0
   */
  def *(other: Any): Column = withExpr(Multiply(expr, toExpr(other)))

  /**
   * Multiply.
   * @group op
   * @since 0.1.0
   */
  def multiply(other: Column): Column = this * other

  /**
   * Divide. Alias for [[divide]].
   * @group op
   * @since 0.1.0
   */
  def /(other: Any): Column = withExpr(Divide(expr, toExpr(other)))

  /**
   * Divide.
   * @group op
   * @since 0.1.0
   */
  def divide(other: Column): Column = this / other

  /**
   * Remainder. Alias for [[mod]].
   * @group op
   * @since 0.1.0
   */
  def %(other: Any): Column = withExpr(Remainder(expr, toExpr(other)))

  /**
   * Remainder.
   * @group op
   * @since 0.1.0
   */
  def mod(other: Column): Column = this % other

  /**
   * Casts the values in the Column to the specified data type.
   * @group op
   * @since 0.1.0
   */
  def cast(to: DataType): Column = withExpr(Cast(expr, to))

  /**
   * Returns a Column expression with values sorted in descending order.
   * @group op
   * @since 0.1.0
   */
  def desc: Column = withExpr(SortOrder(expr, Descending))

  /**
   * Returns a Column expression with values sorted in descending order (null values sorted before
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def desc_nulls_first: Column =
    withExpr(SortOrder(expr, Descending, NullsFirst, Set.empty))

  /**
   * Returns a Column expression with values sorted in descending order (null values sorted after
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def desc_nulls_last: Column =
    withExpr(SortOrder(expr, Descending, NullsLast, Set.empty))

  /**
   * Returns a Column expression with values sorted in ascending order.
   *
   * @group op
   * @since 0.1.0
   */
  def asc: Column = withExpr(SortOrder(expr, Ascending))

  /**
   * Returns a Column expression with values sorted in ascending order (null values sorted before
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def asc_nulls_first: Column = withExpr(SortOrder(expr, Ascending, NullsFirst, Set.empty))

  /**
   * Returns a Column expression with values sorted in ascending order (null values sorted after
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def asc_nulls_last: Column = withExpr(SortOrder(expr, Ascending, NullsLast, Set.empty))

  /**
   * Bitwise or.
   *
   * @group op
   * @since 0.1.0
   */
  def bitor(other: Column): Column = withExpr(BitwiseOr(expr, toExpr(other)))

  /**
   * Bitwise and.
   *
   * @group op
   * @since 0.1.0
   */
  def bitand(other: Column): Column = withExpr(BitwiseAnd(expr, toExpr(other)))

  /**
   * Bitwise xor.
   *
   * @group op
   * @since 0.1.0
   */
  def bitxor(other: Column): Column = withExpr(BitwiseXor(expr, toExpr(other)))

  /**
   * Returns a windows frame, based on the specified [[WindowSpec]].
   *
   * @group op
   * @since 0.1.0
   */
  def over(window: WindowSpec): Column = window.withAggregate(expr)

  /**
   * Returns a windows frame, based on an empty [[WindowSpec]] expression.
   *
   * @group op
   * @since 0.1.0
   */
  def over(): Column = over(Window.spec)

  /**
   * Allows case-sensitive matching of strings based on comparison with a pattern.
   *
   * For details, see the Snowflake documentation on
   * [[https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes LIKE]].
   *
   * @group op
   * @since 0.1.0
   */
  def like(pattern: Column): Column = withExpr(Like(this.expr, pattern.expr))

  // scalastyle:off
  /**
   * Returns true if this [[Column]] matches the specified regular expression.
   *
   * For details, see the Snowflake documentation on
   * [[https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes regular expressions]].
   *
   * @group op
   * @since 0.1.0
   */
  // scalastyle:on
  def regexp(pattern: Column): Column = withExpr(RegExp(this.expr, pattern.expr))

  /**
   * Returns a Column expression that adds a WITHIN GROUP clause to sort the rows by the specified
   * columns.
   *
   * This method is supported on Column expressions returned by some of the aggregate functions,
   * including [[functions.array_agg]], LISTAGG(), PERCENTILE_CONT(), and PERCENTILE_DISC().
   *
   * For example:
   * {{{
   *     import com.snowflake.snowpark.functions._
   *     import session.implicits._
   *     // Create a DataFrame from a sequence.
   *     val df = Seq((3, "v1"), (1, "v3"), (2, "v2")).toDF("a", "b")
   *     // Create a DataFrame containing the values in "a" sorted by "b".
   *     val dfArrayAgg = df.select(array_agg(col("a")).withinGroup(col("b")))
   *     // Create a DataFrame containing the values in "a" grouped by "b"
   *     // and sorted by "a" in descending order.
   *     var dfArrayAggWindow = df.select(
   *       array_agg(col("a"))
   *       .withinGroup(col("a").desc)
   *       .over(Window.partitionBy(col("b")))
   *     )
   * }}}
   *
   * For details, see the Snowflake documentation for the aggregate function that you are using
   * (e.g. [[https://docs.snowflake.com/en/sql-reference/functions/array_agg.html ARRAY_AGG]]).
   *
   * @group op
   * @since 0.6.0
   */
  def withinGroup(first: Column, remaining: Column*): Column =
    withinGroup(first +: remaining)

  /**
   * Returns a Column expression that adds a WITHIN GROUP clause to sort the rows by the specified
   * sequence of columns.
   *
   * This method is supported on Column expressions returned by some of the aggregate functions,
   * including [[functions.array_agg]], LISTAGG(), PERCENTILE_CONT(), and PERCENTILE_DISC().
   *
   * For example:
   * {{{
   *     import com.snowflake.snowpark.functions._
   *     import session.implicits._
   *     // Create a DataFrame from a sequence.
   *     val df = Seq((3, "v1"), (1, "v3"), (2, "v2")).toDF("a", "b")
   *     // Create a DataFrame containing the values in "a" sorted by "b".
   *     df.select(array_agg(col("a")).withinGroup(Seq(col("b"))))
   *     // Create a DataFrame containing the values in "a" grouped by "b"
   *     // and sorted by "a" in descending order.
   *     df.select(
   *       array_agg(Seq(col("a")))
   *       .withinGroup(col("a").desc)
   *       .over(Window.partitionBy(col("b")))
   *     )
   * }}}
   *
   * For details, see the Snowflake documentation for the aggregate function that you are using
   * (e.g. [[https://docs.snowflake.com/en/sql-reference/functions/array_agg.html ARRAY_AGG]]).
   *
   * @group op
   * @since 0.6.0
   */
  def withinGroup(cols: Seq[Column]): Column =
    withExpr(WithinGroup(this.expr, cols.map { _.expr }))

  // scalastyle:off
  /**
   * Returns a copy of the original [[Column]] with the specified `collationSpec` property, rather
   * than the original collation specification property.
   *
   * For details, see the Snowflake documentation on
   * [[https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification collation specifications]].
   *
   * @group op
   * @since 0.1.0
   */
  // scalastyle:on
  def collate(collateSpec: String): Column = withExpr(Collate(this.expr, collateSpec))

  private def toExpr(exp: Any): Expression = exp match {
    case c: Column => c.expr
    case _ => lit(exp).expr
  }

  protected def withExpr(newExpr: Expression): Column = Column(newExpr)
}

private[snowpark] object Column {
  def apply(name: String): Column =
    new Column(name match {
      case "*" => Star(Seq.empty)
      case c if c.contains(".") => UnresolvedDFAliasAttribute(name)
      case _ => UnresolvedAttribute(quoteName(name))
    })

  def expr(e: String): Column = new Column(UnresolvedAttribute(e))
}

/**
 * Represents a [[https://docs.snowflake.com/en/sql-reference/functions/case.html CASE]] expression.
 *
 * To construct this object for a CASE expression, call the
 * [[com.snowflake.snowpark.functions.when functions.when]]. specifying a condition and the
 * corresponding result for that condition. Then, call the [[when]] and [[otherwise]] methods to
 * specify additional conditions and results.
 *
 * For example:
 * {{{
 *     import com.snowflake.snowpark.functions._
 *     df.select(
 *       when(col("col").is_null, lit(1))
 *         .when(col("col") === 1, lit(2))
 *         .otherwise(lit(3))
 *     )
 * }}}
 *
 * @since 0.2.0
 */
class CaseExpr private[snowpark] (branches: Seq[(Expression, Expression)])
    extends Column(CaseWhen(branches)) {

  /**
   * Appends one more WHEN condition to the CASE expression.
   *
   * @since 0.2.0
   */
  def when(condition: Column, value: Column): CaseExpr =
    new CaseExpr(branches :+ ((condition.expr, value.expr)))

  /**
   * Sets the default result for this CASE expression.
   *
   * @since 0.2.0
   */
  def otherwise(value: Column): Column = withExpr {
    CaseWhen(branches, Option(value.expr))
  }

  /**
   * Sets the default result for this CASE expression. Alias for [[otherwise]].
   *
   * @since 0.2.0
   */
  def `else`(value: Column): Column = otherwise(value)
}
