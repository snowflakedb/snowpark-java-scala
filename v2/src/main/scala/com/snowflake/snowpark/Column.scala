package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{AstNode, Logging}
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.proto.ast.Expr.Variant
import com.snowflake.snowpark.types.DataType

// scalastyle:off
/**
 * Represents a column or an expression in a DataFrame.
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
case class Column(override private[snowpark] val ast: Expr) extends AstNode with Logging {

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
  def in(values: Seq[Any]): Column = null
//    createColumn {
//    Variant.ColumnIn(
//      ColumnIn(
//        col = Some(ast),
//        values = values.map {
//          case tuple: Seq[_] => null
//          case df: DataFrame => null
//          case v => createExpr(v)
//        }))
//  }

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
  def apply(field: String): Column = null

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
  def apply(idx: Int): Column = null

  /**
   * Returns the column name (if the column has a name).
   * @group utl
   * @since 0.2.0
   */
  def getName: Option[String] = null

  /**
   * Returns a string representation of the expression corresponding to this Column instance.
   * @since 0.1.0
   * @group utl
   */
  override def toString: String = "" // s"Column[${expr.toString()}]"

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
  private[snowpark] def internalAlias(alias: String): Column = null

  /**
   * Returns a new renamed Column.
   * @group op
   * @since 0.1.0
   */
  def name(alias: String): Column = null

  /**
   * Unary minus.
   *
   * @group op
   * @since 0.1.0
   */
  def unary_- : Column = null

  /**
   * Unary not.
   * @group op
   * @since 0.1.0
   */
  def unary_! : Column = null

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
  def ===(other: Any): Column = null

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
  def =!=(other: Any): Column = null

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
  def >(other: Any): Column = null

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
  def <(other: Any): Column = null

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
  def <=(other: Any): Column = null

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
  def >=(other: Any): Column = null

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
  def <=>(other: Any): Column = null

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
  def equal_nan: Column = null

  /**
   * Is null.
   * @group op
   * @since 0.1.0
   */
  def is_null: Column = null

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
  def is_not_null: Column = null

  /**
   * Or. Alias for [[or]].
   * @group op
   * @since 0.1.0
   */
  def ||(other: Any): Column = null

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
  def &&(other: Any): Column = null

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
  def +(other: Any): Column = null

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
  def -(other: Any): Column = null

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
  def *(other: Any): Column = null

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
  def /(other: Any): Column = null

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
  def %(other: Any): Column = null

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
  def cast(to: DataType): Column = null

  /**
   * Returns a Column expression with values sorted in descending order.
   * @group op
   * @since 0.1.0
   */
  def desc: Column = null

  /**
   * Returns a Column expression with values sorted in descending order (null values sorted before
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def desc_nulls_first: Column = null

  /**
   * Returns a Column expression with values sorted in descending order (null values sorted after
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def desc_nulls_last: Column = null

  /**
   * Returns a Column expression with values sorted in ascending order.
   *
   * @group op
   * @since 0.1.0
   */
  def asc: Column = null

  /**
   * Returns a Column expression with values sorted in ascending order (null values sorted before
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def asc_nulls_first: Column = null

  /**
   * Returns a Column expression with values sorted in ascending order (null values sorted after
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def asc_nulls_last: Column = null

  /**
   * Bitwise or.
   *
   * @group op
   * @since 0.1.0
   */
  def bitor(other: Column): Column = null

  /**
   * Bitwise and.
   *
   * @group op
   * @since 0.1.0
   */
  def bitand(other: Column): Column = null

  /**
   * Bitwise xor.
   *
   * @group op
   * @since 0.1.0
   */
  def bitxor(other: Column): Column = null

  /**
   * Returns a windows frame, based on the specified [[WindowSpec]].
   *
   * @group op
   * @since 0.1.0
   */
//  def over(window: WindowSpec): Column = window.withAggregate(expr)

  /**
   * Returns a windows frame, based on an empty [[WindowSpec]] expression.
   *
   * @group op
   * @since 0.1.0
   */
  def over(): Column = null

  /**
   * Allows case-sensitive matching of strings based on comparison with a pattern.
   *
   * For details, see the Snowflake documentation on
   * [[https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes LIKE]].
   *
   * @group op
   * @since 0.1.0
   */
  def like(pattern: Column): Column = null

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
  def regexp(pattern: Column): Column = null

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
  def withinGroup(cols: Seq[Column]): Column = null

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
  def collate(collateSpec: String): Column = null

}

// private[snowpark] object Column {
//  def apply(name: String): Column =
//    new Column(name match {
//      case "*" => Star(Seq.empty)
//      case c if c.contains(".") => UnresolvedDFAliasAttribute(name)
//      case _ => UnresolvedAttribute(quoteName(name))
//    })
//
//  def expr(e: String): Column = new Column(UnresolvedAttribute(e))
// }

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
// class CaseExpr private[snowpark] (branches: Seq[(Expression, Expression)])
//    extends Column(CaseWhen(branches)) {
//
//  /**
//   * Appends one more WHEN condition to the CASE expression.
//   *
//   * @since 0.2.0
//   */
//  def when(condition: Column, value: Column): CaseExpr =
//    new CaseExpr(branches :+ ((condition.expr, value.expr)))
//
//  /**
//   * Sets the default result for this CASE expression.
//   *
//   * @since 0.2.0
//   */
//  def otherwise(value: Column): Column = withExpr {
//    CaseWhen(branches, Option(value.expr))
//  }
//
//  /**
//   * Sets the default result for this CASE expression. Alias for [[otherwise]].
//   *
//   * @since 0.2.0
//   */
//  def `else`(value: Column): Column = otherwise(value)
// }
