package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{AstUtils, ExprNode, Logging, NameIndices, SrcPositionInfo}
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.internal.AstUtils._

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
case class Column(
    override private[snowpark] val expr: Expr,
    override private[snowpark] val nameIndices: Set[Int])
    extends ExprNode
    with Logging {

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
  def in(values: Seq[Any])(implicit src: SrcPositionInfo): Column = {
    val childNameIndices: collection.mutable.Set[Int] =
      collection.mutable.Set(AstUtils.filenameTable.getFileId(src.filename)) ++ this.nameIndices

    Column(
      Expr.Variant.ColumnIn(
        ColumnIn(
          col = Some(expr),
          src = createSrcPosition(src),
          values = values.map {
            // todo: SNOW-1974661 add support for dataframe and tuple literals
            case df: DataFrame => null
            case tuple: Seq[_] => null
            case v: NameIndices =>
              childNameIndices ++= v.nameIndices
              createExpr(v, src)
            case v => createExpr(v, src)
          })),
      childNameIndices.toSet)
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
  def in(df: DataFrame)(implicit src: SrcPositionInfo): Column = in(Seq(df))(src)

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
  def apply(field: String)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnApplyString(
        ColumnApply_String(col = Some(expr), field = field, src = createSrcPosition(src))),
      src)

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

  def apply(idx: Int)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnApplyInt(
        ColumnApply_Int(col = Some(expr), idx = idx, src = createSrcPosition(src))),
      src)

  /**
   * Returns the column name (if the column has a name).
   * @group utl
   * @since 0.2.0
   */
  def getName: Option[String] = null // todo: snow-1992311

  /**
   * Returns a string representation of the expression corresponding to this Column instance.
   * @since 0.1.0
   * @group utl
   */
  override def toString: String = "" // todo: snow-1992311

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

  /**
   * Returns a new renamed Column.
   * @group op
   * @since 0.1.0
   */
  def name(alias: String): Column = null // todo: snow-1992314

  /**
   * Unary minus.
   *
   * @group op
   * @since 0.1.0
   */
  def unary_-(implicit src: SrcPositionInfo): Column =
    createColumn(Expr.Variant.Neg(Neg(operand = Some(expr), src = createSrcPosition(src))), src)

  /**
   * Unary not.
   * @group op
   * @since 0.1.0
   */
  def unary_!(implicit src: SrcPositionInfo): Column =
    createColumn(Expr.Variant.Not(Not(operand = Some(expr), src = createSrcPosition(src))), src)

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
  def ===(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Eq(
        Eq(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Equal to. Same as `===`.
   * @group op
   * @since 0.1.0
   */
  def equal_to(other: Column)(implicit src: SrcPositionInfo): Column = (this === other)(src)

  /**
   * Not equal to. Alias for [[not_equal]].
   *
   * @group op
   * @since 0.1.0
   */
  def =!=(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Neq(
        Neq(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Not equal to.
   * @group op
   * @since 0.1.0
   */
  def not_equal(other: Column)(implicit src: SrcPositionInfo): Column = (this =!= other)(src)

  /**
   * Greater than. Alias for [[gt]].
   * @group op
   * @since 0.1.0
   */
  def >(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Gt(
        Gt(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Greater than.
   * @group op
   * @since 0.1.0
   */
  def gt(other: Column)(implicit src: SrcPositionInfo): Column = (this > other)(src)

  /**
   * Less than. Alias for [[lt]].
   * @group op
   * @since 0.1.0
   */
  def <(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Lt(
        Lt(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Less than.
   * @group op
   * @since 0.1.0
   */
  def lt(other: Column)(implicit src: SrcPositionInfo): Column = (this < other)(src)

  /**
   * Less than or equal to. Alias for [[leq]].
   * @group op
   * @since 0.1.0
   */
  def <=(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Leq(
        Leq(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Less than or equal to.
   * @group op
   * @since 0.1.0
   */
  def leq(other: Column)(implicit src: SrcPositionInfo): Column = (this <= other)(src)

  /**
   * Greater than or equal to. Alias for [[geq]].
   * @group op
   * @since 0.1.0
   */
  def >=(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Geq(
        Geq(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Greater than or equal to.
   * @group op
   * @since 0.1.0
   */
  def geq(other: Column)(implicit src: SrcPositionInfo): Column = (this >= other)(src)

  /**
   * Equal to. You can use this for comparisons against a null value. Alias for [[equal_null]].
   *
   * @group op
   * @since 0.1.0
   */
  def <=>(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnEqualNull(
        ColumnEqualNull(
          lhs = Some(expr),
          rhs = Some(createExpr(other, src)),
          src = createSrcPosition(src))),
      src,
      other)

  /**
   * Equal to. You can use this for comparisons against a null value.
   * @group op
   * @since 0.1.0
   */
  def equal_null(other: Column)(implicit src: SrcPositionInfo): Column = (this <=> other)(src)

  /**
   * Is NaN.
   * @group op
   * @since 0.1.0
   */
  def equal_nan(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnEqualNan(ColumnEqualNan(col = Some(expr), src = createSrcPosition(src))),
      src)

  /**
   * Is null.
   * @group op
   * @since 0.1.0
   */
  def is_null(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnIsNull(ColumnIsNull(col = Some(expr), src = createSrcPosition(src))),
      src)

  /**
   * Wrapper for is_null function.
   *
   * @group op
   * @since 1.10.0
   */
  def isNull(implicit src: SrcPositionInfo): Column = is_null(src)

  /**
   * Is not null.
   * @group op
   * @since 0.1.0
   */
  def is_not_null(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnIsNotNull(ColumnIsNotNull(col = Some(expr), src = createSrcPosition(src))),
      src)

  /**
   * Or. Alias for [[or]].
   * @group op
   * @since 0.1.0
   */
  def ||(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Or(
        Or(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src)

  /**
   * Or.
   * @group op
   * @since 0.1.0
   */
  def or(other: Column)(implicit src: SrcPositionInfo): Column = (this || other)(src)

  /**
   * And. Alias for [[and]].
   * @group op
   * @since 0.1.0
   */
  def &&(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.And(
        And(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * And.
   * @group op
   * @since 0.1.0
   */
  def and(other: Column)(implicit src: SrcPositionInfo): Column = (this && other)(src)

  /**
   * Between lower bound and upper bound.
   * @group op
   * @since 0.1.0
   */
  def between(lowerBound: Column, upperBound: Column)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnBetween(
        ColumnBetween(
          col = Some(expr),
          lowerBound = Some(lowerBound.expr),
          upperBound = Some(upperBound.expr),
          src = createSrcPosition(src))),
      src,
      lowerBound,
      upperBound)

  /**
   * Plus. Alias for [[plus]].
   * @group op
   * @since 0.1.0
   */
  def +(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Add(
        Add(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Plus.
   * @group op
   * @since 0.1.0
   */
  def plus(other: Column)(implicit src: SrcPositionInfo): Column = (this + other)(src)

  /**
   * Minus. Alias for [[minus]].
   * @group op
   * @since 0.1.0
   */
  def -(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Sub(
        Sub(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Minus.
   * @group op
   * @since 0.1.0
   */
  def minus(other: Column)(implicit src: SrcPositionInfo): Column = (this - other)(src)

  /**
   * Multiply. Alias for [[multiply]].
   * @group op
   * @since 0.1.0
   */
  def *(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Mul(
        Mul(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Multiply.
   * @group op
   * @since 0.1.0
   */
  def multiply(other: Column)(implicit src: SrcPositionInfo): Column = (this * other)(src)

  /**
   * Divide. Alias for [[divide]].
   * @group op
   * @since 0.1.0
   */
  def /(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Div(
        Div(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Divide.
   * @group op
   * @since 0.1.0
   */
  def divide(other: Column)(implicit src: SrcPositionInfo): Column = (this / other)(src)

  /**
   * Remainder. Alias for [[mod]].
   * @group op
   * @since 0.1.0
   */
  def %(other: Any)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.Mod(
        Mod(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Remainder.
   * @group op
   * @since 0.1.0
   */
  def mod(other: Column)(implicit src: SrcPositionInfo): Column = (this % other)(src)

  /**
   * Casts the values in the Column to the specified data type.
   * @group op
   * @since 0.1.0
   */
  def cast(to: types.DataType): Column = null

  /**
   * Returns a Column expression with values sorted in descending order.
   * @group op
   * @since 0.1.0
   */
  def desc(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnDesc(
        ColumnDesc(
          col = Some(expr),
          nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderDefault(true))),
          src = createSrcPosition(src))),
      src)

  /**
   * Returns a Column expression with values sorted in descending order (null values sorted before
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def desc_nulls_first(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnDesc(
        ColumnDesc(
          col = Some(expr),
          nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsFirst(true))),
          src = createSrcPosition(src))),
      src)

  /**
   * Returns a Column expression with values sorted in descending order (null values sorted after
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def desc_nulls_last(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnDesc(
        ColumnDesc(
          col = Some(expr),
          nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsLast(true))),
          src = createSrcPosition(src))),
      src)

  /**
   * Returns a Column expression with values sorted in ascending order.
   *
   * @group op
   * @since 0.1.0
   */
  def asc(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnAsc(
        ColumnAsc(
          col = Some(expr),
          nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderDefault(true))),
          src = createSrcPosition(src))),
      src)

  /**
   * Returns a Column expression with values sorted in ascending order (null values sorted before
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def asc_nulls_first(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnAsc(
        ColumnAsc(
          col = Some(expr),
          nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsFirst(true))),
          src = createSrcPosition(src))),
      src)

  /**
   * Returns a Column expression with values sorted in ascending order (null values sorted after
   * non-null values).
   *
   * @group op
   * @since 0.1.0
   */
  def asc_nulls_last(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnAsc(
        ColumnAsc(
          col = Some(expr),
          nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsLast(true))),
          src = createSrcPosition(src))),
      src)

  /**
   * Bitwise or.
   *
   * @group op
   * @since 0.1.0
   */
  def bitor(other: Column)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.BitOr(
        BitOr(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Bitwise and.
   *
   * @group op
   * @since 0.1.0
   */
  def bitand(other: Column)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.BitAnd(
        BitAnd(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Bitwise xor.
   *
   * @group op
   * @since 0.1.0
   */
  def bitxor(other: Column)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.BitXor(
        BitXor(lhs = Some(expr), rhs = Some(createExpr(other, src)), src = createSrcPosition(src))),
      src,
      other)

  /**
   * Returns a windows frame, based on the specified [[WindowSpec]].
   *
   * @group op
   * @since 0.1.0
   */
//  def over(window: WindowSpec): Column = window.withAggregate(expr) todo: snow-1992325

  /**
   * Returns a windows frame, based on an empty [[WindowSpec]] expression.
   *
   * @group op
   * @since 0.1.0
   */
  def over(): Column = null // todo: snow-1992325

  /**
   * Allows case-sensitive matching of strings based on comparison with a pattern.
   *
   * For details, see the Snowflake documentation on
   * [[https://docs.snowflake.com/en/sql-reference/functions/like.html#usage-notes LIKE]].
   *
   * @group op
   * @since 0.1.0
   */
  def like(pattern: Column)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnStringLike(
        ColumnStringLike(
          col = Some(expr),
          pattern = Some(createExpr(pattern, src)),
          src = createSrcPosition(src))),
      src,
      pattern)

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
  def regexp(pattern: Column)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnRegexp(
        ColumnRegexp(
          col = Some(expr),
          pattern = Some(createExpr(pattern, src)),
          src = createSrcPosition(src))),
      src,
      pattern)

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
  def withinGroup(first: Column, remaining: Column*)(implicit src: SrcPositionInfo): Column =
    withinGroup(first +: remaining)(src)

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
  def withinGroup(cols: Seq[Column])(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnWithinGroup(
        ColumnWithinGroup(
          col = Some(expr),
          cols = Some(ExprArgList(cols.map(col => col.expr))),
          src = createSrcPosition(src))),
      src,
      cols: _*)

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
  def collate(collateSpec: String)(implicit src: SrcPositionInfo): Column =
    createColumn(
      Expr.Variant.ColumnStringCollate(
        ColumnStringCollate(
          col = Some(expr),
          collationSpec = Some(createExpr(collateSpec, src)),
          src = createSrcPosition(src))),
      src)

  // include all child elements when creating a Column to update all
  // the interned value table.
  private def createColumn(
      variant: Expr.Variant,
      srcPositionInfo: SrcPositionInfo,
      children: Any*): Column = {
    val childNameIndices: Set[Int] =
      children
        .map {
          case child: NameIndices => child.nameIndices
          case _ => Set.empty[Int]
        }
        .foldLeft(Set.empty[Int])(_ ++ _) ++ this.nameIndices

    Column(
      variant,
      Set(AstUtils.filenameTable.getFileId(srcPositionInfo.filename)) ++
        childNameIndices)
  }
}

private[snowpark] object Column {
  def apply(variant: Expr.Variant, nameIndices: Set[Int]): Column = {
    Column(Expr(variant), nameIndices)
  }
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
// todo: snow-1992329
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
