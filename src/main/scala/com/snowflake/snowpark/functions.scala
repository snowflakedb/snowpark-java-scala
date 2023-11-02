package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.internal.ScalaFunctions._
import com.snowflake.snowpark.internal.{ErrorMessage, Utils}

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

/**
 * Provides utility functions that generate [[Column]] expressions that you can pass to
 * [[DataFrame]] transformation methods. These functions generate references to columns,
 * literals, and SQL expressions (e.g. "c + 1").
 *
 * This object also provides functions that correspond to Snowflake
 * [[https://docs.snowflake.com/en/sql-reference-functions.html system-defined functions]]
 * (built-in functions), including functions for aggregation and window functions.
 *
 * The following examples demonstrate the use of some of these functions:
 *
 * {{{
 *   // Use columns and literals in expressions.
 *   df.select(col("c") + lit(1))
 *
 *   // Call system-defined (built-in) functions.
 *   // This example calls the function that corresponds to the ADD_MONTHS() SQL function.
 *   df.select(add_months(col("d"), lit(3)))
 *
 *   // Call system-defined functions that have no corresponding function in the functions object.
 *   // This example calls the RADIANS() SQL function, passing in values from the column "e".
 *   df.select(callBuiltin("radians", col("e")))
 *
 *   // Call a user-defined function (UDF) by name.
 *   df.select(callUDF("some_func", col("c")))
 *
 *   // Register and call an anonymous UDF.
 *   val myudf = udf((x:Int) => x + x)
 *   df.select(myudf(col("c")))
 *
 *   // Evaluate an SQL expression
 *   df.select(sqlExpr("c + 1"))
 * }}}
 *
 * For functions that accept scala types, e.g. callUdf, callBuiltin, lit(),
 * the mapping from scala types to Snowflake types is as follows:
 * {{{
 *   String => String
 *   Byte => TinyInt
 *   Int => Int
 *   Short => SmallInt
 *   Long => BigInt
 *   Float => Float
 *   Double => Double
 *   Decimal => Number
 *   Boolean => Boolean
 *   Array => Array
 *   Timestamp => Timestamp
 *   Date => Date
 * }}}
 *
 * @groupname client_func Client-side Functions
 * @groupname sort_func Sorting Functions
 * @groupname agg_func Aggregate Functions
 * @groupname win_func Window Functions
 * @groupname con_func Conditional Expression Functions
 * @groupname num_func Numeric Functions
 * @groupname gen_func Data Generation Functions
 * @groupname bit_func Bitwise Expression Functions
 * @groupname str_func String and Binary Functions
 * @groupname utl_func Utility and Hash Functions
 * @groupname date_func Date and Time Functions
 * @groupname cont_func Context Functions
 * @groupname semi_func Semi-structured Data Functions
 * @groupname udf_func Anonymous UDF Registration and Invocation Functions
 * @since 0.1.0
 */
// scalastyle:off
object functions {
  // scalastyle:on

  /**
   * Returns the [[Column]] with the specified name.
   *
   * @group client_func
   * @since 0.1.0
   */
  def col(colName: String): Column = Column(colName)

  /**
   * Returns a [[Column]] with the specified name. Alias for col.
   *
   * @group client_func
   * @since 0.1.0
   */
  def column(colName: String): Column = Column(colName)

  /**
   * Generate a [[Column]] representing the result of the input DataFrame.
   * The parameter `df` should have one column and must produce one row.
   * Is an alias of [[toScalar]].
   *
   * For Example:
   * {{{
   *     import functions._
   *     val df1 = session.sql("select * from values(1,1,1),(2,2,3) as T(c1,c2,c3)")
   *     val df2 = session.sql("select * from values(2) as T(a)")
   *     df1.select(Column("c1"), col(df2)).show()
   *     df1.filter(Column("c1") < col(df2)).show()
   * }}}
   *
   * @group client_func
   * @since 0.2.0
   */
  def col(df: DataFrame): Column = toScalar(df)

  /**
   * Generate a [[Column]] representing the result of the input DataFrame.
   * The parameter `df` should have one column and must produce one row.
   *
   * For Example:
   * {{{
   *     import functions._
   *     val df1 = session.sql("select * from values(1,1,1),(2,2,3) as T(c1,c2,c3)")
   *     val df2 = session.sql("select * from values(2) as T(a)")
   *     df1.select(Column("c1"), toScalar(df2)).show()
   *     df1.filter(Column("c1") < toScalar(df2)).show()
   * }}}
   *
   * @group client_func
   * @since 0.4.0
   */
  def toScalar(df: DataFrame): Column = {
    if (df.output.size != 1) {
      throw ErrorMessage.DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY(
        df.output.size,
        df.output.map(_.name).mkString(", "))
    }

    Column(ScalarSubquery(df.snowflakePlan))
  }

  /**
   * Creates a [[Column]] expression for a literal value.
   *
   * @group client_func
   * @since 0.1.0
   */
  def lit(literal: Any): Column = typedLit(literal)

  /**
   * Creates a [[Column]] expression for a literal value.
   *
   * @group client_func
   * @since 0.1.0
   */
  def typedLit[T: TypeTag](literal: T): Column = literal match {
    case c: Column => c
    case s: Symbol => Column(s.name)
    case _ => Column(Literal(literal))
  }

  /**
   * Creates a [[Column]] expression from raw SQL text.
   *
   * Note that the function does not interpret or check the SQL text.
   *
   * @group client_func
   * @since 0.1.0
   */
  def sqlExpr(sqlText: String): Column = Column.expr(sqlText)

  /**
   * Uses HyperLogLog to return an approximation of the distinct cardinality of the input
   * (i.e. returns an approximation of `COUNT(DISTINCT col)`).
   *
   * @group agg_func
   * @since 0.1.0
   */
  def approx_count_distinct(e: Column): Column = builtin("approx_count_distinct")(e)

  /**
   * Returns the average of non-NULL records. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def avg(e: Column): Column = builtin("avg")(e)

  /**
   * Returns the correlation coefficient for non-null pairs in a group.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def corr(column1: Column, column2: Column): Column = {
    builtin("corr")(column1, column2)
  }

  /**
   * Returns either the number of non-NULL records for the specified columns,
   * or the total number of records.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def count(e: Column): Column = e.expr match {
    // Turn count(*) into count(1)
    case _: Star => builtin("count")(Literal(1))
    case _ => builtin("count")(e)
  }

  /**
   * Returns either the number of non-NULL distinct records for the specified columns,
   * or the total number of the distinct records.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def count_distinct(expr: Column, exprs: Column*): Column =
    Column(FunctionExpression("count", (expr +: exprs).map(_.expr), isDistinct = true))

  /**
   * Returns the population covariance for non-null pairs in a group.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def covar_pop(column1: Column, column2: Column): Column = {
    builtin("covar_pop")(column1, column2)
  }

  /**
   * Returns the sample covariance for non-null pairs in a group.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def covar_samp(column1: Column, column2: Column): Column = {
    builtin("covar_samp")(column1, column2)
  }

  /**
   * Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def grouping(e: Column): Column = builtin("grouping")(e)

  /**
   * Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def grouping_id(cols: Column*): Column = builtin("grouping_id")(cols: _*)

  /**
   * Returns the population excess kurtosis of non-NULL records.
   * If all records inside a group are NULL, the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def kurtosis(e: Column): Column = builtin("kurtosis")(e)

  /**
   * Returns the maximum value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def max(e: Column): Column = builtin("max")(e)

  /**
   * Returns a non-deterministic value for the specified column.
   *
   * @group agg_func
   * @since 0.12.0
   */
  def any_value(e: Column): Column = builtin("any_value")(e)

  /**
   * Returns the average of non-NULL records. If all records inside a group are NULL,
   * the function returns NULL. Alias of avg
   *
   * @group agg_func
   * @since 0.1.0
   */
  def mean(e: Column): Column = avg(e)

  /**
   * Returns the median value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * @group agg_func
   * @since 0.5.0
   */
  def median(e: Column): Column = {
    builtin("median")(e)
  }

  /**
   * Returns the minimum value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def min(e: Column): Column = builtin("min")(e)

  /**
   * Returns the sample skewness of non-NULL records. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def skew(e: Column): Column = builtin("skew")(e)

  /**
   * Returns the sample standard deviation (square root of sample variance) of non-NULL values.
   * If all records inside a group are NULL, returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def stddev(e: Column): Column = builtin("stddev")(e)

  /**
   * Returns the sample standard deviation (square root of sample variance) of non-NULL values.
   * If all records inside a group are NULL, returns NULL. Alias of stddev
   *
   * @group agg_func
   * @since 0.1.0
   */
  def stddev_samp(e: Column): Column = builtin("stddev_samp")(e)

  /**
   * Returns the population standard deviation (square root of variance) of non-NULL values.
   * If all records inside a group are NULL, returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def stddev_pop(e: Column): Column = builtin("stddev_pop")(e)

  /**
   * Returns the sum of non-NULL records in a group. You can use the DISTINCT keyword to compute
   * the sum of unique non-null values. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def sum(e: Column): Column = builtin("sum")(e)

  /**
   * Returns the sum of non-NULL distinct records in a group. You can use the DISTINCT keyword to
   * compute the sum of unique non-null values. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def sum_distinct(e: Column): Column = internalBuiltinFunction(true, "sum", e)

  /**
   * Returns the sample variance of non-NULL records in a group.
   * If all records inside a group are NULL, a NULL is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def variance(e: Column): Column = builtin("variance")(e)

  /**
   * Returns the sample variance of non-NULL records in a group.
   * If all records inside a group are NULL, a NULL is returned.
   * Alias of var_samp
   *
   * @group agg_func
   * @since 0.1.0
   */
  def var_samp(e: Column): Column = variance(e)

  /**
   * Returns the population variance of non-NULL records in a group.
   * If all records inside a group are NULL, a NULL is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def var_pop(e: Column): Column = builtin("var_pop")(e)

  /**
   * Returns an approximated value for the desired percentile.
   * This function uses the t-Digest algorithm.
   *
   * @group agg_func
   * @since 0.2.0
   */
  def approx_percentile(col: Column, percentile: Double): Column = {
    builtin("approx_percentile")(col, sqlExpr(percentile.toString))
  }

  /**
   * Returns the internal representation of the t-Digest state (as a JSON object) at the end of
   * aggregation.
   * This function uses the t-Digest algorithm.
   *
   * @group agg_func
   * @since 0.2.0
   */
  def approx_percentile_accumulate(col: Column): Column = {
    builtin("approx_percentile_accumulate")(col)
  }

  /**
   * Returns the desired approximated percentile value for the specified t-Digest state.
   * APPROX_PERCENTILE_ESTIMATE(APPROX_PERCENTILE_ACCUMULATE(.)) is equivalent to
   * APPROX_PERCENTILE(.).
   *
   * @group agg_func
   * @since 0.2.0
   */
  def approx_percentile_estimate(state: Column, percentile: Double): Column = {
    builtin("approx_percentile_estimate")(state, sqlExpr(percentile.toString))
  }

  /**
   * Combines (merges) percentile input states into a single output state.
   *
   * This allows scenarios where APPROX_PERCENTILE_ACCUMULATE is run over horizontal partitions
   * of the same table, producing an algorithm state for each table partition. These states can
   * later be combined using APPROX_PERCENTILE_COMBINE, producing the same output state as a
   * single run of APPROX_PERCENTILE_ACCUMULATE over the entire table.
   *
   * @group agg_func
   * @since 0.2.0
   */
  def approx_percentile_combine(state: Column): Column = {
    builtin("approx_percentile_combine")(state)
  }

  /**
   * Finds the cumulative distribution of a value with regard to other values
   * within the same window partition.
   *
   * @group win_func
   * @since 0.1.0
   */
  def cume_dist(): Column = builtin("cume_dist")()

  /**
   * Returns the rank of a value within a group of values, without gaps in the ranks.
   * The rank value starts at 1 and continues up sequentially.
   * If two values are the same, they will have the same rank.
   *
   * @group win_func
   * @since 0.1.0
   */
  def dense_rank(): Column = builtin("dense_rank")()

  /**
   * Accesses data in a previous row in the same result set without having to
   * join the table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lag(e: Column, offset: Int, defaultValue: Column): Column =
    builtin("lag")(e, Literal(offset), defaultValue)

  /**
   * Accesses data in a previous row in the same result set without having to
   * join the table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lag(e: Column, offset: Int): Column = lag(e, offset, lit(null))

  /**
   * Accesses data in a previous row in the same result set without having to
   * join the table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lag(e: Column): Column = lag(e, 1)

  /**
   * Accesses data in a subsequent row in the same result set without having to join the
   * table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lead(e: Column, offset: Int, defaultValue: Column): Column =
    builtin("lead")(e, Literal(offset), defaultValue)

  /**
   * Accesses data in a subsequent row in the same result set without having to join the
   * table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lead(e: Column, offset: Int): Column = lead(e, offset, lit(null))

  /**
   * Accesses data in a subsequent row in the same result set without having to join the
   * table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lead(e: Column): Column = lead(e, 1)

  /**
   * Divides an ordered data set equally into the number of buckets specified by n.
   * Buckets are sequentially numbered 1 through n.
   *
   * @group win_func
   * @since 0.1.0
   */
  def ntile(n: Column): Column = builtin("ntile")(n)

  /**
   * Returns the relative rank of a value within a group of values, specified as a percentage
   * ranging from 0.0 to 1.0.
   *
   * @group win_func
   * @since 0.1.0
   */
  def percent_rank(): Column = builtin("percent_rank")()

  /**
   * Returns the rank of a value within an ordered group of values.
   * The rank value starts at 1 and continues up.
   *
   * @group win_func
   * @since 0.1.0
   */
  def rank(): Column = builtin("rank")()

  /**
   * Returns a unique row number for each row within a window partition.
   * The row number starts at 1 and continues up sequentially.
   *
   * @group win_func
   * @since 0.1.0
   */
  def row_number(): Column = builtin("row_number")()

  /**
   * Returns the first non-NULL expression among its arguments,
   * or NULL if all its arguments are NULL.
   *
   * @group con_func
   * @since 0.1.0
   */
  def coalesce(e: Column*): Column = builtin("coalesce")(e: _*)

  /**
   * Return true if the value in the column is not a number (NaN).
   *
   * @group con_func
   * @since 0.1.0
   */
  def equal_nan(e: Column): Column = withExpr { IsNaN(e.expr) }

  /**
   * Return true if the value in the column is null.
   *
   * @group con_func
   * @since 0.1.0
   */
  def is_null(e: Column): Column = withExpr { IsNull(e.expr) }

  /**
   * Returns the negation of the value in the column (equivalent to a unary minus).
   *
   * @group client_func
   * @since 0.1.0
   */
  def negate(e: Column): Column = -e

  /**
   * Returns the inverse of a boolean expression.
   *
   * @group client_func
   * @since 0.1.0
   */
  def not(e: Column): Column = !e

  /**
   * Each call returns a pseudo-random 64-bit integer.
   *
   * @group gen_func
   * @since 0.1.0
   */
  def random(seed: Long): Column = builtin("random")(Literal(seed))

  /**
   * Each call returns a pseudo-random 64-bit integer.
   *
   * @group gen_func
   * @since 0.1.0
   */
  def random(): Column = random(Random.nextLong())

  /**
   * Returns the bitwise negation of a numeric expression.
   *
   * @group bit_func
   * @since 0.1.0
   */
  def bitnot(e: Column): Column = builtin("bitnot")(e)

  /**
   * Converts an input expression to a decimal
   *
   * @group num_func
   * @since 0.5.0
   */
  def to_decimal(expr: Column, precision: Int, scale: Int): Column = {
    builtin("to_decimal")(expr, sqlExpr(precision.toString), sqlExpr(scale.toString))
  }

  /**
   * Performs division like the division operator (/),
   * but returns 0 when the divisor is 0 (rather than reporting an error).
   *
   * @group num_func
   * @since 0.1.0
   */
  def div0(dividend: Column, divisor: Column): Column =
    builtin("div0")(dividend, divisor)

  /**
   * Returns the square-root of a non-negative numeric expression.
   *
   * @group num_func
   * @since 0.1.0
   */
  def sqrt(e: Column): Column = builtin("sqrt")(e)

  /**
   * Returns the absolute value of a numeric expression.
   *
   * @group num_func
   * @since 0.1.0
   */
  def abs(e: Column): Column = builtin("abs")(e)

  /**
   * Computes the inverse cosine (arc cosine) of its input; the result is a number in the
   * interval [-pi, pi].
   *
   * @group num_func
   * @since 0.1.0
   */
  def acos(e: Column): Column = builtin("acos")(e)

  /**
   * Computes the inverse sine (arc sine) of its argument; the result is a number in the
   * interval [-pi, pi].
   *
   * @group num_func
   * @since 0.1.0
   */
  def asin(e: Column): Column = builtin("asin")(e)

  /**
   * Computes the inverse tangent (arc tangent) of its argument; the result is a number in
   * the interval [-pi, pi].
   *
   * @group num_func
   * @since 0.1.0
   */
  def atan(e: Column): Column = builtin("atan")(e)

  /**
   * Computes the inverse tangent (arc tangent) of the ratio of its two arguments.
   *
   * @group num_func
   * @since 0.1.0
   */
  def atan2(y: Column, x: Column): Column = builtin("atan2")(y, x)

  /**
   * Returns values from the specified column rounded to the nearest equal or larger integer.
   *
   * @group num_func
   * @since 0.1.0
   */
  def ceil(e: Column): Column = builtin("ceil")(e)

  /**
   * Computes the cosine of its argument; the argument should be expressed in radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def cos(e: Column): Column = builtin("cos")(e)

  /**
   * Computes the hyperbolic cosine of its argument.
   *
   * @group num_func
   * @since 0.1.0
   */
  def cosh(e: Column): Column = builtin("cosh")(e)

  /**
   * Computes Euler's number e raised to a floating-point value.
   *
   * @group num_func
   * @since 0.1.0
   */
  def exp(e: Column): Column = builtin("exp")(e)

  /**
   * Computes the factorial of its input. The input argument must be an integer
   * expression in the range of 0 to 33.
   *
   * @group num_func
   * @since 0.1.0
   */
  def factorial(e: Column): Column = builtin("factorial")(e)

  /**
   * Returns values from the specified column rounded to the nearest equal or smaller integer.
   *
   * @group num_func
   * @since 0.1.0
   */
  def floor(e: Column): Column = builtin("floor")(e)

  /**
   * Returns the largest value from a list of expressions. If any of the argument values is NULL,
   * the result is NULL. GREATEST supports all data types, including VARIANT.
   *
   * @group con_func
   * @since 0.1.0
   */
  def greatest(exprs: Column*): Column = builtin("greatest")(exprs: _*)

  /**
   * Returns the smallest value from a list of expressions. LEAST supports all data types,
   * including VARIANT.
   *
   * @group con_func
   * @since 0.1.0
   */
  def least(exprs: Column*): Column = builtin("least")(exprs: _*)

  /**
   * Returns the logarithm of a numeric expression.
   *
   * @group num_func
   * @since 0.1.0
   */
  def log(base: Column, a: Column): Column = builtin("log")(base, a)

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * @group num_func
   * @since 0.1.0
   */
  def pow(l: Column, r: Column): Column = builtin("pow")(l, r)

  /**
   * Returns rounded values for the specified column.
   *
   * @group num_func
   * @since 0.1.0
   */
  def round(e: Column, scale: Column): Column = builtin("round")(e, scale)

  /**
   * Returns rounded values for the specified column.
   *
   * @group num_func
   * @since 0.1.0
   */
  def round(e: Column): Column = round(e, lit(0))

  /**
   * Shifts the bits for a numeric expression numBits positions to the left.
   *
   * @group bit_func
   * @since 0.1.0
   */
  def bitshiftleft(e: Column, numBits: Column): Column = withExpr {
    ShiftLeft(e.expr, numBits.expr)
  }

  /**
   * Shifts the bits for a numeric expression numBits positions to the right.
   *
   * @group bit_func
   * @since 0.1.0
   */
  def bitshiftright(e: Column, numBits: Column): Column = withExpr {
    ShiftRight(e.expr, numBits.expr)
  }

  /**
   * Computes the sine of its argument; the argument should be expressed in radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def sin(e: Column): Column = builtin("sin")(e)

  /**
   * Computes the hyperbolic sine of its argument.
   *
   * @group num_func
   * @since 0.1.0
   */
  def sinh(e: Column): Column = builtin("sinh")(e)

  /**
   * Computes the tangent of its argument; the argument should be expressed in radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def tan(e: Column): Column = builtin("tan")(e)

  /**
   * Computes the hyperbolic tangent of its argument.
   *
   * @group num_func
   * @since 0.1.0
   */
  def tanh(e: Column): Column = builtin("tanh")(e)

  /**
   * Converts radians to degrees.
   *
   * @group num_func
   * @since 0.1.0
   */
  def degrees(e: Column): Column = builtin("degrees")(e)

  /**
   * Converts degrees to radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def radians(e: Column): Column = builtin("radians")(e)

  /**
   * Returns a 32-character hex-encoded string containing the 128-bit MD5 message digest.
   *
   * @group str_func
   * @since 0.1.0
   */
  def md5(e: Column): Column = builtin("md5")(e)

  /**
   * Returns a 40-character hex-encoded string containing the 160-bit SHA-1 message digest.
   *
   * @group str_func
   * @since 0.1.0
   */
  def sha1(e: Column): Column = builtin("sha1")(e)

  /**
   * Returns a hex-encoded string containing the N-bit SHA-2 message digest,
   * where N is the specified output digest size.
   *
   * @group str_func
   * @since 0.1.0
   */
  def sha2(e: Column, numBits: Int): Column = {
    require(
      Seq(0, 224, 256, 384, 512).contains(numBits),
      s"numBits $numBits is not in the permitted values (0, 224, 256, 384, 512)")
    builtin("sha2")(e, Literal(numBits))
  }

  /**
   * Returns a signed 64-bit hash value. Note that HASH never returns NULL, even for NULL inputs.
   *
   * @group utl_func
   * @since 0.1.0
   */
  def hash(cols: Column*): Column = builtin("hash")(cols: _*)

  /**
   * Returns the ASCII code for the first character of a string. If the string is empty,
   * a value of 0 is returned.
   *
   * @group str_func
   * @since 0.1.0
   */
  def ascii(e: Column): Column = builtin("ascii")(e)

  /**
   * Concatenates two or more strings, or concatenates two or more binary values.
   * If any of the values is null, the result is also null.
   *
   * @group str_func
   * @since 0.1.0
   */
  def concat_ws(separator: Column, exprs: Column*): Column = {
    val args = Seq(separator) ++ exprs
    builtin("concat_ws")(args: _*)
  }

  /**
   * Returns the input string with the first letter of each word in uppercase
   * and the subsequent letters in lowercase.
   *
   * @group str_func
   * @since 0.1.0
   */
  def initcap(e: Column): Column = builtin("initcap")(e)

  /**
   * Returns the length of an input string or binary value. For strings,
   * the length is the number of characters, and UTF-8 characters are counted as a
   * single character. For binary, the length is the number of bytes.
   *
   * @group str_func
   * @since 0.1.0
   */
  def length(e: Column): Column = builtin("length")(e)

  /**
   * Returns the input string with all characters converted to lowercase.
   *
   * @group str_func
   * @since 0.1.0
   */
  def lower(e: Column): Column = builtin("lower")(e)

  /**
   * Left-pads a string with characters from another string, or left-pads a
   * binary value with bytes from another binary value.
   *
   * @group str_func
   * @since 0.1.0
   */
  def lpad(str: Column, len: Column, pad: Column): Column =
    builtin("lpad")(str, len, pad)

  /**
   * Removes leading characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def ltrim(e: Column, trimString: Column): Column = builtin("ltrim")(e, trimString)

  /**
   * Removes leading characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def ltrim(e: Column): Column = builtin("ltrim")(e)

  /**
   * Right-pads a string with characters from another string, or right-pads a
   * binary value with bytes from another binary value.
   *
   * @group str_func
   * @since 0.1.0
   */
  def rpad(str: Column, len: Column, pad: Column): Column =
    builtin("rpad")(str, len, pad)

  /**
   * Builds a string by repeating the input for the specified number of times.
   *
   * @group str_func
   * @since 0.1.0
   */
  def repeat(str: Column, n: Column): Column = withExpr {
    StringRepeat(str.expr, n.expr)
  }

  /**
   * Removes trailing characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def rtrim(e: Column, trimString: Column): Column = builtin("rtrim")(e, trimString)

  /**
   * Removes trailing characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def rtrim(e: Column): Column = builtin("rtrim")(e)

  /**
   * Returns a string that contains a phonetic representation of the input string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def soundex(e: Column): Column = builtin("soundex")(e)

  /**
   * Splits a given string with a given separator and returns the result in an array of strings.
   *
   * @group str_func
   * @since 0.1.0
   */
  def split(str: Column, pattern: Column): Column = builtin("split")(str, pattern)

  /**
   * Returns the portion of the string or binary value str,
   * starting from the character/byte specified by pos, with limited length.
   *
   * @group str_func
   * @since 0.1.0
   */
  def substring(str: Column, pos: Column, len: Column): Column =
    builtin("substring")(str, pos, len)

  /**
   * Translates src from the characters in matchingString to the characters in replaceString.
   *
   * @group str_func
   * @since 0.1.0
   */
  def translate(src: Column, matchingString: Column, replaceString: Column): Column =
    builtin("translate")(src, matchingString, replaceString)

  /**
   * Removes leading and trailing characters from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def trim(e: Column, trimString: Column): Column = builtin("trim")(e, trimString)

  /**
   * Returns the input string with all characters converted to uppercase.
   *
   * @group str_func
   * @since 0.1.0
   */
  def upper(e: Column): Column = builtin("upper")(e)

  /**
   * Returns true if col contains str.
   *
   * @group str_func
   * @since 0.1.0
   */
  def contains(col: Column, str: Column): Column =
    builtin("contains")(col, str)

  /**
   * Returns true if col starts with str.
   *
   * @group str_func
   * @since 0.1.0
   */
  def startswith(col: Column, str: Column): Column =
    builtin("startswith")(col, str)

  /**
   * Converts a Unicode code point (including 7-bit ASCII) into the character
   * that matches the input Unicode.
   *
   * @group str_func
   * @since 0.1.0
   */
  def char(col: Column): Column =
    builtin("char")(col)

  /**
   * Adds or subtracts a specified number of months to a date or timestamp,
   * preserving the end-of-month information.
   *
   * @group date_func
   * @since 0.1.0
   */
  def add_months(startDate: Column, numMonths: Column): Column =
    builtin("add_months")(startDate, numMonths)

  /**
   * Returns the current date of the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_date(): Column = builtin("current_date")()

  /**
   * Returns the current timestamp for the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_timestamp(): Column = builtin("current_timestamp")()

  /**
   * Returns the name of the region for the account where the current user is logged in.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_region(): Column = builtin("current_region")()

  /**
   * Returns the current time for the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_time(): Column = builtin("current_time")()

  /**
   * Returns the current Snowflake version.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_version(): Column = builtin("current_version")()

  /**
   * Returns the account used by the user's current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_account(): Column = builtin("current_account")()

  /**
   * Returns the name of the role in use for the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_role(): Column = builtin("current_role")()

  /**
   * Returns a JSON string that lists all roles granted to the current user.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_available_roles(): Column = builtin("current_available_roles")()

  /**
   * Returns a unique system identifier for the Snowflake session corresponding
   * to the present connection.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_session(): Column = builtin("current_session")()

  /**
   * Returns the SQL text of the statement that is currently executing.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_statement(): Column = builtin("current_statement")()

  /**
   * Returns the name of the user currently logged into the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_user(): Column = builtin("current_user")()

  /**
   * Returns the name of the database in use for the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_database(): Column = builtin("current_database")()

  /**
   * Returns the name of the schema in use by the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_schema(): Column = builtin("current_schema")()

  /**
   * Returns active search path schemas.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_schemas(): Column = builtin("current_schemas")()

  /**
   * Returns the name of the warehouse in use for the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_warehouse(): Column = builtin("current_warehouse")()

  /**
   * Returns the current timestamp for the system, but in the UTC time zone.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def sysdate(): Column = builtin("sysdate")()

  // scalastyle:off
  /**
   * Converts the given sourceTimestampNTZ from sourceTimeZone to targetTimeZone.
   *
   * Supported time zones are listed
   * [[https://docs.snowflake.com/en/sql-reference/functions/convert_timezone.html#usage-notes here]]
   *
   * Example
   * {{{
   *   timestampNTZ.select(convert_timezone(lit("America/Los_Angeles"), lit("America/New_York"), col("time")))
   * }}}
   *
   * @group date_func
   * @since 0.1.0
   */
  // scalastyle:on
  def convert_timezone(
      sourceTimeZone: Column,
      targetTimeZone: Column,
      sourceTimestampNTZ: Column): Column =
    builtin("convert_timezone")(sourceTimeZone, targetTimeZone, sourceTimestampNTZ)

  // scalastyle:off
  /**
   * Converts the given sourceTimestampNTZ to targetTimeZone.
   *
   * Supported time zones are listed
   * [[https://docs.snowflake.com/en/sql-reference/functions/convert_timezone.html#usage-notes here]]
   *
   * Example
   * {{{
   *   timestamp.select(convert_timezone(lit("America/New_York"), col("time")))
   * }}}
   *
   * @group date_func
   * @since 0.1.0
   */
  // scalastyle:on
  def convert_timezone(targetTimeZone: Column, sourceTimestamp: Column): Column =
    builtin("convert_timezone")(targetTimeZone, sourceTimestamp)

  /**
   * Extracts the year from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def year(e: Column): Column = builtin("year")(e)

  /**
   * Extracts the quarter from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def quarter(e: Column): Column = builtin("quarter")(e)

  /**
   * Extracts the month from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def month(e: Column): Column = builtin("month")(e)

  /**
   * Extracts the day of week from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayofweek(e: Column): Column = builtin("dayofweek")(e)

  /**
   * Extracts the day of month from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayofmonth(e: Column): Column = builtin("dayofmonth")(e)

  /**
   * Extracts the day of year from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayofyear(e: Column): Column = builtin("dayofyear")(e)

  /**
   * Extracts the hour from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def hour(e: Column): Column = builtin("hour")(e)

  /**
   * Returns the last day of the specified date part for a date or timestamp.
   * Commonly used to return the last day of the month for a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def last_day(e: Column): Column = builtin("last_day")(e)

  /**
   * Extracts the minute from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def minute(e: Column): Column = builtin("minute")(e)

  /**
   * Returns the date of the first specified DOW (day of week) that occurs after the input date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def next_day(date: Column, dayOfWeek: Column): Column = withExpr {
    NextDay(date.expr, lit(dayOfWeek).expr)
  }

  /**
   * Returns the date of the first specified DOW (day of week) that occurs before the input date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def previous_day(date: Column, dayOfWeek: Column): Column =
    builtin("previous_day")(date, dayOfWeek)

  /**
   * Extracts the second from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def second(e: Column): Column = builtin("second")(e)

  /**
   * Extracts the week of year from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def weekofyear(e: Column): Column = builtin("weekofyear")(e)

  /**
   * Converts an input expression into the corresponding timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_timestamp(s: Column): Column = builtin("to_timestamp")(s)

  /**
   * Converts an input expression into the corresponding timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_timestamp(s: Column, fmt: Column): Column = builtin("to_timestamp")(s, fmt)

  /**
   * Converts an input expression to a date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_date(e: Column): Column = builtin("to_date")(e)

  /**
   * Converts an input expression to a date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_date(e: Column, fmt: Column): Column = builtin("to_date")(e, fmt)

  /**
   * Creates a date from individual numeric components that represent the year,
   * month, and day of the month.
   *
   * @group date_func
   * @since 0.1.0
   */
  def date_from_parts(year: Column, month: Column, day: Column): Column =
    builtin("date_from_parts")(year, month, day)

  /**
   * Creates a time from individual numeric components.
   *
   * @group date_func
   * @since 0.1.0
   */
  def time_from_parts(hour: Column, minute: Column, second: Column, nanoseconds: Column): Column =
    builtin("time_from_parts")(hour, minute, second, nanoseconds)

  /**
   * Creates a time from individual numeric components.
   *
   * @group date_func
   * @since 0.1.0
   */
  def time_from_parts(hour: Column, minute: Column, second: Column): Column =
    builtin("time_from_parts")(hour, minute, second)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column): Column =
    builtin("timestamp_from_parts")(year, month, day, hour, minute, second)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column,
      nanosecond: Column): Column =
    builtin("timestamp_from_parts")(year, month, day, hour, minute, second, nanosecond)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_from_parts(dateExpr: Column, timeExpr: Column): Column =
    builtin("timestamp_from_parts")(dateExpr, timeExpr)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_ltz_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column): Column =
    builtin("timestamp_ltz_from_parts")(year, month, day, hour, minute, second)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_ltz_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column,
      nanosecond: Column): Column =
    builtin("timestamp_ltz_from_parts")(year, month, day, hour, minute, second, nanosecond)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_ntz_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column): Column =
    builtin("timestamp_ntz_from_parts")(year, month, day, hour, minute, second)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_ntz_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column,
      nanosecond: Column): Column =
    builtin("timestamp_ntz_from_parts")(year, month, day, hour, minute, second, nanosecond)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_ntz_from_parts(dateExpr: Column, timeExpr: Column): Column =
    builtin("timestamp_ntz_from_parts")(dateExpr, timeExpr)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_tz_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column): Column =
    builtin("timestamp_tz_from_parts")(year, month, day, hour, minute, second)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_tz_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column,
      nanosecond: Column): Column =
    builtin("timestamp_tz_from_parts")(year, month, day, hour, minute, second, nanosecond)

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_tz_from_parts(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      minute: Column,
      second: Column,
      nanosecond: Column,
      timeZone: Column): Column =
    builtin("timestamp_tz_from_parts")(
      year,
      month,
      day,
      hour,
      minute,
      second,
      nanosecond,
      timeZone)

  /**
   * Extracts the three-letter day-of-week name from the specified date or
   * timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayname(expr: Column): Column = builtin("dayname")(expr)

  /**
   * Extracts the three-letter month name from the specified date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def monthname(expr: Column): Column = builtin("monthname")(expr)

  // scalastyle:off
  /**
   * Adds the specified value for the specified date or time art to date or time expr.
   *
   * Supported date and time parts are listed
   * [[https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts here]]
   *
   * Example: add one year on dates
   * {{{
   *   date.select(dateadd("year", lit(1), col("date_col")))
   * }}}
   *
   * @group date_func
   * @since 0.1.0
   */
  // scalastyle:on
  def dateadd(part: String, value: Column, expr: Column): Column =
    builtin("dateadd")(part, value, expr)

  // scalastyle:off
  /**
   * Calculates the difference between two date, time, or timestamp columns based on the date or time part requested.
   *
   * Supported date and time parts are listed
   * [[https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts here]]
   *
   * Example: year difference between two date columns
   * {{{
   *   date.select(datediff("year", col("date_col1"), col("date_col2"))),
   * }}}
   *
   * @group date_func
   * @since 0.1.0
   */
  // scalastyle:on
  def datediff(part: String, col1: Column, col2: Column): Column =
    builtin("datediff")(part, col1, col2)

  /**
   * Rounds the input expression down to the nearest (or equal) integer closer to zero,
   * or to the nearest equal or smaller value with the specified number of
   * places after the decimal point.
   *
   * @group num_func
   * @since 0.1.0
   */
  def trunc(expr: Column, scale: Column): Column = withExpr {
    Trunc(expr.expr, scale.expr)
  }

  /**
   * Truncates a DATE, TIME, or TIMESTAMP to the specified precision.
   *
   * @group date_func
   * @since 0.1.0
   */
  def date_trunc(format: String, timestamp: Column): Column = withExpr {
    DateTrunc(Literal(format), timestamp.expr)
  }

  /**
   * Concatenates one or more strings, or concatenates one or more binary values.
   * If any of the values is null, the result is also null.
   *
   * @group str_func
   * @since 0.1.0
   */
  def concat(exprs: Column*): Column = builtin("concat")(exprs: _*)

  /**
   * Compares whether two arrays have at least one element in common.
   * Returns TRUE if there is at least one element in common; otherwise returns FALSE.
   * The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def arrays_overlap(a1: Column, a2: Column): Column = withExpr {
    ArraysOverlap(a1.expr, a2.expr)
  }

  /**
   * Returns TRUE if expr ends with str.
   *
   * @group str_func
   * @since 0.1.0
   */
  def endswith(expr: Column, str: Column): Column =
    builtin("endswith")(expr, str)

  /**
   * Replaces a substring of the specified length, starting at the specified position,
   * with a new string or binary value.
   *
   * @group str_func
   * @since 0.1.0
   */
  def insert(baseExpr: Column, position: Column, length: Column, insertExpr: Column): Column =
    builtin("insert")(baseExpr, position, length, insertExpr)

  /**
   * Returns a left most substring of strExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def left(strExpr: Column, lengthExpr: Column): Column =
    builtin("left")(strExpr, lengthExpr)

  /**
   * Returns a right most substring of strExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def right(strExpr: Column, lengthExpr: Column): Column =
    builtin("right")(strExpr, lengthExpr)

  // scalastyle:off
  /**
   * Returns the number of times that a pattern occurs in a strExpr.
   *
   * Pattern syntax is specified
   * [[https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes here]]
   *
   * Parameter detail is specified
   * [[https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-parameters-argument here]]
   *
   * @group str_func
   * @since 0.1.0
   */
  // scalastyle:on
  def regexp_count(
      strExpr: Column,
      pattern: Column,
      position: Column,
      parameters: Column): Column =
    builtin("regexp_count")(strExpr, pattern, position, parameters)

  // scalastyle:off
  /**
   * Returns the number of times that a pattern occurs in a strExpr.
   *
   * Pattern syntax is specified
   * [[https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes here]]
   *
   * Parameter detail is specified
   * [[https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-parameters-argument here]]
   *
   * @group str_func
   * @since 0.1.0
   */
  // scalastyle:on
  def regexp_count(strExpr: Column, pattern: Column): Column =
    builtin("regexp_count")(strExpr, pattern)

  /**
   * Returns the subject with the specified pattern (or all occurrences of the pattern) removed.
   * If no matches are found, returns the original subject.
   *
   * @group str_func
   * @since 1.9.0
   */
  def regexp_replace(strExpr: Column, pattern: Column): Column =
    builtin("regexp_replace")(strExpr, pattern)

  /**
   * Returns the subject with the specified pattern (or all occurrences of the pattern)
   * replaced by a replacement string. If no matches are found,
   * returns the original subject.
   *
   * @group str_func
   * @since 1.9.0
   */
  def regexp_replace(strExpr: Column, pattern: Column, replacement: Column): Column =
    builtin("regexp_replace")(strExpr, pattern, replacement)

  /**
   * Removes all occurrences of a specified strExpr,
   * and optionally replaces them with replacement.
   *
   * @group str_func
   * @since 0.1.0
   */
  def replace(strExpr: Column, pattern: Column, replacement: Column): Column =
    builtin("replace")(strExpr, pattern, replacement)

  /**
   * Removes all occurrences of a specified strExpr,
   * and optionally replaces them with replacement.
   *
   * @group str_func
   * @since 0.1.0
   */
  def replace(strExpr: Column, pattern: Column): Column =
    builtin("replace")(strExpr, pattern)

  /**
   * Searches for targetExpr in sourceExpr and, if successful,
   * returns the position (1-based) of the targetExpr in sourceExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def charindex(targetExpr: Column, sourceExpr: Column): Column =
    builtin("charindex")(targetExpr, sourceExpr)

  /**
   * Searches for targetExpr in sourceExpr and, if successful,
   * returns the position (1-based) of the targetExpr in sourceExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def charindex(targetExpr: Column, sourceExpr: Column, position: Column): Column =
    builtin("charindex")(targetExpr, sourceExpr, position)

  // scalastyle:off
  /**
   * Returns a copy of expr, but with the specified collationSpec property
   * instead of the original collation specification property.
   *
   * Collation Specification is specified
   * [[https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification here]]
   *
   * @group str_func
   * @since 0.1.0
   */
  // scalastyle:on
  def collate(expr: Column, collationSpec: String): Column =
    builtin("collate")(expr, collationSpec)

  /**
   * Returns the collation specification of expr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def collation(expr: Column): Column =
    builtin("collation")(expr)

  /**
   * Returns an ARRAY that contains the matching elements in the two input ARRAYs.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def array_intersection(col1: Column, col2: Column): Column = withExpr {
    ArrayIntersect(col1.expr, col2.expr)
  }

  /**
   * Returns true if the specified VARIANT column contains an ARRAY value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_array(col: Column): Column = {
    builtin("is_array")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a Boolean value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_boolean(col: Column): Column = {
    builtin("is_boolean")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a binary value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_binary(col: Column): Column = {
    builtin("is_binary")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a string value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_char(col: Column): Column = {
    builtin("is_char")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a string value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_varchar(col: Column): Column = {
    builtin("is_varchar")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a DATE value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_date(col: Column): Column = {
    builtin("is_date")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a DATE value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_date_value(col: Column): Column = {
    builtin("is_date_value")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a fixed-point decimal value or integer.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_decimal(col: Column): Column = {
    builtin("is_decimal")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a floating-point value, fixed-point
   * decimal, or integer.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_double(col: Column): Column = {
    builtin("is_double")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a floating-point value, fixed-point
   * decimal, or integer.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_real(col: Column): Column = {
    builtin("is_real")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains an integer value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_integer(col: Column): Column = {
    builtin("is_integer")(col)
  }

  /**
   * Returns true if the specified VARIANT column is a JSON null value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_null_value(col: Column): Column = {
    builtin("is_null_value")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains an OBJECT value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_object(col: Column): Column = {
    builtin("is_object")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a TIME value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_time(col: Column): Column = {
    builtin("is_time")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value to be interpreted
   * using the local time zone.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_timestamp_ltz(col: Column): Column = {
    builtin("is_timestamp_ltz")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value with no time zone.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_timestamp_ntz(col: Column): Column = {
    builtin("is_timestamp_ntz")(col)
  }

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value with a time zone.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_timestamp_tz(col: Column): Column = {
    builtin("is_timestamp_tz")(col)
  }

  /**
   * Checks the validity of a JSON document.
   * If the input string is a valid JSON document or a NULL (i.e. no error would occur when
   * parsing the input string), the function returns NULL.
   * In case of a JSON parsing error, the function returns a string that contains the error
   * message.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def check_json(col: Column): Column = {
    builtin("check_json")(col)
  }

  /**
   * Checks the validity of an XML document.
   * If the input string is a valid XML document or a NULL (i.e. no error would occur when parsing
   * the input string), the function returns NULL.
   * In case of an XML parsing error, the output string contains the error message.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def check_xml(col: Column): Column = {
    builtin("check_xml")(col)
  }

  /**
   * Parses a JSON string and returns the value of an element at a specified path in the resulting
   * JSON document.
   *
   * @param col Column containing the JSON string that should be parsed.
   * @param path Column containing the path to the element that should be extracted.
   * @group semi_func
   * @since 0.2.0
   */
  def json_extract_path_text(col: Column, path: Column): Column = {
    builtin("json_extract_path_text")(col, path)
  }

  /**
   * Parse the value of the specified column as a JSON string and returns the resulting JSON
   * document.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def parse_json(col: Column): Column = {
    builtin("parse_json")(col)
  }

  /**
   * Parse the value of the specified column as a JSON string and returns the resulting XML
   * document.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def parse_xml(col: Column): Column = {
    builtin("parse_xml")(col)
  }

  /**
   * Converts a JSON "null" value in the specified column to a SQL NULL value.
   * All other VARIANT values in the column are returned unchanged.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def strip_null_value(col: Column): Column = {
    builtin("strip_null_value")(col)
  }

  /**
   * Returns the input values, pivoted into an ARRAY.
   * If the input is empty, an empty ARRAY is returned.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def array_agg(col: Column): Column = {
    builtin("array_agg")(col)
  }

  /**
   * Returns an ARRAY containing all elements from the source ARRAYas well as the new element.
   * The new element is located at end of the ARRAY.
   *
   * @param array The column containing the source ARRAY.
   * @param element The column containing the element to be appended. The element may be of almost
   *                any data type. The data type does not need to match the data type(s) of the
   *                existing elements in the ARRAY.
   * @group semi_func
   * @since 0.2.0
   */
  def array_append(array: Column, element: Column): Column = {
    builtin("array_append")(array, element)
  }

  /**
   * Returns the concatenation of two ARRAYs.
   *
   * @param array1 Column containing the source ARRAY.
   * @param array2 Column containing the ARRAY to be appended to {@code array1}.
   * @group semi_func
   * @since 0.2.0
   */
  def array_cat(array1: Column, array2: Column): Column = {
    builtin("array_cat")(array1, array2)
  }

  /**
   * Returns a compacted ARRAY with missing and null values removed,
   * effectively converting sparse arrays into dense arrays.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def array_compact(array: Column): Column = {
    builtin("array_compact")(array)
  }

  /**
   * Returns an ARRAY constructed from zero, one, or more inputs.
   *
   * @param cols Columns containing the values (or expressions that evaluate to values). The
   *             values do not all need to be of the same data type.
   * @group semi_func
   * @since 0.2.0
   */
  def array_construct(cols: Column*): Column = {
    builtin("array_construct")(cols: _*)
  }

  /**
   * Returns an ARRAY constructed from zero, one, or more inputs;
   * the constructed ARRAY omits any NULL input values.
   *
   * @param cols Columns containing the values (or expressions that evaluate to values). The
   *             values do not all need to be of the same data type.
   * @group semi_func
   * @since 0.2.0
   */
  def array_construct_compact(cols: Column*): Column = {
    builtin("array_construct_compact")(cols: _*)
  }

  /**
   * Returns {@code true} if the specified VARIANT is found in the specified ARRAY.
   *
   * @param variant Column containing the VARIANT to find.
   * @param array Column containing the ARRAY to search.
   * @group semi_func
   * @since 0.2.0
   */
  def array_contains(variant: Column, array: Column): Column = {
    builtin("array_contains")(variant, array)
  }

  /**
   * Returns an ARRAY containing all elements from the source ARRAY as well as the new element.
   *
   * @param array Column containing the source ARRAY.
   * @param pos Column containing a (zero-based) position in the source ARRAY.
   *            The new element is inserted at this position. The original element from this
   *            position (if any) and all subsequent elements (if any) are shifted by one position
   *            to the right in the resulting array (i.e. inserting at position 0 has the same
   *            effect as using [[array_prepend]]).
   *            A negative position is interpreted as an index from the back of the array (e.g.
   *            {@code -1} results in insertion before the last element in the array).
   * @param element Column containing the element to be inserted. The new element is located at
   *                position {@code pos}. The relative order of the other elements from the source
   *                array is preserved.
   * @group semi_func
   * @since 0.2.0
   */
  def array_insert(array: Column, pos: Column, element: Column): Column = {
    builtin("array_insert")(array, pos, element)
  }

  /**
   * Returns the index of the first occurrence of an element in an ARRAY.
   *
   * @param variant Column containing the VARIANT value that you want to find. The function
   *                searches for the first occurrence of this value in the array.
   * @param array Column containing the ARRAY to be searched.
   * @group semi_func
   * @since 0.2.0
   */
  def array_position(variant: Column, array: Column): Column = {
    builtin("array_position")(variant, array)
  }

  /**
   * Returns an ARRAY containing the new element as well as all elements from the source ARRAY.
   * The new element is positioned at the beginning of the ARRAY.
   *
   * @param array Column containing the source ARRAY.
   * @param element Column containing the element to be prepended.
   * @group semi_func
   * @since 0.2.0
   */
  def array_prepend(array: Column, element: Column): Column = {
    builtin("array_prepend")(array, element)
  }

  /**
   * Returns the size of the input ARRAY.
   *
   * If the specified column contains a VARIANT value that contains an ARRAY, the size of the ARRAY
   * is returned; otherwise, NULL is returned if the value is not an ARRAY.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def array_size(array: Column): Column = {
    builtin("array_size")(array)
  }

  /**
   * Returns an ARRAY constructed from a specified subset of elements of the input ARRAY.
   *
   * @param array Column containing the source ARRAY.
   * @param from Column containing a position in the source ARRAY. The position of the first
   *                    element is {@code 0}. Elements from positions less than this parameter are
   *                    not included in the resulting ARRAY.
   * @param to Column containing a position in the source ARRAY. Elements from positions equal to
   *                  or greater than this parameter are not included in the resulting array.
   * @group semi_func
   * @since 0.2.0
   */
  def array_slice(array: Column, from: Column, to: Column): Column = {
    builtin("array_slice")(array, from, to)
  }

  /**
   * Returns an input ARRAY converted to a string by casting all values to strings (using
   * TO_VARCHAR) and concatenating them (using the string from the second argument to separate
   * the elements).
   *
   * @param array Column containing the ARRAY of elements to convert to a string.
   * @param separator Column containing the string to put between each element (e.g. a space,
   *                  comma, or other human-readable separator).
   * @group semi_func
   * @since 0.2.0
   */
  def array_to_string(array: Column, separator: Column): Column = {
    builtin("array_to_string")(array, separator)
  }

  /**
   * Returns one OBJECT per group. For each (key, value) input pair, where key must be a VARCHAR
   * and value must be a VARIANT, the resulting OBJECT contains a key:value field.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def objectagg(key: Column, value: Column): Column = {
    builtin("objectagg")(key, value)
  }

  /**
   * Returns an OBJECT constructed from the arguments.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_construct(key_values: Column*): Column = {
    builtin("object_construct")(key_values: _*)
  }

  /**
   * Returns an object containing the contents of the input (i.e.source) object with one or more
   * keys removed.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_delete(obj: Column, key1: Column, keys: Column*): Column = {
    val args = Seq(obj, key1) ++ keys
    builtin("object_delete")(args: _*)
  }

  /**
   * Returns an object consisting of the input object with a new key-value pair inserted.
   * The input key must not exist in the object.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_insert(obj: Column, key: Column, value: Column): Column = {
    builtin("object_insert")(obj, key, value)
  }

  /**
   * Returns an object consisting of the input object with a new key-value pair inserted (or an
   * existing key updated with a new value).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_insert(obj: Column, key: Column, value: Column, update_flag: Column): Column = {
    builtin("object_insert")(obj, key, value, update_flag)
  }

  /**
   * Returns a new OBJECT containing some of the key-value pairs from an existing object.
   *
   * To identify the key-value pairs to include in the new object, pass in the keys as arguments,
   * or pass in an array containing the keys.
   *
   * If a specified key is not present in the input object, the key is ignored.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_pick(obj: Column, key1: Column, keys: Column*): Column = {
    val args = Seq(obj, key1) ++ keys
    builtin("object_pick")(args: _*)
  }

  /**
   * Casts a VARIANT value to an array.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_array(variant: Column): Column = {
    builtin("as_array")(variant)
  }

  /**
   * Casts a VARIANT value to a binary string.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_binary(variant: Column): Column = {
    builtin("as_binary")(variant)
  }

  /**
   * Casts a VARIANT value to a string. Does not convert values of other types into string.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_char(variant: Column): Column = {
    builtin("as_char")(variant)
  }

  /**
   * Casts a VARIANT value to a string. Does not convert values of other types into string.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_varchar(variant: Column): Column = {
    builtin("as_varchar")(variant)
  }

  /**
   * Casts a VARIANT value to a date. Does not convert from timestamps.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_date(variant: Column): Column = {
    builtin("as_date")(variant)
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_decimal(variant: Column): Column = {
    builtin("as_decimal")(variant)
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_decimal(variant: Column, precision: Int): Column = {
    builtin("as_decimal")(variant, sqlExpr(precision.toString))
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision and scale.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_decimal(variant: Column, precision: Int, scale: Int): Column = {
    builtin("as_decimal")(variant, sqlExpr(precision.toString), sqlExpr(scale.toString))
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_number(variant: Column): Column = {
    builtin("as_number")(variant)
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_number(variant: Column, precision: Int): Column = {
    builtin("as_number")(variant, sqlExpr(precision.toString))
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision and scale.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_number(variant: Column, precision: Int, scale: Int): Column = {
    builtin("as_number")(variant, sqlExpr(precision.toString), sqlExpr(scale.toString))
  }

  /**
   * Casts a VARIANT value to a floating-point value.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_double(variant: Column): Column = {
    builtin("as_double")(variant)
  }

  /**
   * Casts a VARIANT value to a floating-point value.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_real(variant: Column): Column = {
    builtin("as_real")(variant)
  }

  /**
   * Casts a VARIANT value to an integer. Does not match non-integer values.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_integer(variant: Column): Column = {
    builtin("as_integer")(variant)
  }

  /**
   * Casts a VARIANT value to an object.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_object(variant: Column): Column = {
    builtin("as_object")(variant)
  }

  /**
   * Casts a VARIANT value to a time value. Does not convert from timestamps.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_time(variant: Column): Column = {
    builtin("as_time")(variant)
  }

  /**
   * Casts a VARIANT value to a TIMESTAMP value with local timezone.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_timestamp_ltz(variant: Column): Column = {
    builtin("as_timestamp_ltz")(variant)
  }

  /**
   * Casts a VARIANT value to a TIMESTAMP value with no timezone.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_timestamp_ntz(variant: Column): Column = {
    builtin("as_timestamp_ntz")(variant)
  }

  /**
   * Casts a VARIANT value to a TIMESTAMP value with timezone.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_timestamp_tz(variant: Column): Column = {
    builtin("as_timestamp_tz")(variant)
  }

  /**
   * Tokenizes the given string using the given set of delimiters and returns the tokens as an
   * array. If either parameter is a NULL, a NULL is returned. An empty array is returned if
   * tokenization produces no tokens.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def strtok_to_array(array: Column): Column = {
    builtin("strtok_to_array")(array)
  }

  /**
   * Tokenizes the given string using the given set of delimiters and returns the tokens as an
   * array. If either parameter is a NULL, a NULL is returned. An empty array is returned if
   * tokenization produces no tokens.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def strtok_to_array(array: Column, delimiter: Column): Column = {
    builtin("strtok_to_array")(array, delimiter)
  }

  /**
   * Converts the input expression into an array:
   *
   * If the input is an ARRAY, or VARIANT containing an array value, the result is unchanged.
   * For NULL or a JSON null input, returns NULL.
   * For any other value, the result is a single-element array containing this value.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_array(col: Column): Column = {
    builtin("to_array")(col)
  }

  /**
   * Converts any VARIANT value to a string containing the JSON representation of the value.
   * If the input is NULL, the result is also NULL.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_json(col: Column): Column = {
    builtin("to_json")(col)
  }

  /**
   * Converts the input value to an object:
   *
   * For a variant value containing an object, returns this object (in a value of type OBJECT).
   * For a variant value containing JSON null or for NULL input, returns NULL.
   * For all other input values, reports an error.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_object(col: Column): Column = {
    builtin("to_object")(col)
  }

  /**
   * Converts any value to VARIANT value or NULL (if input is NULL).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_variant(col: Column): Column = {
    builtin("to_variant")(col)
  }

  /**
   * Converts any VARIANT value to a string containing the XML representation of the value.
   * If the input is NULL, the result is also NULL.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_xml(col: Column): Column = {
    builtin("to_xml")(col)
  }

  /**
   * Extracts a value from an object or array; returns NULL if either of the arguments is NULL.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def get(col1: Column, col2: Column): Column = {
    builtin("get")(col1, col2)
  }

  /**
   * Extracts a field value from an object; returns NULL if either of the arguments is NULL.
   * This function is similar to GET but applies case-insensitive matching to field names.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def get_ignore_case(obj: Column, field: Column): Column = {
    builtin("get_ignore_case")(obj, field)
  }

  /**
   * Returns an array containing the list of keys in the input object.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_keys(obj: Column): Column = {
    builtin("object_keys")(obj)
  }

  /**
   * Extracts an XML element object (often referred to as simply a tag) from a content of outer
   * XML element object by the name of the tag and its instance number (counting from 0).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def xmlget(xml: Column, tag: Column, instance: Column): Column = {
    builtin("xmlget")(xml, tag, instance)
  }

  /**
   * Extracts the first XML element object (often referred to as simply a tag) from a content of
   * outer XML element object by the name of the tag
   *
   * @group semi_func
   * @since 0.2.0
   */
  def xmlget(xml: Column, tag: Column): Column = {
    builtin("xmlget")(xml, tag)
  }

  /**
   * Extracts a value from semi-structured data using a path name.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def get_path(col: Column, path: Column): Column = {
    builtin("get_path")(col, path)
  }

  /**
   * Works like a cascading if-then-else statement.
   * A series of conditions are evaluated in sequence.
   * When a condition evaluates to TRUE, the evaluation stops and the associated
   * result (after THEN) is returned. If none of the conditions evaluate to TRUE,
   * then the result after the optional OTHERWISE is returned, if present;
   * otherwise NULL is returned.
   * For Example:
   * {{{
   *     import functions._
   *     df.select(
   *       when(col("col").is_null, lit(1))
   *         .when(col("col") === 1, lit(2))
   *         .otherwise(lit(3))
   *     )
   * }}}
   *
   * @group con_func
   * @since 0.2.0
   */
  def when(condition: Column, value: Column): CaseExpr =
    new CaseExpr(Seq((condition.expr, value.expr)))

  /**
   * Returns one of two specified expressions, depending on a condition.
   *
   * This is equivalent to an `if-then-else` expression.
   * If `condition` evaluates to TRUE, the function returns `expr1`.
   * Otherwise, the function returns `expr2`.
   *
   * @group con_func
   * @param condition The condition to evaluate.
   * @param expr1     The expression to return if the condition evaluates to TRUE.
   * @param expr2     The expression to return if the condition is not TRUE
   *                  (i.e. if it is FALSE or NULL).
   * @since 0.9.0
   */
  def iff(condition: Column, expr1: Column, expr2: Column): Column =
    builtin("iff")(condition, expr1, expr2)

  /**
   * Returns a conditional expression that you can pass to the filter or where method to
   * perform the equivalent of a WHERE ... IN query that matches rows containing a sequence of
   * values.
   *
   * The expression evaluates to true if the values in a row matches the values in one of
   * the specified sequences.
   *
   * For example, the following code returns a DataFrame that contains the rows in which
   * the columns `c1` and `c2` contain the values:
   * - `1` and `"a"`, or
   * - `2` and `"b"`
   * This is equivalent to `SELECT * FROM table WHERE (c1, c2) IN ((1, 'a'), (2, 'b'))`.
   * {{{
   *   val df2 = df.filter(functions.in(Seq(df("c1"), df("c2")), Seq(Seq(1, "a"), Seq(2, "b"))))
   * }}}
   * @group con_func
   * @param columns A sequence of the columns to compare for the IN operation.
   * @param values  A sequence containing the sequences of values to compare for the IN operation.
   * @since 0.10.0
   */
  def in(columns: Seq[Column], values: Seq[Seq[Any]]): Column =
    Column(MultipleExpression(columns.map(_.expr))).in(values)

  /**
   * Returns a conditional expression that you can pass to the filter or where method to
   * perform the equivalent of a WHERE ... IN query with the subquery represented by
   * the specified DataFrame.
   *
   * The expression evaluates to true if the value in the column is one of the values in
   * the column of the same name in a specified DataFrame.
   *
   * For example, the following code returns a DataFrame that contains the rows where
   * the values of the columns `c1` and `c2` in `df2` match the values of the columns
   * `a` and `b` in `df1`. This is equivalent to
   * SELECT * FROM table2 WHERE (c1, c2) IN (SELECT a, b FROM table1).
   * {{{
   *    val df1 = session.sql("select a, b from table1").
   *    val df2 = session.table(table2)
   *    val dfFilter = df2.filter(functions.in(Seq(col("c1"), col("c2")), df1))
   * }}}
   *
   * @group con_func
   * @param columns A sequence of the columns to compare for the IN operation.
   * @param df      The DataFrame used as the values for the IN operation
   * @since 0.10.0
   */
  def in(columns: Seq[Column], df: DataFrame): Column = {
    Column(MultipleExpression(columns.map(_.expr))).in(df)
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 1 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq1(): Column = seq1(true)

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 1 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around,
   *                       otherwise, continues at the smallest representable number
   *                       based on the given integer width.
   * @since 0.11.0
   * @group gen_func
   */
  def seq1(startsFromZero: Boolean): Column =
    builtin("seq1")(if (startsFromZero) 0 else 1)

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 2 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq2(): Column = seq2(true)

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 2 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around,
   *                       otherwise, continues at the smallest representable number
   *                       based on the given integer width.
   * @since 0.11.0
   * @group gen_func
   */
  def seq2(startsFromZero: Boolean): Column =
    builtin("seq2")(if (startsFromZero) 0 else 1)

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 4 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq4(): Column = seq4(true)

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 4 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around,
   *                       otherwise, continues at the smallest representable number
   *                       based on the given integer width.
   * @since 0.11.0
   * @group gen_func
   */
  def seq4(startsFromZero: Boolean): Column =
    builtin("seq4")(if (startsFromZero) 0 else 1)

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 8 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq8(): Column = seq8(true)

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 8 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around,
   *                       otherwise, continues at the smallest representable number
   *                       based on the given integer width.
   * @since 0.11.0
   * @group gen_func
   */
  def seq8(startsFromZero: Boolean): Column =
    builtin("seq8")(if (startsFromZero) 0 else 1)

  // scalastyle:off
  /**
   * Returns a uniformly random number, in the inclusive range (`min`, `max`)
   *
   * For example:
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   session.generator(10, seq4(), uniform(lit(1), lit(5), random())).show()
   * }}}
   *
   * @param min The lower bound
   * @param max The upper bound
   * @param gen The generator expression for the function. for more information, see
   *            [[https://docs.snowflake.com/en/sql-reference/functions-data-generation.html#label-rand-dist-functions]]
   * @since 0.11.0
   * @group gen_func
   */
  // scalastyle:on
  def uniform(min: Column, max: Column, gen: Column): Column =
    builtin("uniform")(min, max, gen)

  /**
   * Returns the concatenated input values, separated by `delimiter` string.
   *
   * For example:
   * {{{
   *   df.groupBy(df.col("col1")).agg(listagg(df.col("col2"), ",")
   *       .withinGroup(df.col("col2").asc))
   *
   *   df.select(listagg(df.col("col2"), ",", false))
   * }}}
   *
   * @param col The expression (typically a Column) that determines the values
   *            to be put into the list. The expression should evaluate to a
   *            string, or to a data type that can be cast to string.
   * @param delimiter A string delimiter.
   * @param isDistinct Whether the input expression is distinct.
   * @since 0.12.0
   * @group agg_func
   */
  def listagg(col: Column, delimiter: String, isDistinct: Boolean): Column =
    Column(ListAgg(col.expr, delimiter, isDistinct))

  /**
   * Returns the concatenated input values, separated by `delimiter` string.
   *
   * For example:
   * {{{
   *   df.groupBy(df.col("col1")).agg(listagg(df.col("col2"), ",")
   *       .withinGroup(df.col("col2").asc))
   *
   *   df.select(listagg(df.col("col2"), ",", false))
   * }}}
   *
   * @param col The expression (typically a Column) that determines the values
   *            to be put into the list. The expression should evaluate to a
   *            string, or to a data type that can be cast to string.
   * @param delimiter A string delimiter.
   * @since 0.12.0
   * @group agg_func
   */
  def listagg(col: Column, delimiter: String): Column =
    listagg(col, delimiter, isDistinct = false)

  /**
   * Returns the concatenated input values, separated by empty string.
   *
   * For example:
   * {{{
   *   df.groupBy(df.col("col1")).agg(listagg(df.col("col2"), ",")
   *       .withinGroup(df.col("col2").asc))
   *
   *   df.select(listagg(df.col("col2"), ",", false))
   * }}}
   *
   * @param col The expression (typically a Column) that determines the values
   *            to be put into the list. The expression should evaluate to a
   *            string, or to a data type that can be cast to string.
   * @since 0.12.0
   * @group agg_func
   */
  def listagg(col: Column): Column = listagg(col, "", isDistinct = false)

  /**
   * Invokes a built-in snowflake function with the specified name and arguments.
   * Arguments can be of two types
   *
   * a. [[Column]], or
   *
   * b. Basic types such as Int, Long, Double, Decimal etc. which are converted to
   * Snowpark literals.
   *
   * @group client_func
   * @since 0.1.0
   */
  def callBuiltin(functionName: String, args: Any*): Column =
    internalBuiltinFunction(false, functionName, args: _*)

  private def withExpr(expr: Expression): Column = Column(expr)

  private def registerUdf(udf: UserDefinedFunction): UserDefinedFunction = {
    val session = Session.getActiveSession
      .getOrElse(throw ErrorMessage.UDF_NO_DEFAULT_SESSION_FOUND())
    session.udf.register(None, udf)
  }

  /**
   * Calls a user-defined function (UDF) by name.
   *
   * @group udf_func
   * @since 0.1.0
   */
  def callUDF(udfName: String, cols: Any*): Column = {
    Utils.validateObjectName(udfName)
    internalBuiltinFunction(false, udfName, cols: _*)
  }

  // scalastyle:off line.size.limit
  /* Code below for udf 0-22 generated by this script
    (0 to 22).foreach { x =>
      val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
      val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
      val s = if (x > 1) "s" else ""
      val version = if (x > 10) "0.12.0" else "0.1.0"
      println(s"""
        |/**
        | * Registers a Scala closure of $x argument$s as a Snowflake Java UDF and returns the UDF.
        | * @tparam RT return type of UDF.
        | * @group udf_func
        | * @since $version
        | */
        |def udf[$typeTags](func: Function$x[$types]): UserDefinedFunction = {
        |  registerUdf(_toUdf(func))
        |}""".stripMargin)
    }
   */

  /**
   * Registers a Scala closure of 0 argument as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[RT: TypeTag](func: Function0[RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 1 argument as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[RT: TypeTag, A1: TypeTag](func: Function1[A1, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 2 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](
      func: Function2[A1, A2, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 3 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      func: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 4 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 5 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](
      func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 6 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 7 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 8 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 9 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag](
      func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 10 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.1.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag](
      func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 11 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag](
      func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 12 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag](func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT])
    : UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 13 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag](func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT])
    : UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 14 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag](
      func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT])
    : UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 15 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag](
      func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT])
    : UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 16 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag](
      func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT])
    : UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 17 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag](
      func: Function17[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 18 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag](
      func: Function18[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 19 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag](
      func: Function19[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 20 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag](
      func: Function20[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 21 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag,
      A21: TypeTag](
      func: Function21[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        A21,
        RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Registers a Scala closure of 22 arguments as a Snowflake Java UDF and returns the UDF.
   * @tparam RT return type of UDF.
   * @group udf_func
   * @since 0.12.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag,
      A11: TypeTag,
      A12: TypeTag,
      A13: TypeTag,
      A14: TypeTag,
      A15: TypeTag,
      A16: TypeTag,
      A17: TypeTag,
      A18: TypeTag,
      A19: TypeTag,
      A20: TypeTag,
      A21: TypeTag,
      A22: TypeTag](
      func: Function22[
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        A21,
        A22,
        RT]): UserDefinedFunction = {
    registerUdf(_toUdf(func))
  }

  /**
   * Function object to invoke a Snowflake builtin. Use this to invoke
   * any builtins not explicitly listed in this object.
   *
   * Example
   * {{{
   *    val repeat = functions.builtin("repeat")
   *    df.select(repeat(col("col_1"), 3))
   * }}}
   *
   * @group client_func
   * @since 0.1.0
   */
  // scalastyle:off
  case class builtin(functionName: String) {
    // scalastyle:on

    def apply(args: Any*): Column = internalBuiltinFunction(false, functionName, args: _*)
  }

  private def internalBuiltinFunction(isDistinct: Boolean, name: String, args: Any*): Column = {
    val exprs: Seq[Expression] = args.map {
      case col: Column => col.expr
      case expr: Expression => expr
      case arg => Literal(arg)
    }
    Column(FunctionExpression(name, exprs, isDistinct))
  }

}
