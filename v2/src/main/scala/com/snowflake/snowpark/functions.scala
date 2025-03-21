package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.internal.ScalaFunctions._
import com.snowflake.snowpark.internal.{
  ErrorMessage,
  OpenTelemetry,
  UDXRegistrationHandler,
  Utils
}
import com.snowflake.snowpark.types.TimestampType
import com.snowflake.snowpark.types._
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
  def col(colName: String): Column = null

  /**
   * Returns a [[Column]] with the specified name. Alias for col.
   *
   * @group client_func
   * @since 0.1.0
   */
  def column(colName: String): Column = null

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
  def col(df: DataFrame): Column = null

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
  def toScalar(df: DataFrame): Column = null

  /**
   * Creates a [[Column]] expression for a literal value.
   *
   * @group client_func
   * @since 0.1.0
   */
  def lit(literal: Any): Column = null

  /**
   * Creates a [[Column]] expression for a literal value.
   *
   * @group client_func
   * @since 0.1.0
   */
  def typedLit[T: TypeTag](literal: T): Column = null

  /**
   * Creates a [[Column]] expression from raw SQL text.
   *
   * Note that the function does not interpret or check the SQL text.
   *
   * @group client_func
   * @since 0.1.0
   */
  def sqlExpr(sqlText: String): Column = null

  /**
   * Uses HyperLogLog to return an approximation of the distinct cardinality of the input
   * (i.e. returns an approximation of `COUNT(DISTINCT col)`).
   *
   * @group agg_func
   * @since 0.1.0
   */
  def approx_count_distinct(e: Column): Column = null

  /**
   * Returns the average of non-NULL records. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def avg(e: Column): Column = null

  /**
   * Returns the correlation coefficient for non-null pairs in a group.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def corr(column1: Column, column2: Column): Column = null

  /**
   * Returns either the number of non-NULL records for the specified columns,
   * or the total number of records.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def count(e: Column): Column = null

  /**
   * Returns either the number of non-NULL distinct records for the specified columns,
   * or the total number of the distinct records. An alias of count_distinct.
   *
   * @group agg_func
   * @since 1.13.0
   */
  def countDistinct(colName: String, colNames: String*): Column = null

  /**
   * Returns either the number of non-NULL distinct records for the specified columns,
   * or the total number of the distinct records. An alias of count_distinct.
   *
   * @group agg_func
   * @since 1.13.0
   */
  def countDistinct(expr: Column, exprs: Column*): Column = null

  /**
   * Returns either the number of non-NULL distinct records for the specified columns,
   * or the total number of the distinct records.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def count_distinct(expr: Column, exprs: Column*): Column = null

  /**
   * Returns the population covariance for non-null pairs in a group.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def covar_pop(column1: Column, column2: Column): Column = null

  /**
   * Returns the sample covariance for non-null pairs in a group.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def covar_samp(column1: Column, column2: Column): Column = null

  /**
   * Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def grouping(e: Column): Column = null

  /**
   * Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def grouping_id(cols: Column*): Column = null
  /**
   * Returns the population excess kurtosis of non-NULL records.
   * If all records inside a group are NULL, the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def kurtosis(e: Column): Column = null

  /**
   * Returns the maximum value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq(1, 3, 10, 1, 3)).toDF("x")
   *   df.select(max("x")).show()
   *
   *   ----------------
   *   |"MAX(""X"")"  |
   *   ----------------
   *   |10            |
   *   ----------------
   * }}}
   *
   * @param colName The name of the column
   * @return The maximum value of the given column
   * @group agg_func
   * @since 1.13.0
   */
  def max(colName: String): Column = null
  /**
   * Returns the maximum value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def max(e: Column): Column = null

  /**
   * Returns a non-deterministic value for the specified column.
   *
   * @group agg_func
   * @since 0.12.0
   */
  def any_value(e: Column): Column = null

  /**
   * Returns the average of non-NULL records. If all records inside a group are NULL,
   * the function returns NULL. Alias of avg.
   *
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq(1, 3, 10, 1, 3)).toDF("x")
   *   df.select(mean("x")).show()
   *
   *   ----------------
   *   |"AVG(""X"")"  |
   *   ----------------
   *   |3.600000      |
   *   ----------------
   * }}}
   *
   * @param colName The name of the column
   * @return The average value of the given column
   * @group agg_func
   * @since 1.13.0
   */
  def mean(colName: String): Column = null

  /**
   * Returns the average of non-NULL records. If all records inside a group are NULL,
   * the function returns NULL. Alias of avg
   *
   * @group agg_func
   * @since 0.1.0
   */
  def mean(e: Column): Column = null

  /**
   * Returns the median value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * @group agg_func
   * @since 0.5.0
   */
  def median(e: Column): Column = null

  /**
   * Returns the minimum value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq(1, 3, 10, 1, 3)).toDF("x")
   *   df.select(min("x")).show()
   *
   *   ----------------
   *   |"MIN(""X"")"  |
   *   ----------------
   *   |1             |
   *   ----------------
   * }}}
   *
   * @param colName The name of the column
   * @return The minimum value of the given column
   * @group agg_func
   * @since 1.13.0
   */
  def min(colName: String): Column = null

  /**
   * Returns the minimum value for the records in a group. NULL values are ignored unless all
   * the records are NULL, in which case a NULL value is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def min(e: Column): Column = null

  /**
   * Returns the sample skewness of non-NULL records. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def skew(e: Column): Column = null

  /**
   * Returns the sample standard deviation (square root of sample variance) of non-NULL values.
   * If all records inside a group are NULL, returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def stddev(e: Column): Column = null

  /**
   * Returns the sample standard deviation (square root of sample variance) of non-NULL values.
   * If all records inside a group are NULL, returns NULL. Alias of stddev
   *
   * @group agg_func
   * @since 0.1.0
   */
  def stddev_samp(e: Column): Column = null

  /**
   * Returns the population standard deviation (square root of variance) of non-NULL values.
   * If all records inside a group are NULL, returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def stddev_pop(e: Column): Column = null

  /**
   * Returns the sum of non-NULL records in a group. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def sum(e: Column): Column = null

  /**
   * Returns the sum of non-NULL records in a group. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 1.12.0
   * @param colName The input column name
   * @return The result column
   */
  def sum(colName: String): Column = null

  /**
   * Returns the sum of non-NULL distinct records in a group. You can use the DISTINCT keyword to
   * compute the sum of unique non-null values. If all records inside a group are NULL,
   * the function returns NULL.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def sum_distinct(e: Column): Column = null

  /**
   * Returns the sample variance of non-NULL records in a group.
   * If all records inside a group are NULL, a NULL is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def variance(e: Column): Column = null

  /**
   * Returns the sample variance of non-NULL records in a group.
   * If all records inside a group are NULL, a NULL is returned.
   * Alias of var_samp
   *
   * @group agg_func
   * @since 0.1.0
   */
  def var_samp(e: Column): Column = null

  /**
   * Returns the population variance of non-NULL records in a group.
   * If all records inside a group are NULL, a NULL is returned.
   *
   * @group agg_func
   * @since 0.1.0
   */
  def var_pop(e: Column): Column = null

  /**
   * Returns an approximated value for the desired percentile.
   * This function uses the t-Digest algorithm.
   *
   * @group agg_func
   * @since 0.2.0
   */
  def approx_percentile(col: Column, percentile: Double): Column = null

  /**
   * Returns the internal representation of the t-Digest state (as a JSON object) at the end of
   * aggregation.
   * This function uses the t-Digest algorithm.
   *
   * @group agg_func
   * @since 0.2.0
   */
  def approx_percentile_accumulate(col: Column): Column = null

  /**
   * Returns the desired approximated percentile value for the specified t-Digest state.
   * APPROX_PERCENTILE_ESTIMATE(APPROX_PERCENTILE_ACCUMULATE(.)) is equivalent to
   * APPROX_PERCENTILE(.).
   *
   * @group agg_func
   * @since 0.2.0
   */
  def approx_percentile_estimate(state: Column, percentile: Double): Column = null

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
  def approx_percentile_combine(state: Column): Column = null

  /**
   * Finds the cumulative distribution of a value with regard to other values
   * within the same window partition.
   *
   * @group win_func
   * @since 0.1.0
   */
  def cume_dist(): Column = null

  /**
   * Returns the rank of a value within a group of values, without gaps in the ranks.
   * The rank value starts at 1 and continues up sequentially.
   * If two values are the same, they will have the same rank.
   *
   * @group win_func
   * @since 0.1.0
   */
  def dense_rank(): Column = null

  /**
   * Accesses data in a previous row in the same result set without having to
   * join the table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lag(e: Column, offset: Int, defaultValue: Column): Column =
    null

  /**
   * Accesses data in a previous row in the same result set without having to
   * join the table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lag(e: Column, offset: Int): Column = null

  /**
   * Accesses data in a previous row in the same result set without having to
   * join the table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lag(e: Column): Column = null

  /**
   * Accesses data in a subsequent row in the same result set without having to join the
   * table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lead(e: Column, offset: Int, defaultValue: Column): Column = null

  /**
   * Accesses data in a subsequent row in the same result set without having to join the
   * table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lead(e: Column, offset: Int): Column = null

  /**
   * Accesses data in a subsequent row in the same result set without having to join the
   * table to itself.
   *
   * @group win_func
   * @since 0.1.0
   */
  def lead(e: Column): Column = null

  /**
   * Divides an ordered data set equally into the number of buckets specified by n.
   * Buckets are sequentially numbered 1 through n.
   *
   * @group win_func
   * @since 0.1.0
   */
  def ntile(n: Column): Column = null

  /**
   * Returns the relative rank of a value within a group of values, specified as a percentage
   * ranging from 0.0 to 1.0.
   *
   * @group win_func
   * @since 0.1.0
   */
  def percent_rank(): Column = null

  /**
   * Returns the rank of a value within an ordered group of values.
   * The rank value starts at 1 and continues up.
   *
   * @group win_func
   * @since 0.1.0
   */
  def rank(): Column = null

  /**
   * Returns a unique row number for each row within a window partition.
   * The row number starts at 1 and continues up sequentially.
   *
   * @group win_func
   * @since 0.1.0
   */
  def row_number(): Column = null

  /**
   * Returns the first non-NULL expression among its arguments,
   * or NULL if all its arguments are NULL.
   *
   * @group con_func
   * @since 0.1.0
   */
  def coalesce(e: Column*): Column = null

  /**
   * Return true if the value in the column is not a number (NaN).
   *
   * @group con_func
   * @since 0.1.0
   */
  def equal_nan(e: Column): Column = null

  /**
   * Return true if the value in the column is null.
   *
   * @group con_func
   * @since 0.1.0
   */
  def is_null(e: Column): Column = null

  /**
   * Returns the negation of the value in the column (equivalent to a unary minus).
   *
   * @group client_func
   * @since 0.1.0
   */
  def negate(e: Column): Column = null

  /**
   * Returns the inverse of a boolean expression.
   *
   * @group client_func
   * @since 0.1.0
   */
  def not(e: Column): Column = null

  /**
   * Each call returns a pseudo-random 64-bit integer.
   *
   * @group gen_func
   * @since 0.1.0
   */
  def random(seed: Long): Column = null

  /**
   * Each call returns a pseudo-random 64-bit integer.
   *
   * @group gen_func
   * @since 0.1.0
   */
  def random(): Column = null

  /**
   * Returns the bitwise negation of a numeric expression.
   *
   * @group bit_func
   * @since 0.1.0
   */
  def bitnot(e: Column): Column = null

  /**
   * Converts an input expression to a decimal
   *
   * @group num_func
   * @since 0.5.0
   */
  def to_decimal(expr: Column, precision: Int, scale: Int): Column = null

  /**
   * Performs division like the division operator (/),
   * but returns 0 when the divisor is 0 (rather than reporting an error).
   *
   * @group num_func
   * @since 0.1.0
   */
  def div0(dividend: Column, divisor: Column): Column = null

  /**
   * Returns the square-root of a non-negative numeric expression.
   *
   * @group num_func
   * @since 0.1.0
   */
  def sqrt(e: Column): Column = null

  /**
   * Returns the absolute value of a numeric expression.
   *
   * @group num_func
   * @since 0.1.0
   */
  def abs(e: Column): Column = null

  /**
   * Computes the inverse cosine (arc cosine) of its input; the result is a number in the
   * interval [-pi, pi].
   *
   * @group num_func
   * @since 0.1.0
   */
  def acos(e: Column): Column = null

  /**
   * Computes the inverse sine (arc sine) of its argument; the result is a number in the
   * interval [-pi, pi].
   *
   * @group num_func
   * @since 0.1.0
   */
  def asin(e: Column): Column = null

  /**
   * Computes the inverse tangent (arc tangent) of its argument; the result is a number in
   * the interval [-pi, pi].
   *
   * @group num_func
   * @since 0.1.0
   */
  def atan(e: Column): Column = null

  /**
   * Computes the inverse tangent (arc tangent) of the ratio of its two arguments.
   *
   * @group num_func
   * @since 0.1.0
   */
  def atan2(y: Column, x: Column): Column = null

  /**
   * Returns values from the specified column rounded to the nearest equal or larger integer.
   *
   * @group num_func
   * @since 0.1.0
   */
  def ceil(e: Column): Column = null

  /**
   * Computes the cosine of its argument; the argument should be expressed in radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def cos(e: Column): Column = null

  /**
   * Computes the hyperbolic cosine of its argument.
   *
   * @group num_func
   * @since 0.1.0
   */
  def cosh(e: Column): Column = null
  /**
   * Computes Euler's number e raised to a floating-point value.
   *
   * @group num_func
   * @since 0.1.0
   */
  def exp(e: Column): Column = null

  /**
   * Computes the factorial of its input. The input argument must be an integer
   * expression in the range of 0 to 33.
   *
   * @group num_func
   * @since 0.1.0
   */
  def factorial(e: Column): Column = null

  /**
   * Returns values from the specified column rounded to the nearest equal or smaller integer.
   *
   * @group num_func
   * @since 0.1.0
   */
  def floor(e: Column): Column = null

  /**
   * Returns the largest value from a list of expressions. If any of the argument values is NULL,
   * the result is NULL. GREATEST supports all data types, including VARIANT.
   *
   * @group con_func
   * @since 0.1.0
   */
  def greatest(exprs: Column*): Column = null

  /**
   * Returns the smallest value from a list of expressions. LEAST supports all data types,
   * including VARIANT.
   *
   * @group con_func
   * @since 0.1.0
   */
  def least(exprs: Column*): Column = null

  /**
   * Returns the logarithm of a numeric expression.
   *
   * @group num_func
   * @since 0.1.0
   */
  def log(base: Column, a: Column): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * @group num_func
   * @since 0.1.0
   */
  def pow(l: Column, r: Column): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * Example:
   * {{{
   *   val df = session.sql(
   *     "select * from (values (0.1, 2), (2, 3), (2, 0.5), (2, -1)) as T(base, exponent)")
   *   df.select(col("base"), col("exponent"), pow(col("base"), "exponent").as("result")).show()
   *
   *   ----------------------------------------------
   *   |"BASE"  |"EXPONENT"  |"RESULT"              |
   *   ----------------------------------------------
   *   |0.1     |2.0         |0.010000000000000002  |
   *   |2.0     |3.0         |8.0                   |
   *   |2.0     |0.5         |1.4142135623730951    |
   *   |2.0     |-1.0        |0.5                   |
   *   ----------------------------------------------
   * }}}
   *
   * @param l The numeric column representing the base.
   * @param r The name of the numeric column representing the exponent.
   * @return A column containing the result of raising `l` to the power of `r`.
   * @group num_func
   * @since 1.15.0
   */
  def pow(l: Column, r: String): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * Example:
   * {{{
   *   val df = session.sql(
   *     "select * from (values (0.1, 2), (2, 3), (2, 0.5), (2, -1)) as T(base, exponent)")
   *   df.select(col("base"), col("exponent"), pow("base", col("exponent")).as("result")).show()
   *
   *   ----------------------------------------------
   *   |"BASE"  |"EXPONENT"  |"RESULT"              |
   *   ----------------------------------------------
   *   |0.1     |2.0         |0.010000000000000002  |
   *   |2.0     |3.0         |8.0                   |
   *   |2.0     |0.5         |1.4142135623730951    |
   *   |2.0     |-1.0        |0.5                   |
   *   ----------------------------------------------
   * }}}
   *
   * @param l The name of the numeric column representing the base.
   * @param r The numeric column representing the exponent.
   * @return A column containing the result of raising `l` to the power of `r`.
   * @group num_func
   * @since 1.15.0
   */
  def pow(l: String, r: Column): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * Example:
   * {{{
   *   val df = session.sql(
   *     "select * from (values (0.1, 2), (2, 3), (2, 0.5), (2, -1)) as T(base, exponent)")
   *   df.select(col("base"), col("exponent"), pow("base", "exponent").as("result")).show()
   *
   *   ----------------------------------------------
   *   |"BASE"  |"EXPONENT"  |"RESULT"              |
   *   ----------------------------------------------
   *   |0.1     |2.0         |0.010000000000000002  |
   *   |2.0     |3.0         |8.0                   |
   *   |2.0     |0.5         |1.4142135623730951    |
   *   |2.0     |-1.0        |0.5                   |
   *   ----------------------------------------------
   * }}}
   *
   * @param l The name of the numeric column representing the base.
   * @param r The name of the numeric column representing the exponent.
   * @return A column containing the result of raising `l` to the power of `r`.
   * @group num_func
   * @since 1.15.0
   */
  def pow(l: String, r: String): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * Example:
   * {{{
   *   val df = session.sql("select * from (values (0.5), (2), (2.5), (4)) as T(base)")
   *   df.select(col("base"), lit(2.0).as("exponent"), pow(col("base"), 2.0).as("result")).show()
   *
   *   ----------------------------------
   *   |"BASE"  |"EXPONENT"  |"RESULT"  |
   *   ----------------------------------
   *   |0.5     |2.0         |0.25      |
   *   |2.0     |2.0         |4.0       |
   *   |2.5     |2.0         |6.25      |
   *   |4.0     |2.0         |16.0      |
   *   ----------------------------------
   * }}}
   *
   * @param l The numeric column representing the base.
   * @param r The value of the exponent.
   * @return A column containing the result of raising `l` to the power of `r`.
   * @group num_func
   * @since 1.15.0
   */
  def pow(l: Column, r: Double): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * Example:
   * {{{
   *   val df = session.sql("select * from (values (0.5), (2), (2.5), (4)) as T(base)")
   *   df.select(col("base"), lit(2.0).as("exponent"), pow("base", 2.0).as("result")).show()
   *
   *   ----------------------------------
   *   |"BASE"  |"EXPONENT"  |"RESULT"  |
   *   ----------------------------------
   *   |0.5     |2.0         |0.25      |
   *   |2.0     |2.0         |4.0       |
   *   |2.5     |2.0         |6.25      |
   *   |4.0     |2.0         |16.0      |
   *   ----------------------------------
   * }}}
   *
   * @param l The name of the numeric column representing the base.
   * @param r The value of the exponent.
   * @return A column containing the result of raising `l` to the power of `r`.
   * @group num_func
   * @since 1.15.0
   */
  def pow(l: String, r: Double): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * Example:
   * {{{
   *   val df = session.sql("select * from (values (0.5), (2), (2.5), (4)) as T(exponent)")
   *   df.select(lit(2.0).as("base"), col("exponent"), pow(2.0, col("exponent")).as("result"))
   *     .show()
   *
   *   --------------------------------------------
   *   |"BASE"  |"EXPONENT"  |"RESULT"            |
   *   --------------------------------------------
   *   |2.0     |0.5         |1.4142135623730951  |
   *   |2.0     |2.0         |4.0                 |
   *   |2.0     |2.5         |5.656854249492381   |
   *   |2.0     |4.0         |16.0                |
   *   --------------------------------------------
   * }}}
   *
   * @param l The value of the base.
   * @param r The numeric column representing the exponent.
   * @return A column containing the result of raising `l` to the power of `r`.
   * @group num_func
   * @since 1.15.0
   */
  def pow(l: Double, r: Column): Column = null

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * Example:
   * {{{
   *   val df = session.sql("select * from (values (0.5), (2), (2.5), (4)) as T(exponent)")
   *   df.select(lit(2.0).as("base"), col("exponent"), pow(2.0, "exponent").as("result")).show()
   *
   *   --------------------------------------------
   *   |"BASE"  |"EXPONENT"  |"RESULT"            |
   *   --------------------------------------------
   *   |2.0     |0.5         |1.4142135623730951  |
   *   |2.0     |2.0         |4.0                 |
   *   |2.0     |2.5         |5.656854249492381   |
   *   |2.0     |4.0         |16.0                |
   *   --------------------------------------------
   * }}}
   *
   * @param l The value of the base.
   * @param r The name of the numeric column representing the exponent.
   * @return A column containing the result of raising `l` to the power of `r`.
   * @group num_func
   * @since 1.15.0
   */
  def pow(l: Double, r: String): Column = null

  /**
   * Rounds the numeric values of the given column `e` to the `scale` decimal places using the
   * half away from zero rounding mode.
   *
   * Example:
   * {{{
   *   val df = session.sql(
   *     "select * from (values (-3.78), (-2.55), (1.23), (2.55), (3.78)) as T(a)")
   *   df.select(round(col("a"), lit(1)).alias("round")).show()
   *
   *   -----------
   *   |"ROUND"  |
   *   -----------
   *   |-3.8     |
   *   |-2.6     |
   *   |1.2      |
   *   |2.6      |
   *   |3.8      |
   *   -----------
   * }}}
   *
   * @param e The column of numeric values to round.
   * @param scale A column representing the number of decimal places to which `e` should be rounded.
   * @return A new column containing the rounded numeric values.
   * @group num_func
   * @since 0.1.0
   */
  def round(e: Column, scale: Column): Column = null

  /**
   * Rounds the numeric values of the given column `e` to 0 decimal places using the
   * half away from zero rounding mode.
   *
   * Example:
   * {{{
   *   val df = session.sql("select * from (values (-3.7), (-2.5), (1.2), (2.5), (3.7)) as T(a)")
   *   df.select(round(col("a")).alias("round")).show()
   *
   *   -----------
   *   |"ROUND"  |
   *   -----------
   *   |-4       |
   *   |-3       |
   *   |1        |
   *   |3        |
   *   |4        |
   *   -----------
   * }}}
   *
   * @param e The column of numeric values to round.
   * @return A new column containing the rounded numeric values.
   * @group num_func
   * @since 0.1.0
   */
  def round(e: Column): Column = null

  /**
   * Rounds the numeric values of the given column `e` to the `scale` decimal places using the
   * half away from zero rounding mode.
   *
   * Example:
   * {{{
   *   val df = session.sql(
   *     "select * from (values (-3.78), (-2.55), (1.23), (2.55), (3.78)) as T(a)")
   *   df.select(round(col("a"), 1).alias("round")).show()
   *
   *   -----------
   *   |"ROUND"  |
   *   -----------
   *   |-3.8     |
   *   |-2.6     |
   *   |1.2      |
   *   |2.6      |
   *   |3.8      |
   *   -----------
   * }}}
   *
   * @param e The column of numeric values to round.
   * @param scale The number of decimal places to which `e` should be rounded.
   * @return A new column containing the rounded numeric values.
   * @group num_func
   * @since 1.14.0
   */
  def round(e: Column, scale: Int): Column = null

  /**
   * Shifts the bits for a numeric expression numBits positions to the left.
   *
   * @group bit_func
   * @since 0.1.0
   */
  def bitshiftleft(e: Column, numBits: Column): Column = null

  /**
   * Shifts the bits for a numeric expression numBits positions to the right.
   *
   * @group bit_func
   * @since 0.1.0
   */
  def bitshiftright(e: Column, numBits: Column): Column = null

  /**
   * Computes the sine of its argument; the argument should be expressed in radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def sin(e: Column): Column = null

  /**
   * Computes the hyperbolic sine of its argument.
   *
   * @group num_func
   * @since 0.1.0
   */
  def sinh(e: Column): Column = null

  /**
   * Computes the tangent of its argument; the argument should be expressed in radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def tan(e: Column): Column = null

  /**
   * Computes the hyperbolic tangent of its argument.
   *
   * @group num_func
   * @since 0.1.0
   */
  def tanh(e: Column): Column = null

  /**
   * Converts radians to degrees.
   *
   * @group num_func
   * @since 0.1.0
   */
  def degrees(e: Column): Column = null

  /**
   * Converts degrees to radians.
   *
   * @group num_func
   * @since 0.1.0
   */
  def radians(e: Column): Column = null

  /**
   * Returns a 32-character hex-encoded string containing the 128-bit MD5 message digest.
   *
   * @group str_func
   * @since 0.1.0
   */
  def md5(e: Column): Column = null

  /**
   * Returns a 40-character hex-encoded string containing the 160-bit SHA-1 message digest.
   *
   * @group str_func
   * @since 0.1.0
   */
  def sha1(e: Column): Column = null

  /**
   * Returns a hex-encoded string containing the N-bit SHA-2 message digest,
   * where N is the specified output digest size.
   *
   * @group str_func
   * @since 0.1.0
   */
  def sha2(e: Column, numBits: Int): Column = null

  /**
   * Returns a signed 64-bit hash value. Note that HASH never returns NULL, even for NULL inputs.
   *
   * @group utl_func
   * @since 0.1.0
   */
  def hash(cols: Column*): Column = null

  /**
   * Returns the ASCII code for the first character of a string. If the string is empty,
   * a value of 0 is returned.
   *
   * @group str_func
   * @since 0.1.0
   */
  def ascii(e: Column): Column = null

  /**
   * Concatenates two or more strings, or concatenates two or more binary values.
   * If any of the values is null, the result is also null.
   *
   * @group str_func
   * @since 0.1.0
   */
  def concat_ws(separator: Column, exprs: Column*): Column = null

  /**
   * Returns the input string with the first letter of each word in uppercase
   * and the subsequent letters in lowercase.
   *
   * @group str_func
   * @since 0.1.0
   */
  def initcap(e: Column): Column = null

  /**
   * Returns the length of an input string or binary value. For strings,
   * the length is the number of characters, and UTF-8 characters are counted as a
   * single character. For binary, the length is the number of bytes.
   *
   * @group str_func
   * @since 0.1.0
   */
  def length(e: Column): Column = null

  /**
   * Returns the input string with all characters converted to lowercase.
   *
   * @group str_func
   * @since 0.1.0
   */
  def lower(e: Column): Column = null

  /**
   * Left-pads a string with characters from another string, or left-pads a
   * binary value with bytes from another binary value.
   *
   * @group str_func
   * @since 0.1.0
   */
  def lpad(str: Column, len: Column, pad: Column): Column =
    null

  /**
   * Removes leading characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def ltrim(e: Column, trimString: Column): Column = null

  /**
   * Removes leading characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def ltrim(e: Column): Column = null

  /**
   * Right-pads a string with characters from another string, or right-pads a
   * binary value with bytes from another binary value.
   *
   * @group str_func
   * @since 0.1.0
   */
  def rpad(str: Column, len: Column, pad: Column): Column = null

//  /**
//   * Builds a string by repeating the input for the specified number of times.
//   *
//   * @group str_func
//   * @since 0.1.0
//   */
//  def repeat(str: Column, n: Column): Column = withExpr {
//    StringRepeat(str.expr, n.expr)
//  }

  /**
   * Removes trailing characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def rtrim(e: Column, trimString: Column): Column = null

  /**
   * Removes trailing characters, including whitespace, from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def rtrim(e: Column): Column = null

  /**
   * Returns a string that contains a phonetic representation of the input string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def soundex(e: Column): Column = null

  /**
   * Splits a given string with a given separator and returns the result in an array of strings.
   * To specify a string separator, use the lit() function.
   *
   * Example 1:
   * {{{
   *   val df = session.createDataFrame(
   *                  Seq(("many-many-words", "-"), ("hello--hello", "--"))).toDF("V", "D")
   *   df.select(split(col("V"), col("D"))).show()
   * }}}
   *   -------------------------
   *   |"SPLIT(""V"", ""D"")"  |
   *   -------------------------
   *   |[                      |
   *   |  "many",              |
   *   |  "many",              |
   *   |  "words"              |
   *   |]                      |
   *   |[                      |
   *   |  "hello",             |
   *   |  "hello"              |
   *   |]                      |
   *   -------------------------
   *
   * Example 2:
   * {{{
   *   val df = session.createDataFrame(Seq("many-many-words", "hello-hi-hello")).toDF("V")
   *   df.select(split(col("V"), lit("-"))).show()
   * }}}
   *   -------------------------
   *   |"SPLIT(""V"", ""D"")"  |
   *   -------------------------
   *   |[                      |
   *   |  "many",              |
   *   |  "many",              |
   *   |  "words"              |
   *   |]                      |
   *   |[                      |
   *   |  "hello",             |
   *   |  "hello"              |
   *   |]                      |
   *   -------------------------
   *
   * @group str_func
   * @since 0.1.0
   */
  def split(str: Column, pattern: Column): Column = null

  /**
   * Returns the portion of the string or binary value str,
   * starting from the character/byte specified by pos, with limited length.
   *
   * @group str_func
   * @since 0.1.0
   */
  def substring(str: Column, pos: Column, len: Column): Column =
    null

  /**
   * Translates src from the characters in matchingString to the characters in replaceString.
   *
   * @group str_func
   * @since 0.1.0
   */
  def translate(src: Column, matchingString: Column, replaceString: Column): Column =
    null

  /**
   * Removes leading and trailing characters from a string.
   *
   * @group str_func
   * @since 0.1.0
   */
  def trim(e: Column, trimString: Column): Column = null

  /**
   * Returns the input string with all characters converted to uppercase.
   *
   * @group str_func
   * @since 0.1.0
   */
  def upper(e: Column): Column = null

  /**
   * Returns true if col contains str.
   *
   * @group str_func
   * @since 0.1.0
   */
  def contains(col: Column, str: Column): Column = null

  /**
   * Returns true if col starts with str.
   *
   * @group str_func
   * @since 0.1.0
   */
  def startswith(col: Column, str: Column): Column = null

  /**
   * Converts a Unicode code point (including 7-bit ASCII) into the character
   * that matches the input Unicode.
   *
   * @group str_func
   * @since 0.1.0
   */
  def char(col: Column): Column = null

  /**
   * Adds or subtracts a specified number of months to a date or timestamp,
   * preserving the end-of-month information.
   *
   * @group date_func
   * @since 0.1.0
   */
  def add_months(startDate: Column, numMonths: Column): Column =
    null

  /**
   * Returns the current date of the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_date(): Column = null

  /**
   * Returns the current timestamp for the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_timestamp(): Column = null

  /**
   * Returns the name of the region for the account where the current user is logged in.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_region(): Column = null

  /**
   * Returns the current time for the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_time(): Column = null

  /**
   * Returns the current Snowflake version.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_version(): Column = null

  /**
   * Returns the account used by the user's current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_account(): Column = null

  /**
   * Returns the name of the role in use for the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_role(): Column = null

  /**
   * Returns a JSON string that lists all roles granted to the current user.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_available_roles(): Column = null

  /**
   * Returns a unique system identifier for the Snowflake session corresponding
   * to the present connection.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_session(): Column = null

  /**
   * Returns the SQL text of the statement that is currently executing.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_statement(): Column = null

  /**
   * Returns the name of the user currently logged into the system.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_user(): Column = null

  /**
   * Returns the name of the database in use for the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_database(): Column = null

  /**
   * Returns the name of the schema in use by the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_schema(): Column = null

  /**
   * Returns active search path schemas.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_schemas(): Column = null

  /**
   * Returns the name of the warehouse in use for the current session.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def current_warehouse(): Column = null

  /**
   * Returns the current timestamp for the system, but in the UTC time zone.
   *
   * @group cont_func
   * @since 0.1.0
   */
  def sysdate(): Column = null

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
    null

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
    null

  /**
   * Extracts the year from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def year(e: Column): Column = null

  /**
   * Extracts the quarter from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def quarter(e: Column): Column = null

  /**
   * Extracts the month from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def month(e: Column): Column = null

  /**
   * Extracts the day of week from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayofweek(e: Column): Column = null

  /**
   * Extracts the day of month from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayofmonth(e: Column): Column = null

  /**
   * Extracts the day of year from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayofyear(e: Column): Column = null

  /**
   * Extracts the hour from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def hour(e: Column): Column = null

  /**
   * Returns the last day of the specified date part for a date or timestamp.
   * Commonly used to return the last day of the month for a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def last_day(e: Column): Column = null

  /**
   * Extracts the minute from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def minute(e: Column): Column = null

  /**
   * Returns the date of the first specified DOW (day of week) that occurs after the input date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def next_day(date: Column, dayOfWeek: Column): Column = null

  /**
   * Returns the date of the first specified DOW (day of week) that occurs before the input date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def previous_day(date: Column, dayOfWeek: Column): Column =
    null

  /**
   * Extracts the second from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def second(e: Column): Column = null

  /**
   * Extracts the week of year from a date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def weekofyear(e: Column): Column = null

  /**
   * Converts an input expression into the corresponding timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_timestamp(s: Column): Column = null

  /**
   * Converts an input expression into the corresponding timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_timestamp(s: Column, fmt: Column): Column = null

  /**
   * Converts an input expression to a date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_date(e: Column): Column = null

  /**
   * Converts an input expression to a date.
   *
   * @group date_func
   * @since 0.1.0
   */
  def to_date(e: Column, fmt: Column): Column = null

  /**
   * Creates a date from individual numeric components that represent the year,
   * month, and day of the month.
   *
   * @group date_func
   * @since 0.1.0
   */
  def date_from_parts(year: Column, month: Column, day: Column): Column =
    null

  /**
   * Creates a time from individual numeric components.
   *
   * @group date_func
   * @since 0.1.0
   */
  def time_from_parts(hour: Column, minute: Column, second: Column, nanoseconds: Column): Column =
    null

  /**
   * Creates a time from individual numeric components.
   *
   * @group date_func
   * @since 0.1.0
   */
  def time_from_parts(hour: Column, minute: Column, second: Column): Column =
    null

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
    null

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
    null

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_from_parts(dateExpr: Column, timeExpr: Column): Column =
    null

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
    null

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
    null

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
    null

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
    null

  /**
   * Creates a timestamp from individual numeric components.
   * If no time zone is in effect, the function can be used to create a timestamp
   * from a date expression and a time expression.
   *
   * @group date_func
   * @since 0.1.0
   */
  def timestamp_ntz_from_parts(dateExpr: Column, timeExpr: Column): Column =
    null

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
                               second: Column): Column = null

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
                               nanosecond: Column): Column = null

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
                               timeZone: Column): Column = null

  /**
   * Extracts the three-letter day-of-week name from the specified date or
   * timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def dayname(expr: Column): Column = null

  /**
   * Extracts the three-letter month name from the specified date or timestamp.
   *
   * @group date_func
   * @since 0.1.0
   */
  def monthname(expr: Column): Column = null

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
    null

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
    null

  /**
   * Rounds the input expression down to the nearest (or equal) integer closer to zero,
   * or to the nearest equal or smaller value with the specified number of
   * places after the decimal point.
   *
   * @group num_func
   * @since 0.1.0
   */
  def trunc(expr: Column, scale: Column): Column = null

  /**
   * Truncates a DATE, TIME, or TIMESTAMP to the specified precision.
   *
   * @group date_func
   * @since 0.1.0
   */
  def date_trunc(format: String, timestamp: Column): Column = null

  /**
   * Concatenates one or more strings, or concatenates one or more binary values.
   * If any of the values is null, the result is also null.
   *
   * @group str_func
   * @since 0.1.0
   */
  def concat(exprs: Column*): Column = null

//  /**
//   * Compares whether two arrays have at least one element in common.
//   * Returns TRUE if there is at least one element in common; otherwise returns FALSE.
//   * The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.
//   *
//   * @group semi_func
//   * @since 0.1.0
//   */
//  def arrays_overlap(a1: Column, a2: Column): Column = withExpr {
//    ArraysOverlap(a1.expr, a2.expr)
//  }

  /**
   * Returns TRUE if expr ends with str.
   *
   * @group str_func
   * @since 0.1.0
   */
  def endswith(expr: Column, str: Column): Column =
    null

  /**
   * Replaces a substring of the specified length, starting at the specified position,
   * with a new string or binary value.
   *
   * @group str_func
   * @since 0.1.0
   */
  def insert(baseExpr: Column, position: Column, length: Column, insertExpr: Column): Column =
    null

  /**
   * Returns a left most substring of strExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def left(strExpr: Column, lengthExpr: Column): Column =
    null

  /**
   * Returns a right most substring of strExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def right(strExpr: Column, lengthExpr: Column): Column =
    null

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
                    parameters: Column): Column = null

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
    null

  /**
   * Returns the subject with the specified pattern (or all occurrences of the pattern) removed.
   * If no matches are found, returns the original subject.
   *
   * @group str_func
   * @since 1.9.0
   */
  def regexp_replace(strExpr: Column, pattern: Column): Column =
    null

  /**
   * Returns the subject with the specified pattern (or all occurrences of the pattern)
   * replaced by a replacement string. If no matches are found,
   * returns the original subject.
   *
   * @group str_func
   * @since 1.9.0
   */
  def regexp_replace(strExpr: Column, pattern: Column, replacement: Column): Column =
    null

  /**
   * Removes all occurrences of a specified strExpr,
   * and optionally replaces them with replacement.
   *
   * @group str_func
   * @since 0.1.0
   */
  def replace(strExpr: Column, pattern: Column, replacement: Column): Column =
    null

  /**
   * Removes all occurrences of a specified strExpr,
   * and optionally replaces them with replacement.
   *
   * @group str_func
   * @since 0.1.0
   */
  def replace(strExpr: Column, pattern: Column): Column =
    null

  /**
   * Searches for targetExpr in sourceExpr and, if successful,
   * returns the position (1-based) of the targetExpr in sourceExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def charindex(targetExpr: Column, sourceExpr: Column): Column =
    null

  /**
   * Searches for targetExpr in sourceExpr and, if successful,
   * returns the position (1-based) of the targetExpr in sourceExpr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def charindex(targetExpr: Column, sourceExpr: Column, position: Column): Column =
    null

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
  def collate(expr: Column, collationSpec: String): Column = null

  /**
   * Returns the collation specification of expr.
   *
   * @group str_func
   * @since 0.1.0
   */
  def collation(expr: Column): Column = null

//  /**
//   * Returns an ARRAY that contains the matching elements in the two input ARRAYs.
//   *
//   * @group semi_func
//   * @since 0.1.0
//   */
//  def array_intersection(col1: Column, col2: Column): Column = withExpr {
//    ArrayIntersect(col1.expr, col2.expr)
//  }

  /**
   * Returns true if the specified VARIANT column contains an ARRAY value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_array(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a Boolean value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_boolean(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a binary value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_binary(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a string value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_char(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a string value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_varchar(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a DATE value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_date(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a DATE value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_date_value(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a fixed-point decimal value or integer.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_decimal(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a floating-point value, fixed-point
   * decimal, or integer.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_double(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a floating-point value, fixed-point
   * decimal, or integer.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_real(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains an integer value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_integer(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column is a JSON null value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_null_value(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains an OBJECT value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_object(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a TIME value.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_time(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value to be interpreted
   * using the local time zone.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_timestamp_ltz(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value with no time zone.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_timestamp_ntz(col: Column): Column = null

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value with a time zone.
   *
   * @group semi_func
   * @since 0.1.0
   */
  def is_timestamp_tz(col: Column): Column = null

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
  def check_json(col: Column): Column = null

  /**
   * Checks the validity of an XML document.
   * If the input string is a valid XML document or a NULL (i.e. no error would occur when parsing
   * the input string), the function returns NULL.
   * In case of an XML parsing error, the output string contains the error message.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def check_xml(col: Column): Column = null

  /**
   * Parses a JSON string and returns the value of an element at a specified path in the resulting
   * JSON document.
   *
   * @param col Column containing the JSON string that should be parsed.
   * @param path Column containing the path to the element that should be extracted.
   * @group semi_func
   * @since 0.2.0
   */
  def json_extract_path_text(col: Column, path: Column): Column = null

  /**
   * Parse the value of the specified column as a JSON string and returns the resulting JSON
   * document.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def parse_json(col: Column): Column = null

  /**
   * Parse the value of the specified column as a JSON string and returns the resulting XML
   * document.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def parse_xml(col: Column): Column = null

  /**
   * Converts a JSON "null" value in the specified column to a SQL NULL value.
   * All other VARIANT values in the column are returned unchanged.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def strip_null_value(col: Column): Column = null

  /**
   * Returns the input values, pivoted into an ARRAY.
   * If the input is empty, an empty ARRAY is returned.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def array_agg(col: Column): Column = null

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
  def array_append(array: Column, element: Column): Column = null

  /**
   * Returns the concatenation of two ARRAYs.
   *
   * @param array1 Column containing the source ARRAY.
   * @param array2 Column containing the ARRAY to be appended to {@code array1}.
   * @group semi_func
   * @since 0.2.0
   */
  def array_cat(array1: Column, array2: Column): Column = null

  /**
   * Returns a compacted ARRAY with missing and null values removed,
   * effectively converting sparse arrays into dense arrays.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def array_compact(array: Column): Column = null

  /**
   * Returns an ARRAY constructed from zero, one, or more inputs.
   *
   * @param cols Columns containing the values (or expressions that evaluate to values). The
   *             values do not all need to be of the same data type.
   * @group semi_func
   * @since 0.2.0
   */
  def array_construct(cols: Column*): Column = null

  /**
   * Returns an ARRAY constructed from zero, one, or more inputs;
   * the constructed ARRAY omits any NULL input values.
   *
   * @param cols Columns containing the values (or expressions that evaluate to values). The
   *             values do not all need to be of the same data type.
   * @group semi_func
   * @since 0.2.0
   */
  def array_construct_compact(cols: Column*): Column = null

  /**
   * Returns {@code true} if the specified VARIANT is found in the specified ARRAY.
   *
   * @param variant Column containing the VARIANT to find.
   * @param array Column containing the ARRAY to search.
   * @group semi_func
   * @since 0.2.0
   */
  def array_contains(variant: Column, array: Column): Column = null

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
  def array_insert(array: Column, pos: Column, element: Column): Column = null

  /**
   * Returns the index of the first occurrence of an element in an ARRAY.
   *
   * @param variant Column containing the VARIANT value that you want to find. The function
   *                searches for the first occurrence of this value in the array.
   * @param array Column containing the ARRAY to be searched.
   * @group semi_func
   * @since 0.2.0
   */
  def array_position(variant: Column, array: Column): Column = null

  /**
   * Returns an ARRAY containing the new element as well as all elements from the source ARRAY.
   * The new element is positioned at the beginning of the ARRAY.
   *
   * @param array Column containing the source ARRAY.
   * @param element Column containing the element to be prepended.
   * @group semi_func
   * @since 0.2.0
   */
  def array_prepend(array: Column, element: Column): Column = null

  /**
   * Returns the size of the input ARRAY.
   *
   * If the specified column contains a VARIANT value that contains an ARRAY, the size of the ARRAY
   * is returned; otherwise, NULL is returned if the value is not an ARRAY.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def array_size(array: Column): Column = null

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
  def array_slice(array: Column, from: Column, to: Column): Column = null

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
  def array_to_string(array: Column, separator: Column): Column = null

  /**
   * Returns one OBJECT per group. For each (key, value) input pair, where key must be a VARCHAR
   * and value must be a VARIANT, the resulting OBJECT contains a key:value field.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def objectagg(key: Column, value: Column): Column = null

  /**
   * Returns an OBJECT constructed from the arguments.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_construct(key_values: Column*): Column = null

  /**
   * Returns an object containing the contents of the input (i.e.source) object with one or more
   * keys removed.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_delete(obj: Column, key1: Column, keys: Column*): Column = null

  /**
   * Returns an object consisting of the input object with a new key-value pair inserted.
   * The input key must not exist in the object.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_insert(obj: Column, key: Column, value: Column): Column = null

  /**
   * Returns an object consisting of the input object with a new key-value pair inserted (or an
   * existing key updated with a new value).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_insert(obj: Column, key: Column, value: Column, update_flag: Column): Column = null

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
  def object_pick(obj: Column, key1: Column, keys: Column*): Column = null

  /**
   * Casts a VARIANT value to an array.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_array(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a binary string.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_binary(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a string. Does not convert values of other types into string.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_char(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a string. Does not convert values of other types into string.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_varchar(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a date. Does not convert from timestamps.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_date(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_decimal(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_decimal(variant: Column, precision: Int): Column = null

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision and scale.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_decimal(variant: Column, precision: Int, scale: Int): Column = null

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_number(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_number(variant: Column, precision: Int): Column = null

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values),
   * with precision and scale.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_number(variant: Column, precision: Int, scale: Int): Column = null

  /**
   * Casts a VARIANT value to a floating-point value.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_double(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a floating-point value.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_real(variant: Column): Column = null

  /**
   * Casts a VARIANT value to an integer. Does not match non-integer values.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_integer(variant: Column): Column = null

  /**
   * Casts a VARIANT value to an object.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_object(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a time value. Does not convert from timestamps.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_time(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a TIMESTAMP value with local timezone.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_timestamp_ltz(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a TIMESTAMP value with no timezone.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_timestamp_ntz(variant: Column): Column = null

  /**
   * Casts a VARIANT value to a TIMESTAMP value with timezone.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def as_timestamp_tz(variant: Column): Column = null

  /**
   * Tokenizes the given string using the given set of delimiters and returns the tokens as an
   * array. If either parameter is a NULL, a NULL is returned. An empty array is returned if
   * tokenization produces no tokens.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def strtok_to_array(array: Column): Column = null

  /**
   * Tokenizes the given string using the given set of delimiters and returns the tokens as an
   * array. If either parameter is a NULL, a NULL is returned. An empty array is returned if
   * tokenization produces no tokens.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def strtok_to_array(array: Column, delimiter: Column): Column = null

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
  def to_array(col: Column): Column = null

  /**
   * Converts any VARIANT value to a string containing the JSON representation of the value.
   * If the input is NULL, the result is also NULL.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_json(col: Column): Column = null

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
  def to_object(col: Column): Column = null

  /**
   * Converts any value to VARIANT value or NULL (if input is NULL).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_variant(col: Column): Column = null

  /**
   * Converts any VARIANT value to a string containing the XML representation of the value.
   * If the input is NULL, the result is also NULL.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def to_xml(col: Column): Column = null

  /**
   * Extracts a value from an object or array; returns NULL if either of the arguments is NULL.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def get(col1: Column, col2: Column): Column = null

  /**
   * Extracts a field value from an object; returns NULL if either of the arguments is NULL.
   * This function is similar to GET but applies case-insensitive matching to field names.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def get_ignore_case(obj: Column, field: Column): Column = null

  /**
   * Returns an array containing the list of keys in the input object.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def object_keys(obj: Column): Column = null

  /**
   * Extracts an XML element object (often referred to as simply a tag) from a content of outer
   * XML element object by the name of the tag and its instance number (counting from 0).
   *
   * @group semi_func
   * @since 0.2.0
   */
  def xmlget(xml: Column, tag: Column, instance: Column): Column = null

  /**
   * Extracts the first XML element object (often referred to as simply a tag) from a content of
   * outer XML element object by the name of the tag
   *
   * @group semi_func
   * @since 0.2.0
   */
  def xmlget(xml: Column, tag: Column): Column = null

  /**
   * Extracts a value from semi-structured data using a path name.
   *
   * @group semi_func
   * @since 0.2.0
   */
  def get_path(col: Column, path: Column): Column = null

//  /**
//   * Works like a cascading if-then-else statement.
//   * A series of conditions are evaluated in sequence.
//   * When a condition evaluates to TRUE, the evaluation stops and the associated
//   * result (after THEN) is returned. If none of the conditions evaluate to TRUE,
//   * then the result after the optional OTHERWISE is returned, if present;
//   * otherwise NULL is returned.
//   * For Example:
//   * {{{
//   *     import functions._
//   *     df.select(
//   *       when(col("col").is_null, lit(1))
//   *         .when(col("col") === 1, lit(2))
//   *         .otherwise(lit(3))
//   *     )
//   * }}}
//   *
//   * @group con_func
//   * @since 0.2.0
//   */
//  def when(condition: Column, value: Column): CaseExpr =
//    null
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
    null

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
    null

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
  def in(columns: Seq[Column], df: DataFrame): Column = null
  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 1 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq1(): Column = null

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
    null

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 2 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq2(): Column = null

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
    null

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 4 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq4(): Column = null

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
    null

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around.
   * Wrap-around occurs after the largest representable integer of the integer width
   * 8 byte. the sequence continues at 0 after wrap-around.
   *
   * @since 0.11.0
   * @group gen_func
   */
  def seq8(): Column = null

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
    null

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
    null

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
    null

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
    null

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
  def listagg(col: Column): Column = null

  /**

   * Wrapper for Snowflake built-in reverse function. Gets the reversed string.
   * Reverses the order of characters in a string, or of bytes in a binary value.
   * The returned value is the same length as the input, but with the characters/bytes
   *  in reverse order. If subject is NULL, the result is also NULL.
   * Example: SELECT REVERSE('Hello, world!');
   *+--------------------------+
   *| REVERSE('HELLO, WORLD!') |
   *|--------------------------|
   *| !dlrow ,olleH            |
   *+--------------------------+

   * @since 1.14.0
   * @param c Column to be reverse.
   * @return Column object.
   */
  def reverse(c: Column): Column = null

  /**
   * Wrapper for Snowflake built-in isnull function. Gets a boolean
   * depending if value is NULL or not.
   * Return true if the value in the column is null.
   *Example::
   * >>> from snowflake.snowpark.functions import is_null
   * >>> df = session.create_dataframe([1.2, float("nan"), None, 1.0],
   *  schema=["a"])
   * >>> df.select(is_null("a").as_("a")).collect()
   * [Row(A=False), Row(A=False), Row(A=True), Row(A=False)]
   * @since 1.14.0
   * @param c Column to qnalize if it is null value.
   * @return Column object.
   */
  def isnull(c: Column): Column = null

  /**
   * Returns the current Unix timestamp (in seconds) as a long.
   * Extracts a specified date or time portion from a date, time, or timestamp.
   * how:
   * EXTRACT , HOUR / MINUTE / SECOND , YEAR* / DAY* / WEEK* / MONTH / QUARTER
   * Construction - DATE_PART( <date_or_time_part> , <date_or_time_expr> )
   * SELECT TO_TIMESTAMP('2013-05-08T23:39:20.123-07:00') AS "TIME_STAMP1",
   *  DATE_PART(EPOCH_SECOND, "TIME_STAMP1") AS "EXTRACTED EPOCH SECOND";
   * +-------------------------+------------------------+
   * | TIME_STAMP1             | EXTRACTED EPOCH SECOND |
   * |-------------------------+------------------------|
   * | 2013-05-08 23:39:20.123 |             1368056360 |
   * +-------------------------+------------------------+
   * @since 1.14.0
   * @note All calls of `unix_timestamp` within the same query return the same value
   */
  def unix_timestamp(c: Column): Column = null
  /**


   * Signature - snowflake.snowpark.functions.regexp_extract
   * (value: Union[Column, str], regexp: Union[Column, str], idx: int)
   *   Column
   * Extract a specific group matched by a regex, from the specified string
   * column. If the regex did not match, or the specified group did not match,
   * an empty string is returned.
   * <pr>Example:
   * from snowflake.snowpark.functions import regexp_extract
   * df = session.createDataFrame([["id_20_30", 10], ["id_40_50", 30]],
   *  ["id", "age"])
   * df.select(regexp_extract("id", r"(\d+)", 1).alias("RES")).show()
   *</pr>
   *<pr>
   *     ---------
   *     |"RES"  |
   *     ---------
   *     |20     |
   *     |40     |
   *     ---------
   *</pr>
   * Note: non-greedy tokens such as  are not supported
   * @since 1.14.0
   * @return Column object.
   */
  def regexp_extract(
                      colName: Column,
                      exp: String,
                      position: Int,
                      Occurences: Int,
                      grpIdx: Int): Column = null

  /**
   *    Returns the sign of its argument as mentioned :
   *
   *     - -1 if the argument is negative.
   *     - 1 if it is positive.
   *     - 0 if it is 0.
   *
   * Args:
   *     col: The column to evaluate its sign
   *<pr>
   * Example::
   *     >>> df = session.create_dataframe([(-2, 2, 0)], ["a", "b", "c"])
   *     >>> df.select(sign("a").alias("a_sign"), sign("b").alias("b_sign"),
   * sign("c").alias("c_sign")).show()
   *     ----------------------------------
   *     |"A_SIGN"  |"B_SIGN"  |"C_SIGN"  |
   *     ----------------------------------
   *     |-1        |1         |0         |
   *     ----------------------------------
   * </pr>
   * @since 1.14.0
   * @param e Column to calculate the sign.
   * @return Column object.
   */
  def sign(colName: Column): Column = null

  /**
   *    Returns the sign of its argument:
   *
   *     - -1 if the argument is negative.
   *     - 1 if it is positive.
   *     - 0 if it is 0.
   *
   * Args:
   *     col: The column to evaluate its sign
   *<pr>
   * Example::
   *     >>> df = session.create_dataframe([(-2, 2, 0)], ["a", "b", "c"])
   *     >>> df.select(sign("a").alias("a_sign"), sign("b").alias("b_sign"),
   * sign("c").alias("c_sign")).show()
   *     ----------------------------------
   *     |"A_SIGN"  |"B_SIGN"  |"C_SIGN"  |
   *     ----------------------------------
   *     |-1        |1         |0         |
   *     ----------------------------------
   * </pr>
   * @since 1.14.0
   * @param e Column to calculate the sign.
   * @return Column object.
   */
  def signum(colName: Column): Column = null

  /**
   * Returns the sign of the given column. Returns either 1 for positive,
   *  0 for 0 or
   * NaN, -1 for negative and null for null.
   * NOTE: if string values are provided snowflake will attempts to cast.
   *  If it casts correctly, returns the calculation,
   *  if not an error will be thrown
   * @since 1.14.0
   * @param columnName Name of the column to calculate the sign.
   * @return Column object.
   */
  def signum(columnName: String): Column = null
  /**
   * Returns the substring from string str before count occurrences
   * of the delimiter delim. If count is positive,
   * everything the left of the final delimiter (counting from left)
   *  is returned. If count is negative, every to the right of the
   * final delimiter (counting from the right) is returned.
   * substring_index performs a case-sensitive match when searching for delim.
   *   @since 1.14.0
   */
  def substring_index(str: String, delim: String, count: Int): Column = null

  /**
   *
   * Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
   * ARRAY is returned.
   *<pr>
   * Example::
   *     >>> df = session.create_dataframe([[1], [2], [3], [1]], schema=["a"])
   *     >>> df.select(array_agg("a", True).alias("result")).show()
   *     ------------
   *     |"RESULT"  |
   *     ------------
   *     |[         |
   *     |  1,      |
   *     |  2,      |
   *     |  3       |
   *     |]         |
   *     ------------
   * </pr>
   * @since 1.14.0
   * @param c Column to be collect.
   * @return The array.
   */
  def collect_list(c: Column): Column = null

  /**
   *
   * Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
   * ARRAY is returned.
   *
   * Example::
   *     >>> df = session.create_dataframe([[1], [2], [3], [1]], schema=["a"])
   *     >>> df.select(array_agg("a", True).alias("result")).show()
   *     ------------
   *     |"RESULT"  |
   *     ------------
   *     |[         |
   *     |  1,      |
   *     |  2,      |
   *     |  3       |
   *     |]         |
   *     ------------
   * @since 1.14.0
   * @param s Column name to be collected.
   * @return The array.
   */
  def collect_list(s: String): Column = null

  /**
   *
   *  Returns the date that is `days` days after `start`
   *  Usage - DATE_ADD( date_or_time_part, value, date_or_time_expr )
   * Example::
   *     SELECT TO_DATE('2013-05-08') AS v1, DATE_ADD(year, 2, TO_DATE('2013-05-08')) AS v;
   * +------------+------------+
   * | V1         | V          |
   * |------------+------------|
   * | 2013-05-08 | 2015-05-08 |
   * +------------+------------+
   *
   * @since 1.15.0
   * @param start Column name
   * @param days Int            .
   * @return Column.
   */
  def date_add(days: Int, start: Column): Column = null

  /**
   * Returns the date that is `days` days after `start`
   *   Usage - DATE_ADD( date_or_time_part, value, date_or_time_expr )
   *  Example::
   *      SELECT TO_DATE('2013-05-08') AS v1, DATE_ADD(year, 2, TO_DATE('2013-05-08')) AS v;
   *  +------------+------------+
   *  | V1         | V          |
   *  |------------+------------|
   *  | 2013-05-08 | 2015-05-08 |
   *  +------------+------------+
   *
   * @since 1.15.0
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param days  The number of days to add to `start`, can be negative to subtract days
   * @return A date, or null if `start` was a string that could not be cast to a date
   */
  def date_add(start: Column, days: Column): Column = null

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *  Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
   *  ARRAY is returned.
   *
   *  Example::
   *  >>> df = session.create_dataframe([[1], [2], [3], [1]], schema=["a"])
   *  >>> df.select(array_agg("a", True).alias("result")).show()
   *  ------------
   *  |"RESULT"  |
   *  ------------
   *  |[         |
   *  |  1,      |
   *  |  2,      |
   *  |  3       |
   *  |]         |
   *  ------------
   * @since 1.15.0
   * @param e The column to collect the list values
   * @return A list with unique values
   */
  def collect_set(e: Column): Column = null

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   * Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
   * ARRAY is returned.
   *
   * Example::
   * >>> df = session.create_dataframe([[1], [2], [3], [1]], schema=["a"])
   * >>> df.select(array_agg("a", True).alias("result")).show()
   * ------------
   * |"RESULT"  |
   * ------------
   * |[         |
   * |  1,      |
   * |  2,      |
   * |  3       |
   * |]         |
   * ------------
   * @since 1.15.0
   * @param e The column to collect the list values
   * @return A list with unique values
   */
  def collect_set(e: String): Column = null

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the
   * yyyy-MM-dd HH:mm:ss format.
   * @since 1.15.0
   * @param ut A number of a type that is castable to a long, such as string or integer. Can be
   *           negative for timestamps before the unix epoch
   * @return A string, or null if the input was a string that could not be cast to a long
   */
  def from_unixtime(ut: Column): Column =
    null

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   * @since 1.15.0
   * @param ut A number of a type that is castable to a long, such as string or integer. Can be
   *           negative for timestamps before the unix epoch
   * @param f  A date time pattern that the input will be formatted to
   * @return A string, or null if `ut` was a string that could not be cast to a long or `f` was
   *         an invalid date time pattern
   */
  def from_unixtime(ut: Column, f: String): Column =
    null

  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   * Returns a sequence of monotonically increasing integers, with wrap-around
   * which happens after largest representable integer of integer width 8 byte.
   *
   * Args:
   * sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
   * continues at smallest representable 8 byte integer. Defaults to 0.
   *
   * See Also:
   *         - :meth:`Session.generator`, which can be used to generate in tandem with `seq8` to
   *           generate sequences.
   *
   * Example::
   * >>> df = session.generator(seq8(0), rowcount=3)
   * >>> df.collect()
   * [Row(SEQ8(0)=0), Row(SEQ8(0)=1), Row(SEQ8(0)=2)]
   * @since 1.15.0
   */
  def monotonically_increasing_id(): Column = null

  /**
   * Returns number of months between dates `start` and `end`.
   *
   * A whole number is returned if both inputs have the same day of month or both are the last day
   * of their respective months. Otherwise, the difference is calculated assuming 31 days per month.
   *
   * For example:
   * {{{
   * months_between("2017-11-14", "2017-07-14")  // returns 4.0
   * months_between("2017-01-01", "2017-01-10")  // returns 0.29032258
   * months_between("2017-06-01", "2017-06-16 12:00:00")  // returns -0.5
   * }}}
   * @since 1.15.0
   * @param end  Column name. If a string, the data must be in a format that can
   *              be cast to a timestamp, such as yyyy-MM-dd
   *              or yyyy-MM-dd HH:mm:ss.SSSS
   * @param start  Column name . If a string, the data must be in a format that can
   *              cast to a timestamp, such as yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.SSSS
   * @return A double, or null if either end or start were strings that could not be cast to a
   *         timestamp. Negative if end is before start
   */
  def months_between(end: String, start: String): Column =
    null

  /**
   * Locate the position of the first occurrence of substr column in the given string.
   * Returns null if either of the arguments are null.
   * For example
   * SELECT id,
   *        string1,
   *         REGEXP_SUBSTR(string1, 'nevermore\\d') AS substring,
   *        REGEXP_INSTR( string1, 'nevermore\\d') AS position
   *    FROM demo1
   *    ORDER BY id;
   * +----+-------------------------------------+------------+----------+
   *  | ID | STRING1                             | SUBSTRING  | POSITION |
   *  |----+-------------------------------------+------------+----------|
   *  |  1 | nevermore1, nevermore2, nevermore3. | nevermore1 |        1 |
   *  +----+-------------------------------------+------------+----------+
   *
   * @since 1.15.0
   * @note The position is not zero based, but 1 based index. Returns 0 if substr
   * could not be found in str.
   */
  def instr(str: Column, substring: String): Column = null

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield
   * '2017-07-14 03:40:00.0'.
   * ALTER SESSION SET TIMEZONE = 'America/Los_Angeles';
   *  SELECT TO_TIMESTAMP_TZ('2024-04-05 01:02:03');
   *   +----------------------------------------+
   *  | TO_TIMESTAMP_TZ('2024-04-05 01:02:03') |
   *  |----------------------------------------|
   *  | 2024-04-05 01:02:03.000 -0700          |
   *  +----------------------------------------+
   *
   * @since 1.15.0
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   *           A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   */
  def from_utc_timestamp(ts: Column): Column =
    null

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time
   * zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield
   * '2017-07-14 01:40:00.0'.
   * @since 1.15.0
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   *           A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   */
  def to_utc_timestamp(ts: Column): Column = null

  /**
   * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places
   * with HALF_EVEN round mode, and returns the result as a string column.
   * @since 1.15.0
   * If d is 0, the result has no decimal point or fractional part.
   * If d is less than 0, the result will be null.
   *
   * @param x numeric column to be transformed
   * @param d Amount of decimal for the number format
   *
   * @return Number casted to the specific string format
   */
  def format_number(x: Column, d: Int): Column = null

  /** Returns a Column expression with values sorted in descending order.
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq(1, 2, 3)).toDF("id")
   *   df.sort(desc("id")).show()
   *
   * --------
   * |"ID"  |
   * --------
   * |3     |
   * |2     |
   * |1     |
   * --------
   * }}}
   *
   * @since 1.14.0
   * @param colName Column name.
   * @return Column object ordered in a descending manner.
   */
  def desc(colName: String): Column = null

  /**
   * Returns a Column expression with values sorted in ascending order.
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq(3, 2, 1)).toDF("id")
   *   df.sort(asc("id")).show()
   *
   * --------
   * |"ID"  |
   * --------
   * |1     |
   * |2     |
   * |3     |
   * --------
   * }}}
   * @since 1.14.0
   * @param colName Column name.
   * @return Column object ordered in an ascending manner.
   */
  def asc(colName: String): Column = null

  /**
   * Returns the size of the input ARRAY.
   *
   * If the specified column contains a VARIANT value that contains an ARRAY, the size of the ARRAY
   * is returned; otherwise, NULL is returned if the value is not an ARRAY.
   *
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq(Array(1, 2, 3))).toDF("id")
   *   df.select(size(col("id"))).show()
   *
   * ------------------------
   * |"ARRAY_SIZE(""ID"")"  |
   * ------------------------
   * |3                     |
   * ------------------------
   * }}}
   *
   * @since 1.14.0
   * @param c Column to get the size.
   * @return Size of array column.
   */
  def size(c: Column): Column = null

  /**
   * Creates a [[Column]] expression from raw SQL text.
   *
   * Note that the function does not interpret or check the SQL text.
   *
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq(Array(1, 2, 3))).toDF("id")
   *   df.filter(expr("id > 2")).show()
   *
   *  --------
   *  |"ID"  |
   *  --------
   *  |3     |
   *  --------
   * }}}
   *
   * @since 1.14.0
   * @param s SQL Expression as text.
   * @return Converted SQL Expression.
   */
  def expr(s: String): Column = null

  /**
   * Returns an ARRAY constructed from zero, one, or more inputs.
   *
   * Example:
   * {{{
   *   val df = session.createDataFrame(Seq((1, 2, 3), (4, 5, 6))).toDF("id")
   *   df.select(array(col("a"), col("b")).as("id")).show()
   *
   *  --------
   * |"ID"  |
   * --------
   * |[     |
   * |  1,  |
   * |  2   |
   * |]     |
   * |[     |
   * |  4,  |
   * |  5   |
   * |]     |
   * --------
   * }}}
   *
   * @since 1.14.0
   * @param c Columns to build the array.
   * @return The array.
   */
  def array(c: Column*): Column = null

  /**
   * Converts an input expression into the corresponding date in the specified date format.
   * Example:
   * {{{
   *  val df = Seq("2023-10-10", "2022-05-15", null.asInstanceOf[String]).toDF("date")
   *  df.select(date_format(col("date"), "YYYY/MM/DD").as("formatted_date")).show()
   *
   * --------------------
   * |"FORMATTED_DATE"  |
   * --------------------
   * |2023/10/10        |
   * |2022/05/15        |
   * |NULL              |
   * --------------------
   *
   * }}}
   *
   * @since 1.14.0
   * @param c Column to format to date.
   * @param s Date format.
   * @return Column object.
   */
  def date_format(c: Column, s: String): Column = null

  /**
   * Returns the last value of the column in a group.
   * Example
   * {{{
   *  val df = session.createDataFrame(Seq((5, "a", 10),
   *                                       (5, "b", 20),
   *                                       (3, "d", 15),
   *                                       (3, "e", 40))).toDF("grade", "name", "score")
   *     val window = Window.partitionBy(col("grade")).orderBy(col("score").desc)
   *     df.select(last(col("name")).over(window)).show()
   *
   * ---------------------
   * |"LAST_SCORE_NAME"  |
   * ---------------------
   * |a                  |
   * |a                  |
   * |d                  |
   * |d                  |
   * ---------------------
   * }}}
   *
   * @since 1.14.0
   * @param c Column to obtain last value.
   * @return Column object.
   */
  def last(c: Column): Column = null

  /**
   * Computes the logarithm of the given value in base 10.
   * Example
   * {{{
   *  val df = session.createDataFrame(Seq(100)).toDF("a")
   *  df.select(log10(col("a"))).show()
   *
   * -----------
   * |"LOG10"  |
   * -----------
   * |2.0      |
   * -----------
   * }}}
   *
   * @since 1.14.0
   * @param c Column to apply logarithm operation
   * @return log10 of the given column
   */
  def log10(c: Column): Column = null

  /**
   * Computes the logarithm of the given column in base 10.
   * Example
   * {{{
   *  val df = session.createDataFrame(Seq(100)).toDF("a")
   *  df.select(log10("a"))).show()
   * -----------
   * |"LOG10"  |
   * -----------
   * |2.0      |
   * -----------
   *
   * }}}
   *
   * @since 1.14.0
   * @param columnName ColumnName in String to apply logarithm operation
   * @return log10 of the given column
   */
  def log10(columnName: String): Column = null

  /**
   * Computes the natural logarithm of the given value plus one.
   *Example
   * {{{
   *  val df = session.createDataFrame(Seq(0.1)).toDF("a")
   *  df.select(log1p(col("a")).as("log1p")).show()
   * -----------------------
   * |"LOG1P"              |
   * -----------------------
   * |0.09531017980432493  |
   * -----------------------
   *
   * }}}
   *
   * @since 1.14.0
   * @param c Column to apply logarithm operation
   * @return the natural logarithm of the given value plus one.
   */
  def log1p(c: Column): Column = null

  /**
   * Computes the natural logarithm of the given value plus one.
   *Example
   * {{{
   *  val df = session.createDataFrame(Seq(0.1)).toDF("a")
   *  df.select(log1p("a").as("log1p")).show()
   * -----------------------
   * |"LOG1P"              |
   * -----------------------
   * |0.09531017980432493  |
   * -----------------------
   *
   * }}}
   *
   * @since 1.14.0
   * @param columnName ColumnName in String to apply logarithm operation
   * @return the natural logarithm of the given value plus one.
   */
  def log1p(columnName: String): Column = null

  /**
   * Computes the BASE64 encoding of a column and returns it as a string column.
   * This is the reverse of unbase64.
   *Example
   * {{{
   *  val df = session.createDataFrame(Seq("test")).toDF("a")
   *  df.select(base64(col("a")).as("base64")).show()
   * ------------
   * |"BASE64"  |
   * ------------
   * |dGVzdA==  |
   * ------------
   *
   * }}}
   *
   * @since 1.14.0
   * @param columnName ColumnName to apply base64 operation
   * @return base64 encoded value of the given input column.
   */
  def base64(col: Column): Column = null

  /**
   * Decodes a BASE64 encoded string column and returns it as a column.
   *Example
   * {{{
   *  val df = session.createDataFrame(Seq("dGVzdA==")).toDF("a")
   *  df.select(unbase64(col("a")).as("unbase64")).show()
   * --------------
   * |"UNBASE64"  |
   * --------------
   * |test        |
   * --------------
   *
   * }}}
   *
   * @since 1.14.0
   * @param columnName ColumnName to apply unbase64 operation
   * @return the decoded value of the given encoded value.
   */
  def unbase64(col: Column): Column = null

  /**

   *
   * Locate the position of the first occurrence of substr in a string column, after position pos.
   *
   * @note The position is not zero based, but 1 based index. returns 0 if substr
   * could not be found in str. This function is just leverages the SF POSITION builtin
   *Example
   * {{{
   *  val df = session.createDataFrame(Seq(("b", "abcd"))).toDF("a", "b")
   *  df.select(locate(col("a"), col("b"), 1).as("locate")).show()
   * ------------
   * |"LOCATE"  |
   * ------------
   * |2         |
   * ------------
   *
   * }}}
   * @since 1.14.0
   * @param substr string to search
   * @param str value where string will be searched
   * @param pos index for starting the search
   * @return returns the position of the first occurrence.
   */
  def locate(substr: Column, str: Column, pos: Int): Column =
    null

  /**
   * Locate the position of the first occurrence of substr in a string column, after position pos.
   *
   * @note The position is not zero based, but 1 based index. returns 0 if substr
   * could not be found in str. This function is just leverages the SF POSITION builtin
   * Example
   * {{{
   *  val df = session.createDataFrame(Seq("java scala python")).toDF("a")
   *  df.select(locate("scala", col("a")).as("locate")).show()
   * ------------
   * |"LOCATE"  |
   * ------------
   * |6         |
   * ------------
   *
   * }}}
   * @since 1.14.0
   * @param substr string to search
   * @param str value where string will be searched
   * @param pos index for starting the search. default to 1.
   * @return Returns the position of the first occurrence
   */
  def locate(substr: String, str: Column, pos: Int = 1): Column =
    null

  /**
   * Window function: returns the ntile group id (from 1 to `n` inclusive) in an ordered window
   * partition. For example, if `n` is 4, the first quarter of the rows will get value 1, the second
   * quarter will get 2, the third quarter will get 3, and the last quarter will get 4.
   *
   * This is equivalent to the NTILE function in SQL.
   * Example
   * {{{
   *   val df = Seq((5, 15), (5, 15), (5, 15), (5, 20)).toDF("grade", "score")
   *   val window = Window.partitionBy(col("grade")).orderBy(col("score"))
   *   df.select(ntile(2).over(window).as("ntile")).show()
   * -----------
   * |"NTILE"  |
   * -----------
   * |1        |
   * |1        |
   * |2        |
   * |2        |
   * -----------
   * }}}
   *
   * @since 1.14.0
   * @param n number of groups
   * @return returns the ntile group id (from 1 to n inclusive) in an ordered window partition.
   */
  def ntile(n: Int): Column = null

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the standard normal distribution.
   * Return a call to the Snowflake RANDOM function.
   * NOTE: Snowflake returns integers of 17-19 digits.
   * Example
   * {{{
   *   val df = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
   *   df.withColumn("randn", randn()).select("randn").show()
   * ------------------------
   * |"RANDN"               |
   * ------------------------
   * |-2093909082984812541  |
   * |-1379817492278593383  |
   * |-1231198046297539927  |
   * ------------------------
   * }}}
   *
   * @since 1.14.0
   * @return Random number.
   */
  def randn(): Column =
    null

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the standard normal distribution.
   * Calls to the Snowflake RANDOM function.
   * NOTE: Snowflake returns integers of 17-19 digits.
   * Example
   * {{{
   *   val df = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
   *   df.withColumn("randn_with_seed", randn(123L)).select("randn_with_seed").show()
   * ------------------------
   * |"RANDN_WITH_SEED"     |
   * ------------------------
   * |5777523539921853504   |
   * |-8190739547906189845  |
   * |-1138438814981368515  |
   * ------------------------
   * }}}
   *
   * @since 1.14.0
   * @param seed Seed to use in the random function.
   * @return Random number.
   */
  def randn(seed: Long): Column =
    null

  /**
   * Shift the given value numBits left. If the given value is a long value,
   * this function will return a long value else it will return an integer value.
   * Example
   * {{{
   *   val df = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
   *   df.select(shiftleft(col("A"), 1).as("shiftleft")).show()
   * ---------------
   * |"SHIFTLEFT"  |
   * ---------------
   * |2            |
   * |4            |
   * |6            |
   * ---------------
   * }}}
   *
   * @since 1.14.0
   * @param c Column to modify.
   * @param numBits Number of bits to shift.
   * @return Column object.
   */
  def shiftleft(c: Column, numBits: Int): Column =
    null

  /**
   * Shift the given value numBits right. If the given value is a long value,
   * it will return a long value else it will return an integer value.
   * Example
   * {{{
   *   val df = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
   *   df.select(shiftright(col("A"), 1).as("shiftright")).show()
   * ----------------
   * |"SHIFTRIGHT"  |
   * ----------------
   * |0             |
   * |1             |
   * |1             |
   * ----------------
   * }}}
   *
   * @since 1.14.0
   * @param c Column to modify.
   * @param numBits Number of bits to shift.
   * @return Column object.
   */
  def shiftright(c: Column, numBits: Int): Column =
    null
  /**
   * Computes hex value of the given column.
   * Example
   * {{{
   *   val df = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
   *   df.withColumn("hex_col", hex(col("A"))).select("hex_col").show()
   * -------------
   * |"HEX_COL"  |
   * -------------
   * |31         |
   * |32         |
   * |33         |
   * -------------
   * }}}
   *
   * @since 1.14.0
   * @param c Column to encode.
   * @return Encoded string.
   */
  def hex(c: Column): Column =
    null

  /**
   * Inverse of hex. Interprets each pair of characters as a hexadecimal number
   * and converts to the byte representation of number.
   * Example
   * {{{
   *   val df = session.createDataFrame(Seq((31), (32), (33))).toDF("a")
   *   df.withColumn("unhex_col", unhex(col("A"))).select("unhex_col").show()
   * ---------------
   * |"UNHEX_COL"  |
   * ---------------
   * |1            |
   * |2            |
   * |3            |
   * ---------------
   * }}}
   *
   * @param c Column to encode.
   * @since 1.14.0
   * @return Encoded string.
   */
  def unhex(c: Column): Column =
    null

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
    null



  /**
   * Calls a user-defined function (UDF) by name.
   *
   * @group udf_func
   * @since 0.1.0
   */
  def callUDF(udfName: String, cols: Any*): Column = null

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
        |def udf[$typeTags](func: Function$x[$types]): UserDefinedFunction = udf("udf") {
        |  registerUdf(_toUdf(func))
        |}""".stripMargin)
    }
   */

  // todo: generated UDF from SBT


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

    def apply(args: Any*): Column = null
  }

 }
