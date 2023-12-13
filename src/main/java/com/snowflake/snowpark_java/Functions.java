package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.udf.*;
import java.util.List;

/**
 * Provides utility functions that generate Column expression that you can pass to DataFrame
 * transformation methods. These functions generate references to columns, literals, and SQL
 * expression.
 *
 * <p>This object also provides functions that correspond to Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference-functions.html">system-defined functions</a>
 * (built-in functions), including functions for aggregation and window functions.
 *
 * @since 0.9.0
 */
public final class Functions {
  // lock constructor
  private Functions() {}

  /**
   * Creates a Column with the specified name.
   *
   * @since 0.9.0
   * @param name The column name
   * @return The result column
   */
  public static Column col(String name) {
    return new Column(com.snowflake.snowpark.functions.col(name));
  }

  /**
   * Generates a Column representing the result of the input DataFrame. The parameter 'df' should
   * have one column and must produce one row. It is an alias of {@code toScalar} function.
   *
   * <p>For example:
   *
   * <pre>{@code
   * DataFrame df1 = session.sql("select * from values(1,1,1),(2,2,3) as T(c1, c2, c3)");
   * DataFrame df2 = session.sql("select * from values(2) as T(a)");
   * df1.select(Functions.col("c1"), Functions.col(df2)).show();
   * }</pre>
   *
   * @since 0.9.0
   * @param df The sub-query Dataframe
   * @return The result column
   */
  public static Column col(DataFrame df) {
    return new Column(com.snowflake.snowpark.functions.col(df.getScalaDataFrame()));
  }

  /**
   * Generates a Column representing the result of the input DataFrame. The parameter 'df' should
   * have one column and must produce one row.
   *
   * <p>For example:
   *
   * <pre>{@code
   * DataFrame df1 = session.sql("select * from values(1,1,1),(2,2,3) as T(c1, c2, c3)");
   * DataFrame df2 = session.sql("select * from values(2) as T(a)");
   * df1.select(Functions.col("c1"), Functions.toScalar(df2)).show();
   * }</pre>
   *
   * @since 0.9.0
   * @param df The sub-query Dataframe
   * @return The result column
   */
  public static Column toScalar(DataFrame df) {
    return new Column(com.snowflake.snowpark.functions.toScalar(df.getScalaDataFrame()));
  }

  /**
   * Creates a Column expression for a literal value.
   *
   * @since 0.9.0
   * @param literal The literal value
   * @return The result column
   */
  public static Column lit(Object literal) {
    return new Column(com.snowflake.snowpark.functions.lit(JavaUtils.toScala(literal)));
  }

  /**
   * Accesses data in a subsequent row in the same result set without having to join the table to
   * itself.
   *
   * @since 0.9.0
   * @param col The input column
   * @param offset The function offset
   * @param defaultValue The default value of lead function
   * @return The result column
   */
  public static Column lead(Column col, int offset, Column defaultValue) {
    return new Column(
        com.snowflake.snowpark.functions.lead(
            col.toScalaColumn(), offset, defaultValue.toScalaColumn()));
  }

  /**
   * Accesses data in a subsequent row in the same result set without having to join the table to
   * itself.
   *
   * @since 0.9.0
   * @param col The input column
   * @param offset The function offset
   * @return The result column
   */
  public static Column lead(Column col, int offset) {
    return new Column(com.snowflake.snowpark.functions.lead(col.toScalaColumn(), offset));
  }

  /**
   * Accesses data in a subsequent row in the same result set without having to join the table to
   * itself.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column lead(Column col) {
    return new Column(com.snowflake.snowpark.functions.lead(col.toScalaColumn()));
  }

  /**
   * Creates a Column expression from row SQL text.
   *
   * <p>Note that the function does not interpret or check the SQL text.
   *
   * @since 0.9.0
   * @param sqlText The SQL query
   * @return The result column
   */
  public static Column sqlExpr(String sqlText) {
    return new Column(com.snowflake.snowpark.functions.sqlExpr(sqlText));
  }

  /**
   * Uses HyperLogLog to return an approximation of the distinct cardinality of the input (i.e.
   * returns an approximation of `COUNT(DISTINCT col)`).
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column approx_count_distinct(Column col) {
    return new Column(com.snowflake.snowpark.functions.approx_count_distinct(col.toScalaColumn()));
  }

  /**
   * Returns the average of non-NULL records. If all records inside a group are NULL, the function
   * returns NULL.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column avg(Column col) {
    return new Column(com.snowflake.snowpark.functions.avg(col.toScalaColumn()));
  }

  /**
   * Returns the correlation coefficient for non-null pairs in a group.
   *
   * @since 0.9.0
   * @param col1 The first input column
   * @param col2 The second input column
   * @return The result column
   */
  public static Column corr(Column col1, Column col2) {
    return new Column(
        com.snowflake.snowpark.functions.corr(col1.toScalaColumn(), col2.toScalaColumn()));
  }

  /**
   * Returns either the number of non-NULL records for the specified columns, or the total number of
   * records.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column count(Column col) {
    return new Column(com.snowflake.snowpark.functions.count(col.toScalaColumn()));
  }

  /**
   * Returns either the number of non-NULL distinct records for the specified columns, or the total
   * number of the distinct records.
   *
   * @since 0.9.0
   * @param first The first column
   * @param remaining A column list except the first column
   * @return The result column
   */
  public static Column count_distinct(Column first, Column... remaining) {
    return new Column(
        com.snowflake.snowpark.functions.count_distinct(
            first.toScalaColumn(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(remaining))));
  }

  /**
   * Calculates the population covariance for non-null pairs in a group.
   *
   * @since 0.9.0
   * @param col1 The first column
   * @param col2 The second column
   * @return The result column
   */
  public static Column covar_pop(Column col1, Column col2) {
    return new Column(
        com.snowflake.snowpark.functions.covar_pop(col1.toScalaColumn(), col2.toScalaColumn()));
  }

  /**
   * Calculates the sample covariance for non-null pairs in a group.
   *
   * @since 0.9.0
   * @param col1 The first column
   * @param col2 The second column
   * @return The result column
   */
  public static Column covar_samp(Column col1, Column col2) {
    return new Column(
        com.snowflake.snowpark.functions.covar_samp(col1.toScalaColumn(), col2.toScalaColumn()));
  }

  /**
   * Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column grouping(Column col) {
    return new Column(com.snowflake.snowpark.functions.grouping(col.toScalaColumn()));
  }

  /**
   * Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.
   *
   * @since 0.9.0
   * @param cols A list of input column
   * @return The result column
   */
  public static Column grouping_id(Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.grouping_id(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns the population excess kurtosis of non-NULL records. If all records inside a group are
   * NULL, the function returns NULL.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column kurtosis(Column col) {
    return new Column(com.snowflake.snowpark.functions.kurtosis(col.toScalaColumn()));
  }

  /**
   * Returns the maximum value for the records in a group. NULL values are ignored unless all the
   * records are NULL, in which case a NULL value is returned.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column max(Column col) {
    return new Column(com.snowflake.snowpark.functions.max(col.toScalaColumn()));
  }

  /**
   * Returns the minimum value for the records in a group. NULL values are ignored unless all the
   * records are NULL, in which case a NULL value is returned.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column min(Column col) {
    return new Column(com.snowflake.snowpark.functions.min(col.toScalaColumn()));
  }

  /**
   * Returns the average of non-NULL records. If all records inside a group are NULL, the function
   * returns NULL. Alias of avg
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column mean(Column col) {
    return new Column(com.snowflake.snowpark.functions.mean(col.toScalaColumn()));
  }

  /**
   * Returns the median value for the records in a group. NULL values are ignored unless all the
   * records are NULL, in which case a NULL value is returned.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column median(Column col) {
    return new Column(com.snowflake.snowpark.functions.median(col.toScalaColumn()));
  }

  /**
   * Returns the sample skewness of non-NULL records. If all records inside a group are NULL, the
   * function returns NULL.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column skew(Column col) {
    return new Column(com.snowflake.snowpark.functions.skew(col.toScalaColumn()));
  }

  /**
   * Returns the sample standard deviation (square root of sample variance) of non-NULL values. If
   * all records inside a group are NULL, returns NULL.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column stddev(Column col) {
    return new Column(com.snowflake.snowpark.functions.stddev(col.toScalaColumn()));
  }

  /**
   * Returns the sample standard deviation (square root of sample variance) of non-NULL values. If
   * all records inside a group are NULL, returns NULL. Alias of stddev
   *
   * @param col The input Column
   * @return The result Column
   * @since 0.9.0
   */
  public static Column stddev_samp(Column col) {
    return new Column(com.snowflake.snowpark.functions.stddev_samp(col.toScalaColumn()));
  }

  /**
   * Returns the population standard deviation (square root of variance) of non-NULL values. If all
   * records inside a group are NULL, returns NULL.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column stddev_pop(Column col) {
    return new Column(com.snowflake.snowpark.functions.stddev_pop(col.toScalaColumn()));
  }

  /**
   * Returns the sum of non-NULL records in a group. You can use the DISTINCT keyword to compute the
   * sum of unique non-null values. If all records inside a group are NULL, the function returns
   * NULL.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column sum(Column col) {
    return new Column(com.snowflake.snowpark.functions.sum(col.toScalaColumn()));
  }

  /**
   * Returns the sum of non-NULL distinct records in a group. You can use the DISTINCT keyword to
   * compute the sum of unique non-null values. If all records inside a group are NULL, the function
   * returns NULL.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column sum_distinct(Column col) {
    return new Column(com.snowflake.snowpark.functions.sum_distinct(col.toScalaColumn()));
  }

  /**
   * Returns the sample variance of non-NULL records in a group. If all records inside a group are
   * NULL, a NULL is returned.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column variance(Column col) {
    return new Column(com.snowflake.snowpark.functions.variance(col.toScalaColumn()));
  }

  /**
   * Returns the sample variance of non-NULL records in a group. If all records inside a group are
   * NULL, a NULL is returned. Alias of var_samp
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column var_samp(Column col) {
    return new Column(com.snowflake.snowpark.functions.var_samp(col.toScalaColumn()));
  }

  /**
   * Returns the population variance of non-NULL records in a group. If all records inside a group
   * are NULL, a NULL is returned.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column var_pop(Column col) {
    return new Column(com.snowflake.snowpark.functions.var_pop(col.toScalaColumn()));
  }

  /**
   * Returns an approximated value for the desired percentile. This function uses the t-Digest
   * algorithm.
   *
   * @since 0.9.0
   * @param col The input column
   * @param percentile The desired percentile
   * @return The result column
   */
  public static Column approx_percentile(Column col, double percentile) {
    return new Column(
        com.snowflake.snowpark.functions.approx_percentile(col.toScalaColumn(), percentile));
  }

  /**
   * Returns the internal representation of the t-Digest state (as a JSON object) at the end of
   * aggregation. This function uses the t-Digest algorithm.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column approx_percentile_accumulate(Column col) {
    return new Column(
        com.snowflake.snowpark.functions.approx_percentile_accumulate(col.toScalaColumn()));
  }

  /**
   * Returns the desired approximated percentile value for the specified t-Digest state.
   * APPROX_PERCENTILE_ESTIMATE(APPROX_PERCENTILE_ACCUMULATE(.)) is equivalent to
   * APPROX_PERCENTILE(.).
   *
   * @since 0.9.0
   * @param col The input column
   * @param percentile The desired percentile
   * @return The result column
   */
  public static Column approx_percentile_estimate(Column col, double percentile) {
    return new Column(
        com.snowflake.snowpark.functions.approx_percentile_estimate(
            col.toScalaColumn(), percentile));
  }

  /**
   * Combines (merges) percentile input states into a single output state.
   *
   * <p>This allows scenarios where APPROX_PERCENTILE_ACCUMULATE is run over horizontal partitions
   * of the same table, producing an algorithm state for each table partition. These states can
   * later be combined using APPROX_PERCENTILE_COMBINE, producing the same output state as a single
   * run of APPROX_PERCENTILE_ACCUMULATE over the entire table.
   *
   * @since 0.9.0
   * @param state The input column
   * @return The result column
   */
  public static Column approx_percentile_combine(Column state) {
    return new Column(
        com.snowflake.snowpark.functions.approx_percentile_combine(state.toScalaColumn()));
  }

  /**
   * Finds the cumulative distribution of a value with regard to other values within the same window
   * partition.
   *
   * @since 0.9.0
   * @return The result column
   */
  public static Column cume_dist() {
    return new Column(com.snowflake.snowpark.functions.cume_dist());
  }

  /**
   * Returns the rank of a value within a group of values, without gaps in the ranks. The rank value
   * starts at 1 and continues up sequentially. If two values are the same, they will have the same
   * rank.
   *
   * @since 0.9.0
   * @return The result column
   */
  public static Column dense_rank() {
    return new Column(com.snowflake.snowpark.functions.dense_rank());
  }

  /**
   * Accesses data in a previous row in the same result set without having to join the table to
   * itself.
   *
   * @since 0.9.0
   * @param col The input column
   * @param offset The function offset
   * @param defaultValue The default value
   * @return The result column
   */
  public static Column lag(Column col, int offset, Column defaultValue) {
    return new Column(
        com.snowflake.snowpark.functions.lag(
            col.toScalaColumn(), offset, defaultValue.toScalaColumn()));
  }

  /**
   * Accesses data in a previous row in the same result set without having to join the table to
   * itself.
   *
   * @since 0.9.0
   * @param col The input column
   * @param offset The function offset
   * @return The result column
   */
  public static Column lag(Column col, int offset) {
    return new Column(com.snowflake.snowpark.functions.lag(col.toScalaColumn(), offset));
  }

  /**
   * Accesses data in a previous row in the same result set without having to join the table to
   * itself.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column lag(Column col) {
    return new Column(com.snowflake.snowpark.functions.lag(col.toScalaColumn()));
  }

  /**
   * Divides an ordered data set equally into the number of buckets specified by n. Buckets are
   * sequentially numbered 1 through n.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column ntile(Column col) {
    return new Column(com.snowflake.snowpark.functions.ntile(col.toScalaColumn()));
  }

  /**
   * Returns the relative rank of a value within a group of values, specified as a percentage
   * ranging from 0.0 to 1.0.
   *
   * @since 0.9.0
   * @return The result column
   */
  public static Column percent_rank() {
    return new Column(com.snowflake.snowpark.functions.percent_rank());
  }

  /**
   * Returns the rank of a value within an ordered group of values. The rank value starts at 1 and
   * continues up.
   *
   * @since 0.1.0
   * @return The result column
   */
  public static Column rank() {
    return new Column(com.snowflake.snowpark.functions.rank());
  }

  /**
   * Returns a unique row number for each row within a window partition. The row number starts at 1
   * and continues up sequentially.
   *
   * @since 0.9.0
   * @return The result column
   */
  public static Column row_number() {
    return new Column(com.snowflake.snowpark.functions.row_number());
  }

  /**
   * Returns the first non-NULL expression among its arguments, or NULL if all its arguments are
   * NULL.
   *
   * @since 0.9.0
   * @param cols The list of input column
   * @return The result column
   */
  public static Column coalesce(Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.coalesce(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns true if the value in the column is not a number (NaN).
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column equal_nan(Column col) {
    return new Column(com.snowflake.snowpark.functions.equal_nan(col.toScalaColumn()));
  }

  /**
   * Returns true if the value in the column is null.
   *
   * @since 0.1.0
   * @param col The input column
   * @return The result column
   */
  public static Column is_null(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_null(col.toScalaColumn()));
  }

  /**
   * Returns the negation of the value in the column (equivalent to a unary minus).
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column negate(Column col) {
    return new Column(com.snowflake.snowpark.functions.negate(col.toScalaColumn()));
  }

  /**
   * Returns the inverse of a boolean expression.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column not(Column col) {
    return new Column(com.snowflake.snowpark.functions.not(col.toScalaColumn()));
  }

  /**
   * Each call returns a pseudo-random 64-bit integer.
   *
   * @since 0.9.0
   * @param seed The random seed
   * @return The result column
   */
  public static Column random(long seed) {
    return new Column(com.snowflake.snowpark.functions.random(seed));
  }

  /**
   * Each call returns a pseudo-random 64-bit integer.
   *
   * @since 0.9.0
   * @return The result column
   */
  public static Column random() {
    return new Column(com.snowflake.snowpark.functions.random());
  }

  /**
   * Returns the bitwise negation of a numeric expression.
   *
   * @since 0.9.0
   * @param col The input value
   * @return The result column
   */
  public static Column bitnot(Column col) {
    return new Column(com.snowflake.snowpark.functions.bitnot(col.toScalaColumn()));
  }

  /**
   * Converts an input expression to a decimal
   *
   * @since 0.9.0
   * @param expr The input column
   * @param precision The precision
   * @param scale The scale
   * @return The result column
   */
  public static Column to_decimal(Column expr, int precision, int scale) {
    return new Column(
        com.snowflake.snowpark.functions.to_decimal(expr.toScalaColumn(), precision, scale));
  }

  /**
   * Performs division like the division operator (/), but returns 0 when the divisor is 0 (rather
   * than reporting an error).
   *
   * @since 0.9.0
   * @param dividend The dividend
   * @param divisor The divisor
   * @return The result column
   */
  public static Column div0(Column dividend, Column divisor) {
    return new Column(
        com.snowflake.snowpark.functions.div0(dividend.toScalaColumn(), divisor.toScalaColumn()));
  }

  /**
   * Returns the square-root of a non-negative numeric expression.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column sqrt(Column col) {
    return new Column(com.snowflake.snowpark.functions.sqrt(col.toScalaColumn()));
  }

  /**
   * Returns the absolute value of a numeric expression.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column abs(Column col) {
    return new Column(com.snowflake.snowpark.functions.abs(col.toScalaColumn()));
  }

  /**
   * Computes the inverse cosine (arc cosine) of its input; the result is a number in the interval
   * [-pi, pi].
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column acos(Column col) {
    return new Column(com.snowflake.snowpark.functions.acos(col.toScalaColumn()));
  }

  /**
   * Computes the inverse sine (arc sine) of its argument; the result is a number in the interval
   * [-pi, pi].
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column asin(Column col) {
    return new Column(com.snowflake.snowpark.functions.asin(col.toScalaColumn()));
  }

  /**
   * Computes the inverse tangent (arc tangent) of its argument; the result is a number in the
   * interval [-pi, pi].
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column atan(Column col) {
    return new Column(com.snowflake.snowpark.functions.atan(col.toScalaColumn()));
  }

  /**
   * Computes the inverse tangent (arc tangent) of the ratio of its two arguments.
   *
   * @since 0.9.0
   * @param y The value of y
   * @param x The value of x
   * @return The result column
   */
  public static Column atan2(Column y, Column x) {
    return new Column(com.snowflake.snowpark.functions.atan2(y.toScalaColumn(), x.toScalaColumn()));
  }

  /**
   * Returns values from the specified column rounded to the nearest equal or larger integer.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column ceil(Column col) {
    return new Column(com.snowflake.snowpark.functions.ceil(col.toScalaColumn()));
  }

  /**
   * Returns values from the specified column rounded to the nearest equal or smaller integer.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column floor(Column col) {
    return new Column(com.snowflake.snowpark.functions.floor(col.toScalaColumn()));
  }

  /**
   * Computes the cosine of its argument; the argument should be expressed in radians.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column cos(Column col) {
    return new Column(com.snowflake.snowpark.functions.cos(col.toScalaColumn()));
  }

  /**
   * Computes the hyperbolic cosine of its argument.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column cosh(Column col) {
    return new Column(com.snowflake.snowpark.functions.cosh(col.toScalaColumn()));
  }

  /**
   * Computes Euler's number e raised to a floating-point value.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column exp(Column col) {
    return new Column(com.snowflake.snowpark.functions.exp(col.toScalaColumn()));
  }

  /**
   * Computes the factorial of its input. The input argument must be an integer expression in the
   * range of 0 to 33.
   *
   * @since 0.9.0
   * @param col The input column
   * @return The result column
   */
  public static Column factorial(Column col) {
    return new Column(com.snowflake.snowpark.functions.factorial(col.toScalaColumn()));
  }

  /**
   * Returns the largest value from a list of expressions. If any of the argument values is NULL,
   * the result is NULL. GREATEST supports all data types, including VARIANT.
   *
   * @since 0.9.0
   * @param cols The list of input column
   * @return The result column
   */
  public static Column greatest(Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.greatest(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns the smallest value from a list of expressions. LEAST supports all data types, including
   * VARIANT.
   *
   * @since 0.9.0
   * @param cols The list of input column
   * @return The result column
   */
  public static Column least(Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.least(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns the logarithm of a numeric expression.
   *
   * @since 0.9.0
   * @param base The value of base
   * @param a the value of A
   * @return The result column
   */
  public static Column log(Column base, Column a) {
    return new Column(
        com.snowflake.snowpark.functions.log(base.toScalaColumn(), a.toScalaColumn()));
  }

  /**
   * Returns a number (l) raised to the specified power (r).
   *
   * @since 0.9.0
   * @param l The value of l
   * @param r The value of r
   * @return The result column
   */
  public static Column pow(Column l, Column r) {
    return new Column(com.snowflake.snowpark.functions.pow(l.toScalaColumn(), r.toScalaColumn()));
  }

  /**
   * Returns rounded values for the specified column.
   *
   * @since 0.9.0
   * @param e The input column
   * @param scale The scale
   * @return The result column
   */
  public static Column round(Column e, Column scale) {
    return new Column(
        com.snowflake.snowpark.functions.round(e.toScalaColumn(), scale.toScalaColumn()));
  }

  /**
   * Returns rounded values for the specified column.
   *
   * @since 0.9.0
   * @param e The input column
   * @return The result column
   */
  public static Column round(Column e) {
    return new Column(com.snowflake.snowpark.functions.round(e.toScalaColumn()));
  }

  /**
   * Shifts the bits for a numeric expression numBits positions to the left.
   *
   * @since 0.9.0
   * @param e The input column
   * @param numBits The number of bits
   * @return The result column
   */
  public static Column bitshiftleft(Column e, Column numBits) {
    return new Column(
        com.snowflake.snowpark.functions.bitshiftleft(e.toScalaColumn(), numBits.toScalaColumn()));
  }

  /**
   * Shifts the bits for a numeric expression numBits positions to the right.
   *
   * @since 0.9.0
   * @param e The input column
   * @param numBits The number of bits
   * @return The result column
   */
  public static Column bitshiftright(Column e, Column numBits) {
    return new Column(
        com.snowflake.snowpark.functions.bitshiftright(e.toScalaColumn(), numBits.toScalaColumn()));
  }

  /**
   * Computes the sine of its argument; the argument should be expressed in radians.
   *
   * @since 0.9.0
   * @param e The input column
   * @return The result column
   */
  public static Column sin(Column e) {
    return new Column(com.snowflake.snowpark.functions.sin(e.toScalaColumn()));
  }

  /**
   * Computes the hyperbolic sine of its argument.
   *
   * @since 0.9.0
   * @param e The input column
   * @return The result column
   */
  public static Column sinh(Column e) {
    return new Column(com.snowflake.snowpark.functions.sinh(e.toScalaColumn()));
  }

  /**
   * Computes the tangent of its argument; the argument should be expressed in radians.
   *
   * @since 0.9.0
   * @param e The input column
   * @return The result column
   */
  public static Column tan(Column e) {
    return new Column(com.snowflake.snowpark.functions.tan(e.toScalaColumn()));
  }

  /**
   * Computes the hyperbolic tangent of its argument.
   *
   * @since 0.9.0
   * @param e The input column
   * @return The result column
   */
  public static Column tanh(Column e) {
    return new Column(com.snowflake.snowpark.functions.tanh(e.toScalaColumn()));
  }

  /**
   * Converts radians to degrees.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column degrees(Column e) {
    return new Column(com.snowflake.snowpark.functions.degrees(e.toScalaColumn()));
  }

  /**
   * Converts degrees to radians.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column radians(Column e) {
    return new Column(com.snowflake.snowpark.functions.radians(e.toScalaColumn()));
  }

  /**
   * Returns a 32-character hex-encoded string containing the 128-bit MD5 message digest.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column md5(Column e) {
    return new Column(com.snowflake.snowpark.functions.md5(e.toScalaColumn()));
  }

  /**
   * Returns a 40-character hex-encoded string containing the 160-bit SHA-1 message digest.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column sha1(Column e) {
    return new Column(com.snowflake.snowpark.functions.sha1(e.toScalaColumn()));
  }

  /**
   * Returns a hex-encoded string containing the N-bit SHA-2 message digest, where N is the
   * specified output digest size.
   *
   * @since 0.11.0
   * @param e The input column
   * @param numBits The number of bits
   * @return The result column
   */
  public static Column sha2(Column e, int numBits) {
    return new Column(com.snowflake.snowpark.functions.sha2(e.toScalaColumn(), numBits));
  }

  /**
   * Returns a signed 64-bit hash value. Note that HASH never returns NULL, even for NULL inputs.
   *
   * @since 0.11.0
   * @param cols The list of input column
   * @return The result column
   */
  public static Column hash(Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.hash(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns the ASCII code for the first character of a string. If the string is empty, a value of
   * 0 is returned.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column ascii(Column e) {
    return new Column(com.snowflake.snowpark.functions.ascii(e.toScalaColumn()));
  }

  /**
   * Concatenates two or more strings, or concatenates two or more binary values. If any of the
   * values is null, the result is also null.
   *
   * @since 0.11.0
   * @param separator The separator
   * @param exprs The list of input values
   * @return The result column
   */
  public static Column concat_ws(Column separator, Column... exprs) {
    return new Column(
        com.snowflake.snowpark.functions.concat_ws(
            separator.toScalaColumn(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(exprs))));
  }

  /**
   * Returns the input string with the first letter of each word in uppercase and the subsequent
   * letters in lowercase.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column initcap(Column e) {
    return new Column(com.snowflake.snowpark.functions.initcap(e.toScalaColumn()));
  }

  /**
   * Returns the length of an input string or binary value. For strings, the length is the number of
   * characters, and UTF-8 characters are counted as a single character. For binary, the length is
   * the number of bytes.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column length(Column e) {
    return new Column(com.snowflake.snowpark.functions.length(e.toScalaColumn()));
  }

  /**
   * Returns the input string with all characters converted to lowercase.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column lower(Column e) {
    return new Column(com.snowflake.snowpark.functions.lower(e.toScalaColumn()));
  }

  /**
   * Returns the input string with all characters converted to uppercase.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column upper(Column e) {
    return new Column(com.snowflake.snowpark.functions.upper(e.toScalaColumn()));
  }

  /**
   * Left-pads a string with characters from another string, or left-pads a binary value with bytes
   * from another binary value.
   *
   * @since 0.11.0
   * @param str The input string
   * @param len The length
   * @param pad The pad
   * @return The result column
   */
  public static Column lpad(Column str, Column len, Column pad) {
    return new Column(
        com.snowflake.snowpark.functions.lpad(
            str.toScalaColumn(), len.toScalaColumn(), pad.toScalaColumn()));
  }

  /**
   * Right-pads a string with characters from another string, or right-pads a binary value with
   * bytes from another binary value.
   *
   * @since 0.11.0
   * @param str The input string
   * @param len The length
   * @param pad The pad
   * @return The result column
   */
  public static Column rpad(Column str, Column len, Column pad) {
    return new Column(
        com.snowflake.snowpark.functions.rpad(
            str.toScalaColumn(), len.toScalaColumn(), pad.toScalaColumn()));
  }

  /**
   * Removes leading characters, including whitespace, from a string.
   *
   * @since 0.11.0
   * @param e The input column
   * @param trimString The trim string
   * @return The result column
   */
  public static Column ltrim(Column e, Column trimString) {
    return new Column(
        com.snowflake.snowpark.functions.ltrim(e.toScalaColumn(), trimString.toScalaColumn()));
  }

  /**
   * Removes leading characters, including whitespace, from a string.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column ltrim(Column e) {
    return new Column(com.snowflake.snowpark.functions.ltrim(e.toScalaColumn()));
  }

  /**
   * Removes trailing characters, including whitespace, from a string.
   *
   * @since 0.11.0
   * @param e The input column
   * @param trimString The trim string
   * @return The result column
   */
  public static Column rtrim(Column e, Column trimString) {
    return new Column(
        com.snowflake.snowpark.functions.rtrim(e.toScalaColumn(), trimString.toScalaColumn()));
  }

  /**
   * Removes trailing characters, including whitespace, from a string.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column rtrim(Column e) {
    return new Column(com.snowflake.snowpark.functions.rtrim(e.toScalaColumn()));
  }

  /**
   * Removes leading and trailing characters from a string.
   *
   * @since 0.11.0
   * @param e The input column
   * @param trimString The trim string
   * @return The result column
   */
  public static Column trim(Column e, Column trimString) {
    return new Column(
        com.snowflake.snowpark.functions.trim(e.toScalaColumn(), trimString.toScalaColumn()));
  }

  /**
   * Builds a string by repeating the input for the specified number of times.
   *
   * @since 0.11.0
   * @param str The input string
   * @param n The time of repeat
   * @return The result column
   */
  public static Column repeat(Column str, Column n) {
    return new Column(
        com.snowflake.snowpark.functions.repeat(str.toScalaColumn(), n.toScalaColumn()));
  }

  /**
   * Returns a string that contains a phonetic representation of the input string.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column soundex(Column e) {
    return new Column(com.snowflake.snowpark.functions.soundex(e.toScalaColumn()));
  }

  /**
   * Splits a given string with a given separator and returns the result in an array of strings.
   *
   * @since 0.11.0
   * @param str The input string
   * @param pattern The pattern
   * @return The result column
   */
  public static Column split(Column str, Column pattern) {
    return new Column(
        com.snowflake.snowpark.functions.split(str.toScalaColumn(), pattern.toScalaColumn()));
  }

  /**
   * Returns the portion of the string or binary value str, starting from the character/byte
   * specified by pos, with limited length.
   *
   * @since 0.11.0
   * @param str The input string
   * @param len The length
   * @param pos The position
   * @return The result column
   */
  public static Column substring(Column str, Column pos, Column len) {
    return new Column(
        com.snowflake.snowpark.functions.substring(
            str.toScalaColumn(), pos.toScalaColumn(), len.toScalaColumn()));
  }

  /**
   * Returns a non-deterministic value for the specified column.
   *
   * @since 1.2.0
   * @param e The input column
   * @return The result column
   */
  public static Column any_value(Column e) {
    return new Column(com.snowflake.snowpark.functions.any_value(e.toScalaColumn()));
  }

  /**
   * Translates src from the characters in matchingString to the characters in replaceString.
   *
   * @since 0.11.0
   * @param src The source column
   * @param matchingString The matching string
   * @param replaceString The replacement
   * @return The result column
   */
  public static Column translate(Column src, Column matchingString, Column replaceString) {
    return new Column(
        com.snowflake.snowpark.functions.translate(
            src.toScalaColumn(), matchingString.toScalaColumn(), replaceString.toScalaColumn()));
  }

  /**
   * Returns true if col contains str.
   *
   * @since 0.11.0
   * @param col The input column
   * @param str The target string
   * @return The result column
   */
  public static Column contains(Column col, Column str) {
    return new Column(
        com.snowflake.snowpark.functions.contains(col.toScalaColumn(), str.toScalaColumn()));
  }

  /**
   * Returns true if col starts with str.
   *
   * @since 0.11.0
   * @param col The input column
   * @param str The target string
   * @return The result column
   */
  public static Column startswith(Column col, Column str) {
    return new Column(
        com.snowflake.snowpark.functions.startswith(col.toScalaColumn(), str.toScalaColumn()));
  }

  /**
   * Converts a Unicode code point (including 7-bit ASCII) into the character that matches the input
   * Unicode.
   *
   * <p>alias for Snowflake char function.
   *
   * @since 0.11.0
   * @param col The input column
   * @return The result column
   */
  public static Column chr(Column col) {
    return new Column(com.snowflake.snowpark.internal.JavaUtils.charFunc(col.toScalaColumn()));
  }

  /**
   * Adds or subtracts a specified number of months to a date or timestamp, preserving the
   * end-of-month information.
   *
   * @since 0.11.0
   * @param startDate The start date
   * @param numMonths the number of Months
   * @return The result column
   */
  public static Column add_months(Column startDate, Column numMonths) {
    return new Column(
        com.snowflake.snowpark.functions.add_months(
            startDate.toScalaColumn(), numMonths.toScalaColumn()));
  }

  /**
   * Returns the current date of the system.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_date() {
    return new Column(com.snowflake.snowpark.functions.current_date());
  }

  /**
   * Returns the current timestamp for the system.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_timestamp() {
    return new Column(com.snowflake.snowpark.functions.current_timestamp());
  }

  /**
   * Returns the name of the region for the account where the current user is logged in.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_region() {
    return new Column(com.snowflake.snowpark.functions.current_region());
  }

  /**
   * Returns the current time for the system.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_time() {
    return new Column(com.snowflake.snowpark.functions.current_time());
  }

  /**
   * Returns the current Snowflake version.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_version() {
    return new Column(com.snowflake.snowpark.functions.current_version());
  }

  /**
   * Returns the account used by the user's current session.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_account() {
    return new Column(com.snowflake.snowpark.functions.current_account());
  }

  /**
   * Returns the name of the role in use for the current session.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_role() {
    return new Column(com.snowflake.snowpark.functions.current_role());
  }

  /**
   * Returns a JSON string that lists all roles granted to the current user.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_available_roles() {
    return new Column(com.snowflake.snowpark.functions.current_available_roles());
  }

  /**
   * Returns a unique system identifier for the Snowflake session corresponding to the present
   * connection.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_session() {
    return new Column(com.snowflake.snowpark.functions.current_session());
  }

  /**
   * Returns the SQL text of the statement that is currently executing.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_statement() {
    return new Column(com.snowflake.snowpark.functions.current_statement());
  }

  /**
   * Returns the name of the user currently logged into the system.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_user() {
    return new Column(com.snowflake.snowpark.functions.current_user());
  }

  /**
   * Returns the name of the database in use for the current session.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_database() {
    return new Column(com.snowflake.snowpark.functions.current_database());
  }

  /**
   * Returns the name of the schema in use by the current session.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_schema() {
    return new Column(com.snowflake.snowpark.functions.current_schema());
  }

  /**
   * Returns active search path schemas.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_schemas() {
    return new Column(com.snowflake.snowpark.functions.current_schemas());
  }

  /**
   * Returns the name of the warehouse in use for the current session.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column current_warehouse() {
    return new Column(com.snowflake.snowpark.functions.current_warehouse());
  }

  /**
   * Returns the current timestamp for the system, but in the UTC time zone.
   *
   * @since 0.11.0
   * @return The result column
   */
  public static Column sysdate() {
    return new Column(com.snowflake.snowpark.functions.sysdate());
  }

  /**
   * Converts the given sourceTimestampNTZ from sourceTimeZone to targetTimeZone.
   *
   * <p>Supported time zones are listed <a
   * href="https://docs.snowflake.com/en/sql-reference/functions/convert_timezone.html#usage-notes">here</a>
   *
   * <p>Example
   *
   * <pre>{@code
   * df.select(Functions.convert_timezone(Functions.lit("America/Los_Angeles"),
   * Functions.lit("America/New_York"), df.col("time")));
   * }</pre>
   *
   * @since 0.11.0
   * @param sourceTimestampNTZ The timestamp
   * @param targetTimeZone The target time zone
   * @param sourceTimeZone The source time zone
   * @return The result column
   */
  public static Column convert_timezone(
      Column sourceTimeZone, Column targetTimeZone, Column sourceTimestampNTZ) {
    return new Column(
        com.snowflake.snowpark.functions.convert_timezone(
            sourceTimeZone.toScalaColumn(),
            targetTimeZone.toScalaColumn(),
            sourceTimestampNTZ.toScalaColumn()));
  }

  /**
   * Converts the given sourceTimestampNTZ to targetTimeZone.
   *
   * <p>Supported time zones are listed <a
   * href="https://docs.snowflake.com/en/sql-reference/functions/convert_timezone.html#usage-notes">here</a>
   *
   * <p>Example
   *
   * <pre>{@code
   * df.select(Functions.convert_timezone(Functions.lit("America/New_York"), df.col("time")));
   * }</pre>
   *
   * @since 0.11.0
   * @param sourceTimestamp The timestamp
   * @param targetTimeZone The target time zone
   * @return The result column
   */
  public static Column convert_timezone(Column targetTimeZone, Column sourceTimestamp) {
    return new Column(
        com.snowflake.snowpark.functions.convert_timezone(
            targetTimeZone.toScalaColumn(), sourceTimestamp.toScalaColumn()));
  }

  /**
   * Extracts the year from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column year(Column e) {
    return new Column(com.snowflake.snowpark.functions.year(e.toScalaColumn()));
  }

  /**
   * Extracts the quarter from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column quarter(Column e) {
    return new Column(com.snowflake.snowpark.functions.quarter(e.toScalaColumn()));
  }

  /**
   * Extracts the month from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column month(Column e) {
    return new Column(com.snowflake.snowpark.functions.month(e.toScalaColumn()));
  }

  /**
   * Extracts the day of week from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column dayofweek(Column e) {
    return new Column(com.snowflake.snowpark.functions.dayofweek(e.toScalaColumn()));
  }

  /**
   * Extracts the day of month from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column dayofmonth(Column e) {
    return new Column(com.snowflake.snowpark.functions.dayofmonth(e.toScalaColumn()));
  }

  /**
   * Extracts the day of year from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column dayofyear(Column e) {
    return new Column(com.snowflake.snowpark.functions.dayofyear(e.toScalaColumn()));
  }

  /**
   * Returns the last day of the specified date part for a date or timestamp. Commonly used to
   * return the last day of the month for a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column last_day(Column e) {
    return new Column(com.snowflake.snowpark.functions.last_day(e.toScalaColumn()));
  }

  /**
   * Extracts the week of year from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column weekofyear(Column e) {
    return new Column(com.snowflake.snowpark.functions.weekofyear(e.toScalaColumn()));
  }

  /**
   * Extracts the hour from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column hour(Column e) {
    return new Column(com.snowflake.snowpark.functions.hour(e.toScalaColumn()));
  }

  /**
   * Extracts the minute from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column minute(Column e) {
    return new Column(com.snowflake.snowpark.functions.minute(e.toScalaColumn()));
  }

  /**
   * Extracts the second from a date or timestamp.
   *
   * @since 0.11.0
   * @param e The input column
   * @return The result column
   */
  public static Column second(Column e) {
    return new Column(com.snowflake.snowpark.functions.second(e.toScalaColumn()));
  }

  /**
   * Returns the date of the first specified DOW (day of week) that occurs after the input date.
   *
   * @since 0.11.0
   * @param date The date
   * @param dayOfWeek The day of week
   * @return The result column
   */
  public static Column next_day(Column date, Column dayOfWeek) {
    return new Column(
        com.snowflake.snowpark.functions.next_day(date.toScalaColumn(), dayOfWeek.toScalaColumn()));
  }

  /**
   * Returns the date of the first specified DOW (day of week) that occurs before the input date.
   *
   * @since 0.11.0
   * @param date The date
   * @param dayOfWeek The day of week
   * @return The result column
   */
  public static Column previous_day(Column date, Column dayOfWeek) {
    return new Column(
        com.snowflake.snowpark.functions.previous_day(
            date.toScalaColumn(), dayOfWeek.toScalaColumn()));
  }

  /**
   * Converts an input expression into the corresponding timestamp.
   *
   * @since 0.11.0
   * @param s The input column
   * @return The result column
   */
  public static Column to_timestamp(Column s) {
    return new Column(com.snowflake.snowpark.functions.to_timestamp(s.toScalaColumn()));
  }

  /**
   * Converts an input expression into the corresponding timestamp.
   *
   * @since 0.11.0
   * @param s The input value
   * @param fmt The time format
   * @return The result column
   */
  public static Column to_timestamp(Column s, Column fmt) {
    return new Column(
        com.snowflake.snowpark.functions.to_timestamp(s.toScalaColumn(), fmt.toScalaColumn()));
  }

  /**
   * Converts an input expression to a date.
   *
   * @since 0.11.0
   * @param e The input value
   * @return The result column
   */
  public static Column to_date(Column e) {
    return new Column(com.snowflake.snowpark.functions.to_date(e.toScalaColumn()));
  }

  /**
   * Converts an input expression to a date.
   *
   * @since 0.11.0
   * @param e The input value
   * @param fmt The time format
   * @return The result column
   */
  public static Column to_date(Column e, Column fmt) {
    return new Column(
        com.snowflake.snowpark.functions.to_date(e.toScalaColumn(), fmt.toScalaColumn()));
  }

  /**
   * Creates a date from individual numeric components that represent the year, month, and day of
   * the month.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @return The result column
   */
  public static Column date_from_parts(Column year, Column month, Column day) {
    return new Column(
        com.snowflake.snowpark.functions.date_from_parts(
            year.toScalaColumn(), month.toScalaColumn(), day.toScalaColumn()));
  }

  /**
   * Creates a time from individual numeric components.
   *
   * @since 0.11.0
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @param nanosecond The nanosecond
   * @return The result column
   */
  public static Column time_from_parts(
      Column hour, Column minute, Column second, Column nanosecond) {
    return new Column(
        com.snowflake.snowpark.functions.time_from_parts(
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn(),
            nanosecond.toScalaColumn()));
  }

  /**
   * Creates a time from individual numeric components.
   *
   * @since 0.11.0
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @return The result column
   */
  public static Column time_from_parts(Column hour, Column minute, Column second) {
    return new Column(
        com.snowflake.snowpark.functions.time_from_parts(
            hour.toScalaColumn(), minute.toScalaColumn(), second.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @return The result column
   */
  public static Column timestamp_from_parts(
      Column year, Column month, Column day, Column hour, Column minute, Column second) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @param nanosecond The nanosecond
   * @return The result column
   */
  public static Column timestamp_from_parts(
      Column year,
      Column month,
      Column day,
      Column hour,
      Column minute,
      Column second,
      Column nanosecond) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn(),
            nanosecond.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param dateExpr The date expression
   * @param timeExpr The time expression
   * @return The result column
   */
  public static Column timestamp_from_parts(Column dateExpr, Column timeExpr) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_from_parts(
            dateExpr.toScalaColumn(), timeExpr.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @return The result column
   */
  public static Column timestamp_ltz_from_parts(
      Column year, Column month, Column day, Column hour, Column minute, Column second) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_ltz_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @param nanosecond The nanosecond
   * @return The result column
   */
  public static Column timestamp_ltz_from_parts(
      Column year,
      Column month,
      Column day,
      Column hour,
      Column minute,
      Column second,
      Column nanosecond) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_ltz_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn(),
            nanosecond.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @return The result column
   */
  public static Column timestamp_ntz_from_parts(
      Column year, Column month, Column day, Column hour, Column minute, Column second) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_ntz_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @param nanosecond The nanosecond
   * @return The result column
   */
  public static Column timestamp_ntz_from_parts(
      Column year,
      Column month,
      Column day,
      Column hour,
      Column minute,
      Column second,
      Column nanosecond) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_ntz_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn(),
            nanosecond.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param dateExpr The date expression
   * @param timeExpr The time expression
   * @return The result column
   */
  public static Column timestamp_ntz_from_parts(Column dateExpr, Column timeExpr) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_ntz_from_parts(
            dateExpr.toScalaColumn(), timeExpr.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @return The result column
   */
  public static Column timestamp_tz_from_parts(
      Column year, Column month, Column day, Column hour, Column minute, Column second) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_tz_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @param nanosecond The nanosecond
   * @return The result column
   */
  public static Column timestamp_tz_from_parts(
      Column year,
      Column month,
      Column day,
      Column hour,
      Column minute,
      Column second,
      Column nanosecond) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_tz_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn(),
            nanosecond.toScalaColumn()));
  }

  /**
   * Creates a timestamp from individual numeric components. If no time zone is in effect, the
   * function can be used to create a timestamp from a date expression and a time expression.
   *
   * @since 0.11.0
   * @param year The year
   * @param month The month
   * @param day The day
   * @param hour The hour
   * @param minute The minute
   * @param second The second
   * @param nanosecond The nanosecond
   * @param timezone The time zone
   * @return The result column
   */
  public static Column timestamp_tz_from_parts(
      Column year,
      Column month,
      Column day,
      Column hour,
      Column minute,
      Column second,
      Column nanosecond,
      Column timezone) {
    return new Column(
        com.snowflake.snowpark.functions.timestamp_tz_from_parts(
            year.toScalaColumn(),
            month.toScalaColumn(),
            day.toScalaColumn(),
            hour.toScalaColumn(),
            minute.toScalaColumn(),
            second.toScalaColumn(),
            nanosecond.toScalaColumn(),
            timezone.toScalaColumn()));
  }

  /**
   * Extracts the three-letter day-of-week name from the specified date or timestamp.
   *
   * @since 0.11.0
   * @param expr The input value
   * @return The result column
   */
  public static Column dayname(Column expr) {
    return new Column(com.snowflake.snowpark.functions.dayname(expr.toScalaColumn()));
  }

  /**
   * Extracts the three-letter month name from the specified date or timestamp.
   *
   * @since 0.11.0
   * @param expr The input value
   * @return The result column
   */
  public static Column monthname(Column expr) {
    return new Column(com.snowflake.snowpark.functions.monthname(expr.toScalaColumn()));
  }

  /**
   * Adds the specified value for the specified date or time art to date or time expr.
   *
   * <p>Supported date and time parts are listed <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts">here</a>
   *
   * <p>Example: add one year on dates
   *
   * <pre>{@code
   * date.select(Functions.dateadd("year", Functions.lit(1), date.col("date_col")))
   * }</pre>
   *
   * @since 0.11.0
   * @param part The part of time being added
   * @param value The value being added
   * @param expr The input value
   * @return The result column
   */
  public static Column dateadd(String part, Column value, Column expr) {
    return new Column(
        com.snowflake.snowpark.functions.dateadd(
            part, value.toScalaColumn(), expr.toScalaColumn()));
  }
  /**
   * Calculates the difference between two date, time, or timestamp columns based on the date or
   * time part requested.
   *
   * <p>Supported date and time parts are listed <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts">here</a>
   *
   * <p>Example: year difference between two date columns
   *
   * <pre>{@code
   * date.select(Functions.datediff("year", date.col("date_col1"), date.col("date_col2")))
   * }</pre>
   *
   * @since 0.11.0
   * @param part The part of time
   * @param col1 The first input value
   * @param col2 The second input value
   * @return The result column
   */
  public static Column datediff(String part, Column col1, Column col2) {
    return new Column(
        com.snowflake.snowpark.functions.datediff(
            part, col1.toScalaColumn(), col2.toScalaColumn()));
  }

  /**
   * Rounds the input expression down to the nearest (or equal) integer closer to zero, or to the
   * nearest equal or smaller value with the specified number of places after the decimal point.
   *
   * @since 0.11.0
   * @param expr The input value
   * @param scale The scale
   * @return The result column
   */
  public static Column trunc(Column expr, Column scale) {
    return new Column(
        com.snowflake.snowpark.functions.trunc(expr.toScalaColumn(), scale.toScalaColumn()));
  }

  /**
   * Truncates a DATE, TIME, or TIMESTAMP to the specified precision.
   *
   * @since 0.11.0
   * @param format The time format
   * @param timestamp The input timestamp
   * @return The result column
   */
  public static Column date_trunc(String format, Column timestamp) {
    return new Column(
        com.snowflake.snowpark.functions.date_trunc(format, timestamp.toScalaColumn()));
  }

  /**
   * Concatenates one or more strings, or concatenates one or more binary values. If any of the
   * values is null, the result is also null.
   *
   * @since 0.11.0
   * @param exprs A list of input values
   * @return The result column
   */
  public static Column concat(Column... exprs) {
    return new Column(
        com.snowflake.snowpark.functions.concat(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(exprs))));
  }

  /**
   * Compares whether two arrays have at least one element in common. Returns TRUE if there is at
   * least one element in common; otherwise returns FALSE. The function is NULL-safe, meaning it
   * treats NULLs as known values for comparing equality.
   *
   * @since 0.11.0
   * @param a1 The first input array
   * @param a2 The second input array
   * @return The result column
   */
  public static Column arrays_overlap(Column a1, Column a2) {
    return new Column(
        com.snowflake.snowpark.functions.arrays_overlap(a1.toScalaColumn(), a2.toScalaColumn()));
  }

  /**
   * Returns TRUE if expr ends with str.
   *
   * @since 0.11.0
   * @param expr The input value
   * @param str The ending string
   * @return The result column
   */
  public static Column endswith(Column expr, Column str) {
    return new Column(
        com.snowflake.snowpark.functions.endswith(expr.toScalaColumn(), str.toScalaColumn()));
  }

  /**
   * Replaces a substring of the specified length, starting at the specified position, with a new
   * string or binary value.
   *
   * @since 0.11.0
   * @param baseExpr The base expression
   * @param position The position
   * @param insertExpr The new expression
   * @param length The length
   * @return The result column
   */
  public static Column insert(Column baseExpr, Column position, Column length, Column insertExpr) {
    return new Column(
        com.snowflake.snowpark.functions.insert(
            baseExpr.toScalaColumn(),
            position.toScalaColumn(),
            length.toScalaColumn(),
            insertExpr.toScalaColumn()));
  }

  /**
   * Returns a left most substring of strExpr.
   *
   * @since 0.11.0
   * @param strExpr The input string
   * @param lengthExpr The length
   * @return The result column
   */
  public static Column left(Column strExpr, Column lengthExpr) {
    return new Column(
        com.snowflake.snowpark.functions.left(strExpr.toScalaColumn(), lengthExpr.toScalaColumn()));
  }

  /**
   * Returns a right most substring of strExpr.
   *
   * @since 0.11.0
   * @param strExpr The input string
   * @param lengthExpr The length
   * @return The result column
   */
  public static Column right(Column strExpr, Column lengthExpr) {
    return new Column(
        com.snowflake.snowpark.functions.right(
            strExpr.toScalaColumn(), lengthExpr.toScalaColumn()));
  }

  /**
   * Returns the number of times that a pattern occurs in a strExpr.
   *
   * <p>Pattern syntax is specified <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes">here</a>
   *
   * <p>Parameter detail is specified <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-parameters-argument">here</a>
   *
   * @since 0.11.0
   * @param strExpr The input string
   * @param pattern The pattern
   * @param position The position
   * @param parameters The parameters
   * @return The result column
   */
  public static Column regexp_count(
      Column strExpr, Column pattern, Column position, Column parameters) {
    return new Column(
        com.snowflake.snowpark.functions.regexp_count(
            strExpr.toScalaColumn(),
            pattern.toScalaColumn(),
            position.toScalaColumn(),
            parameters.toScalaColumn()));
  }

  /**
   * Returns the number of times that a pattern occurs in a strExpr.
   *
   * <p>Pattern syntax is specified <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-general-usage-notes">here</a>
   *
   * <p>Parameter detail is specified <a
   * href="https://docs.snowflake.com/en/sql-reference/functions-regexp.html#label-regexp-parameters-argument">here</a>
   *
   * @since 0.11.0
   * @param strExpr The input string
   * @param pattern The pattern
   * @return The result column
   */
  public static Column regexp_count(Column strExpr, Column pattern) {
    return new Column(
        com.snowflake.snowpark.functions.regexp_count(
            strExpr.toScalaColumn(), pattern.toScalaColumn()));
  }

  /**
   * Returns the subject with the specified pattern (or all occurrences of the pattern) removed. If
   * no matches are found, returns the original subject.
   *
   * @param strExpr The input string
   * @param pattern The pattern
   * @return The result column
   * @since 1.9.0
   */
  public static Column regexp_replace(Column strExpr, Column pattern) {
    return new Column(
        com.snowflake.snowpark.functions.regexp_replace(
            strExpr.toScalaColumn(), pattern.toScalaColumn()));
  }

  /**
   * Returns the subject with the specified pattern (or all occurrences of the pattern) replaced by
   * a replacement string. If no matches are found, returns the original subject.
   *
   * @param strExpr The input string
   * @param pattern The pattern
   * @param replacement The replacement string
   * @return The result column
   * @since 1.9.0
   */
  public static Column regexp_replace(Column strExpr, Column pattern, Column replacement) {
    return new Column(
        com.snowflake.snowpark.functions.regexp_replace(
            strExpr.toScalaColumn(), pattern.toScalaColumn(), replacement.toScalaColumn()));
  }

  /**
   * Removes all occurrences of a specified strExpr, and optionally replaces them with replacement.
   *
   * @since 0.11.0
   * @param strExpr The input string
   * @param pattern The pattern
   * @param replacement The replacement string
   * @return The result column
   */
  public static Column replace(Column strExpr, Column pattern, Column replacement) {
    return new Column(
        com.snowflake.snowpark.functions.replace(
            strExpr.toScalaColumn(), pattern.toScalaColumn(), replacement.toScalaColumn()));
  }

  /**
   * Removes all occurrences of a specified strExpr, and optionally replaces them with replacement.
   *
   * @since 0.11.0
   * @param strExpr The input string
   * @param pattern The pattern
   * @return The result column
   */
  public static Column replace(Column strExpr, Column pattern) {
    return new Column(
        com.snowflake.snowpark.functions.replace(strExpr.toScalaColumn(), pattern.toScalaColumn()));
  }

  /**
   * Searches for targetExpr in sourceExpr and, if successful, returns the position (1-based) of the
   * targetExpr in sourceExpr.
   *
   * @since 0.11.0
   * @param sourceExpr The source value
   * @param targetExpr The target value
   * @return The result column
   */
  public static Column charindex(Column targetExpr, Column sourceExpr) {
    return new Column(
        com.snowflake.snowpark.functions.charindex(
            targetExpr.toScalaColumn(), sourceExpr.toScalaColumn()));
  }

  /**
   * Searches for targetExpr in sourceExpr and, if successful, returns the position (1-based) of the
   * targetExpr in sourceExpr.
   *
   * @since 0.11.0
   * @param sourceExpr The source value
   * @param targetExpr The target value
   * @param position The position
   * @return The result column
   */
  public static Column charindex(Column targetExpr, Column sourceExpr, Column position) {
    return new Column(
        com.snowflake.snowpark.functions.charindex(
            targetExpr.toScalaColumn(), sourceExpr.toScalaColumn(), position.toScalaColumn()));
  }

  /**
   * Returns a copy of expr, but with the specified collationSpec property instead of the original
   * collation specification property.
   *
   * <p>Collation Specification is specified <a
   * href="https://docs.snowflake.com/en/sql-reference/collation.html#label-collation-specification">
   * here </a>
   *
   * @since 0.11.0
   * @param expr The input value
   * @param collationSpec The collation specification
   * @return The result column
   */
  public static Column collate(Column expr, String collationSpec) {
    return new Column(
        com.snowflake.snowpark.functions.collate(expr.toScalaColumn(), collationSpec));
  }

  /**
   * Returns the collation specification of expr.
   *
   * @since 0.11.0
   * @param expr The input value
   * @return The result column
   */
  public static Column collation(Column expr) {
    return new Column(com.snowflake.snowpark.functions.collation(expr.toScalaColumn()));
  }

  /**
   * Returns an ARRAY that contains the matching elements in the two input ARRAYs.
   *
   * @since 0.11.0
   * @param col1 The first input array
   * @param col2 The second input array
   * @return The result column
   */
  public static Column array_intersection(Column col1, Column col2) {
    return new Column(
        com.snowflake.snowpark.functions.array_intersection(
            col1.toScalaColumn(), col2.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains an ARRAY value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_array(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_array(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a Boolean value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_boolean(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_boolean(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a binary value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_binary(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_binary(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a string value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_char(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_char(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a string value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_varchar(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_varchar(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a DATE value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_date(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_date(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a DATE value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_date_value(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_date_value(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a fixed-point decimal value or integer.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_decimal(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_decimal(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a floating-point value, fixed-point
   * decimal, or integer.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_double(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_double(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a floating-point value, fixed-point
   * decimal, or integer.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_real(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_real(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains an integer value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_integer(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_integer(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column is a JSON null value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_null_value(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_null_value(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains an OBJECT value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_object(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_object(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a TIME value.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_time(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_time(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value to be interpreted using
   * the local time zone.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_timestamp_ltz(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_timestamp_ltz(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value with no time zone.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_timestamp_ntz(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_timestamp_ntz(col.toScalaColumn()));
  }

  /**
   * Returns true if the specified VARIANT column contains a TIMESTAMP value with a time zone.
   *
   * @since 0.11.0
   * @param col The input value
   * @return The result column
   */
  public static Column is_timestamp_tz(Column col) {
    return new Column(com.snowflake.snowpark.functions.is_timestamp_tz(col.toScalaColumn()));
  }

  /**
   * Checks the validity of a JSON document. If the input string is a valid JSON document or a NULL
   * (i.e. no error would occur when parsing the input string), the function returns NULL. In case
   * of a JSON parsing error, the function returns a string that contains the error message.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column check_json(Column col) {
    return new Column(com.snowflake.snowpark.functions.check_json(col.toScalaColumn()));
  }

  /**
   * Checks the validity of an XML document. If the input string is a valid XML document or a NULL
   * (i.e. no error would occur when parsing the input string), the function returns NULL. In case
   * of an XML parsing error, the output string contains the error message.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column check_xml(Column col) {
    return new Column(com.snowflake.snowpark.functions.check_xml(col.toScalaColumn()));
  }

  /**
   * Parses a JSON string and returns the value of an element at a specified path in the resulting
   * JSON document.
   *
   * @param col Column containing the JSON string that should be parsed.
   * @param path Column containing the path to the element that should be extracted.
   * @since 0.12.0
   * @return The result column
   */
  public static Column json_extract_path_text(Column col, Column path) {
    return new Column(
        com.snowflake.snowpark.functions.json_extract_path_text(
            col.toScalaColumn(), path.toScalaColumn()));
  }

  /**
   * Parse the value of the specified column as a JSON string and returns the resulting JSON
   * document.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column parse_json(Column col) {
    return new Column(com.snowflake.snowpark.functions.parse_json(col.toScalaColumn()));
  }

  /**
   * Parse the value of the specified column as a JSON string and returns the resulting XML
   * document.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column parse_xml(Column col) {
    return new Column(com.snowflake.snowpark.functions.parse_xml(col.toScalaColumn()));
  }

  /**
   * Converts a JSON "null" value in the specified column to a SQL NULL value. All other VARIANT
   * values in the column are returned unchanged.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column strip_null_value(Column col) {
    return new Column(com.snowflake.snowpark.functions.strip_null_value(col.toScalaColumn()));
  }

  /**
   * Returns the input values, pivoted into an ARRAY. If the input is empty, an empty ARRAY is
   * returned.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column array_agg(Column col) {
    return new Column(com.snowflake.snowpark.functions.array_agg(col.toScalaColumn()));
  }

  /**
   * Returns an ARRAY containing all elements from the source ARRAYas well as the new element. The
   * new element is located at end of the ARRAY.
   *
   * @param array The column containing the source ARRAY.
   * @param element The column containing the element to be appended. The element may be of almost
   *     any data type. The data type does not need to match the data type(s) of the existing
   *     elements in the ARRAY.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_append(Column array, Column element) {
    return new Column(
        com.snowflake.snowpark.functions.array_append(
            array.toScalaColumn(), element.toScalaColumn()));
  }

  /**
   * Returns the concatenation of two ARRAYs.
   *
   * @param array1 Column containing the source ARRAY.
   * @param array2 Column containing the ARRAY to be appended to {@code array1}.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_cat(Column array1, Column array2) {
    return new Column(
        com.snowflake.snowpark.functions.array_cat(array1.toScalaColumn(), array2.toScalaColumn()));
  }

  /**
   * Returns a compacted ARRAY with missing and null values removed, effectively converting sparse
   * arrays into dense arrays.
   *
   * @since 0.12.0
   * @param array The input array
   * @return The result column
   */
  public static Column array_compact(Column array) {
    return new Column(com.snowflake.snowpark.functions.array_compact(array.toScalaColumn()));
  }

  /**
   * Returns an ARRAY constructed from zero, one, or more inputs.
   *
   * @param cols Columns containing the values (or expressions that evaluate to values). The values
   *     do not all need to be of the same data type.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_construct(Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.array_construct(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns an ARRAY constructed from zero, one, or more inputs; the constructed ARRAY omits any
   * NULL input values.
   *
   * @param cols Columns containing the values (or expressions that evaluate to values). The values
   *     do not all need to be of the same data type.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_construct_compact(Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.array_construct_compact(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(cols))));
  }

  /**
   * Returns {@code true} if the specified VARIANT is found in the specified ARRAY.
   *
   * @param variant Column containing the VARIANT to find.
   * @param array Column containing the ARRAY to search.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_contains(Column variant, Column array) {
    return new Column(
        com.snowflake.snowpark.functions.array_contains(
            variant.toScalaColumn(), array.toScalaColumn()));
  }

  /**
   * Returns an ARRAY containing all elements from the source ARRAY as well as the new element.
   *
   * @param array Column containing the source ARRAY.
   * @param pos Column containing a (zero-based) position in the source ARRAY. The new element is
   *     inserted at this position. The original element from this position (if any) and all
   *     subsequent elements (if any) are shifted by one position to the right in the resulting
   *     array (i.e. inserting at position 0 has the same effect as using [[array_prepend]]). A
   *     negative position is interpreted as an index from the back of the array (e.g. {@code -1}
   *     results in insertion before the last element in the array).
   * @param element Column containing the element to be inserted. The new element is located at
   *     position {@code pos}. The relative order of the other elements from the source array is
   *     preserved.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_insert(Column array, Column pos, Column element) {
    return new Column(
        com.snowflake.snowpark.functions.array_insert(
            array.toScalaColumn(), pos.toScalaColumn(), element.toScalaColumn()));
  }

  /**
   * Returns the index of the first occurrence of an element in an ARRAY.
   *
   * @param variant Column containing the VARIANT value that you want to find. The function searches
   *     for the first occurrence of this value in the array.
   * @param array Column containing the ARRAY to be searched.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_position(Column variant, Column array) {
    return new Column(
        com.snowflake.snowpark.functions.array_position(
            variant.toScalaColumn(), array.toScalaColumn()));
  }

  /**
   * Returns an ARRAY containing the new element as well as all elements from the source ARRAY. The
   * new element is positioned at the beginning of the ARRAY.
   *
   * @param array Column containing the source ARRAY.
   * @param element Column containing the element to be prepended.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_prepend(Column array, Column element) {
    return new Column(
        com.snowflake.snowpark.functions.array_prepend(
            array.toScalaColumn(), element.toScalaColumn()));
  }

  /**
   * Returns the size of the input ARRAY.
   *
   * <p>If the specified column contains a VARIANT value that contains an ARRAY, the size of the
   * ARRAY is returned; otherwise, NULL is returned if the value is not an ARRAY.
   *
   * @since 0.12.0
   * @param array The input array
   * @return The result column
   */
  public static Column array_size(Column array) {
    return new Column(com.snowflake.snowpark.functions.array_size(array.toScalaColumn()));
  }

  /**
   * Returns an ARRAY constructed from a specified subset of elements of the input ARRAY.
   *
   * @param array Column containing the source ARRAY.
   * @param from Column containing a position in the source ARRAY. The position of the first element
   *     is {@code 0}. Elements from positions less than this parameter are not included in the
   *     resulting ARRAY.
   * @param to Column containing a position in the source ARRAY. Elements from positions equal to or
   *     greater than this parameter are not included in the resulting array.
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_slice(Column array, Column from, Column to) {
    return new Column(
        com.snowflake.snowpark.functions.array_slice(
            array.toScalaColumn(), from.toScalaColumn(), to.toScalaColumn()));
  }

  /**
   * Returns an input ARRAY converted to a string by casting all values to strings (using
   * TO_VARCHAR) and concatenating them (using the string from the second argument to separate the
   * elements).
   *
   * @param array Column containing the ARRAY of elements to convert to a string.
   * @param separator Column containing the string to put between each element (e.g. a space, comma,
   *     or other human-readable separator).
   * @since 0.12.0
   * @return The result column
   */
  public static Column array_to_string(Column array, Column separator) {
    return new Column(
        com.snowflake.snowpark.functions.array_to_string(
            array.toScalaColumn(), separator.toScalaColumn()));
  }

  /**
   * Returns one OBJECT per group. For each (key, value) input pair, where key must be a VARCHAR and
   * value must be a VARIANT, the resulting OBJECT contains a key:value field.
   *
   * @since 0.12.0
   * @param key The key
   * @param value The value
   * @return The result column
   */
  public static Column objectagg(Column key, Column value) {
    return new Column(
        com.snowflake.snowpark.functions.objectagg(key.toScalaColumn(), value.toScalaColumn()));
  }

  /**
   * Returns an OBJECT constructed from the arguments.
   *
   * @since 0.12.0
   * @param key_values The key and value
   * @return The result column
   */
  public static Column object_construct(Column... key_values) {
    return new Column(
        com.snowflake.snowpark.functions.object_construct(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(key_values))));
  }

  /**
   * Returns an object containing the contents of the input (i.e.source) object with one or more
   * keys removed.
   *
   * @since 0.12.0
   * @param obj The input object
   * @param key1 The key being removed
   * @param keys A list of keys being removed except key1
   * @return The result column
   */
  public static Column object_delete(Column obj, Column key1, Column... keys) {
    return new Column(
        com.snowflake.snowpark.functions.object_delete(
            obj.toScalaColumn(),
            key1.toScalaColumn(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(keys))));
  }

  /**
   * Returns an object consisting of the input object with a new key-value pair inserted. The input
   * key must not exist in the object.
   *
   * @since 0.12.0
   * @param obj The input object
   * @param key The key
   * @param value The value
   * @return The result column
   */
  public static Column object_insert(Column obj, Column key, Column value) {
    return new Column(
        com.snowflake.snowpark.functions.object_insert(
            obj.toScalaColumn(), key.toScalaColumn(), value.toScalaColumn()));
  }

  /**
   * Returns an object consisting of the input object with a new key-value pair inserted (or an
   * existing key updated with a new value).
   *
   * @since 0.12.0
   * @param obj The input object
   * @param key The key
   * @param value The value
   * @param update_flag The flags
   * @return The result column
   */
  public static Column object_insert(Column obj, Column key, Column value, Column update_flag) {
    return new Column(
        com.snowflake.snowpark.functions.object_insert(
            obj.toScalaColumn(),
            key.toScalaColumn(),
            value.toScalaColumn(),
            update_flag.toScalaColumn()));
  }

  /**
   * Returns a new OBJECT containing some of the key-value pairs from an existing object.
   *
   * <p>To identify the key-value pairs to include in the new object, pass in the keys as arguments,
   * or pass in an array containing the keys.
   *
   * <p>If a specified key is not present in the input object, the key is ignored.
   *
   * @since 0.12.0
   * @param obj The input object
   * @param key1 The key
   * @param keys A list of keys except key1
   * @return The result column
   */
  public static Column object_pick(Column obj, Column key1, Column... keys) {
    return new Column(
        com.snowflake.snowpark.functions.object_pick(
            obj.toScalaColumn(),
            key1.toScalaColumn(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(keys))));
  }

  /**
   * Casts a VARIANT value to an array.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_array(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_array(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a binary string.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_binary(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_binary(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a string. Does not convert values of other types into string.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_char(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_char(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a string. Does not convert values of other types into string.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_varchar(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_varchar(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a date. Does not convert from timestamps.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_date(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_date(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values).
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_decimal(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_decimal(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values), with
   * precision.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @param precision The precision
   * @return The result column
   */
  public static Column as_decimal(Column variant, int precision) {
    return new Column(
        com.snowflake.snowpark.functions.as_decimal(variant.toScalaColumn(), precision));
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values), with
   * precision and scale.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @param precision The precision
   * @param scale The scale
   * @return The result column
   */
  public static Column as_decimal(Column variant, int precision, int scale) {
    return new Column(
        com.snowflake.snowpark.functions.as_decimal(variant.toScalaColumn(), precision, scale));
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values).
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_number(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_number(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values), with
   * precision and scale.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @param precision The precision
   * @return The result column
   */
  public static Column as_number(Column variant, int precision) {
    return new Column(
        com.snowflake.snowpark.functions.as_number(variant.toScalaColumn(), precision));
  }

  /**
   * Casts a VARIANT value to a fixed-point decimal (does not match floating-point values), with
   * precision.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @param precision The precision
   * @param scale The scale
   * @return The result column
   */
  public static Column as_number(Column variant, int precision, int scale) {
    return new Column(
        com.snowflake.snowpark.functions.as_number(variant.toScalaColumn(), precision, scale));
  }

  /**
   * Casts a VARIANT value to a floating-point value.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_double(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_double(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a floating-point value.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_real(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_real(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to an integer. Does not match non-integer values.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_integer(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_integer(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to an object.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_object(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_object(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a time value. Does not convert from timestamps.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_time(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_time(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a TIMESTAMP value with local timezone.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_timestamp_ltz(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_timestamp_ltz(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a TIMESTAMP value with no timezone.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_timestamp_ntz(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_timestamp_ntz(variant.toScalaColumn()));
  }

  /**
   * Casts a VARIANT value to a TIMESTAMP value with timezone.
   *
   * @since 0.12.0
   * @param variant The input variant
   * @return The result column
   */
  public static Column as_timestamp_tz(Column variant) {
    return new Column(com.snowflake.snowpark.functions.as_timestamp_tz(variant.toScalaColumn()));
  }

  /**
   * Tokenizes the given string using the given set of delimiters and returns the tokens as an
   * array. If either parameter is a NULL, a NULL is returned. An empty array is returned if
   * tokenization produces no tokens.
   *
   * @since 0.12.0
   * @param array The input array
   * @return The result column
   */
  public static Column strtok_to_array(Column array) {
    return new Column(com.snowflake.snowpark.functions.strtok_to_array(array.toScalaColumn()));
  }

  /**
   * Tokenizes the given string using the given set of delimiters and returns the tokens as an
   * array. If either parameter is a NULL, a NULL is returned. An empty array is returned if
   * tokenization produces no tokens.
   *
   * @since 0.12.0
   * @param array The input array
   * @param delimiter The delimiter
   * @return The result column
   */
  public static Column strtok_to_array(Column array, Column delimiter) {
    return new Column(
        com.snowflake.snowpark.functions.strtok_to_array(
            array.toScalaColumn(), delimiter.toScalaColumn()));
  }

  /**
   * Converts the input expression into an array:
   *
   * <p>If the input is an ARRAY, or VARIANT containing an array value, the result is unchanged. For
   * NULL or a JSON null input, returns NULL. For any other value, the result is a single-element
   * array containing this value.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column to_array(Column col) {
    return new Column(com.snowflake.snowpark.functions.to_array(col.toScalaColumn()));
  }

  /**
   * Converts any VARIANT value to a string containing the JSON representation of the value. If the
   * input is NULL, the result is also NULL.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column to_json(Column col) {
    return new Column(com.snowflake.snowpark.functions.to_json(col.toScalaColumn()));
  }

  /**
   * Converts the input value to an object:
   *
   * <p>For a variant value containing an object, returns this object (in a value of type OBJECT).
   * For a variant value containing JSON null or for NULL input, returns NULL. For all other input
   * values, reports an error.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column to_object(Column col) {
    return new Column(com.snowflake.snowpark.functions.to_object(col.toScalaColumn()));
  }

  /**
   * Converts any value to VARIANT value or NULL (if input is NULL).
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column to_variant(Column col) {
    return new Column(com.snowflake.snowpark.functions.to_variant(col.toScalaColumn()));
  }

  /**
   * Converts any VARIANT value to a string containing the XML representation of the value. If the
   * input is NULL, the result is also NULL.
   *
   * @since 0.12.0
   * @param col The input value
   * @return The result column
   */
  public static Column to_xml(Column col) {
    return new Column(com.snowflake.snowpark.functions.to_xml(col.toScalaColumn()));
  }

  /**
   * Extracts a value from an object or array; returns NULL if either of the arguments is NULL.
   *
   * @since 0.12.0
   * @param col1 The first input value
   * @param col2 The second input value
   * @return The result column
   */
  public static Column get(Column col1, Column col2) {
    return new Column(
        com.snowflake.snowpark.functions.get(col1.toScalaColumn(), col2.toScalaColumn()));
  }

  /**
   * Extracts a field value from an object; returns NULL if either of the arguments is NULL. This
   * function is similar to GET but applies case-insensitive matching to field names.
   *
   * @since 0.12.0
   * @param obj The input object
   * @param field The field
   * @return The result column
   */
  public static Column get_ignore_case(Column obj, Column field) {
    return new Column(
        com.snowflake.snowpark.functions.get_ignore_case(
            obj.toScalaColumn(), field.toScalaColumn()));
  }

  /**
   * Returns an array containing the list of keys in the input object.
   *
   * @since 0.12.0
   * @param obj The input object
   * @return The result column
   */
  public static Column object_keys(Column obj) {
    return new Column(com.snowflake.snowpark.functions.object_keys(obj.toScalaColumn()));
  }

  /**
   * Extracts an XML element object (often referred to as simply a tag) from a content of outer XML
   * element object by the name of the tag and its instance number (counting from 0).
   *
   * @since 0.12.0
   * @param xml The input xml
   * @param tag The tag
   * @param instance The instance
   * @return The result column
   */
  public static Column xmlget(Column xml, Column tag, Column instance) {
    return new Column(
        com.snowflake.snowpark.functions.xmlget(
            xml.toScalaColumn(), tag.toScalaColumn(), instance.toScalaColumn()));
  }

  /**
   * Extracts the first XML element object (often referred to as simply a tag) from a content of
   * outer XML element object by the name of the tag
   *
   * @since 0.12.0
   * @param xml The input xml
   * @param tag The tag
   * @return The result column
   */
  public static Column xmlget(Column xml, Column tag) {
    return new Column(
        com.snowflake.snowpark.functions.xmlget(xml.toScalaColumn(), tag.toScalaColumn()));
  }

  /**
   * Extracts a value from semi-structured data using a path name.
   *
   * @since 0.12.0
   * @param col The input value
   * @param path The path
   * @return The result column
   */
  public static Column get_path(Column col, Column path) {
    return new Column(
        com.snowflake.snowpark.functions.get_path(col.toScalaColumn(), path.toScalaColumn()));
  }

  /**
   * Works like a cascading if-then-else statement. A series of conditions are evaluated in
   * sequence. When a condition evaluates to TRUE, the evaluation stops and the associated result
   * (after THEN) is returned. If none of the conditions evaluate to TRUE, then the result after the
   * optional OTHERWISE is returned, if present; otherwise NULL is returned. For Example:
   *
   * <pre>{@code
   * import com.snowflake.snowpark_java.Functions;
   * df.select(Functions
   *     .when(df.col("col").is_null, Functions.lit(1))
   *     .when(df.col("col").equal_to(Functions.lit(1)), Functions.lit(6))
   *     .otherwise(Functions.lit(7)));
   * }</pre>
   *
   * @since 0.12.0
   * @param condition The condition
   * @param value The result value
   * @return The result column
   */
  public static CaseExpr when(Column condition, Column value) {
    return new CaseExpr(
        com.snowflake.snowpark.functions.when(condition.toScalaColumn(), value.toScalaColumn()));
  }

  /**
   * Returns one of two specified expressions, depending on a condition.
   *
   * <p>This is equivalent to an `if-then-else` expression. If `condition` evaluates to TRUE, the
   * function returns `expr1`. Otherwise, the function returns `expr2`.
   *
   * @param condition The condition to evaluate.
   * @param expr1 The expression to return if the condition evaluates to TRUE.
   * @param expr2 The expression to return if the condition is not TRUE (i.e. if it is FALSE or
   *     NULL).
   * @since 0.12.0
   * @return The result column
   */
  public static Column iff(Column condition, Column expr1, Column expr2) {
    return new Column(
        com.snowflake.snowpark.functions.iff(
            condition.toScalaColumn(), expr1.toScalaColumn(), expr2.toScalaColumn()));
  }

  /**
   * Returns a conditional expression that you can pass to the filter or where method to perform the
   * equivalent of a WHERE ... IN query that matches rows containing a sequence of values.
   *
   * <p>The expression evaluates to true if the values in a row matches the values in one of the
   * specified sequences.
   *
   * <p>For example, the following code returns a DataFrame that contains the rows in which the
   * columns `c1` and `c2` contain the values: - `1` and `"a"`, or - `2` and `"b"` This is
   * equivalent to `SELECT * FROM table WHERE (c1, c2) IN ((1, 'a'), (2, 'b'))`.
   *
   * <pre><code>
   * import com.snowflake.snowpark_java.Functions;
   * df.filter(Functions.in(new Column[]{df.col("c1"), df.col("c2")},
   *   Arrays.asList(Array.asList(1, "a"), Array.asList(2, "b"))));
   * </code></pre>
   *
   * @param columns A sequence of the columns to compare for the IN operation.
   * @param values A sequence containing the sequences of values to compare for the IN operation.
   * @since 0.12.0
   * @return The result column
   */
  public static Column in(Column[] columns, List<List<Object>> values) {
    return new Column(
        com.snowflake.snowpark.functions.in(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(columns)),
            JavaUtils.objectListToAnySeq(values)));
  }

  /**
   * Returns a conditional expression that you can pass to the filter or where method to perform the
   * equivalent of a WHERE ... IN query with the subquery represented by the specified DataFrame.
   *
   * <p>The expression evaluates to true if the value in the column is one of the values in the
   * column of the same name in a specified DataFrame.
   *
   * <p>For example, the following code returns a DataFrame that contains the rows where the values
   * of the columns `c1` and `c2` in `df2` match the values of the columns `a` and `b` in `df1`.
   * This is equivalent to SELECT * FROM table2 WHERE (c1, c2) IN (SELECT a, b FROM table1).
   *
   * <pre><code>
   * import com.snowflake.snowpark_java.Functions;
   * import com.snowflake.snowpark_java.DataFrame;
   * DataFrame df1 = session.sql("select a, b from table1");
   * DataFrame df2 = session.table("table2");
   * df2.filter(Functions.in(new Column[]{df2.col("c1"), df2.col("c2")}, df1));
   * </code></pre>
   *
   * @param columns A sequence of the columns to compare for the IN operation.
   * @param df The DataFrame used as the values for the IN operation
   * @since 0.12.0
   * @return The result column
   */
  public static Column in(Column[] columns, DataFrame df) {
    return new Column(
        com.snowflake.snowpark.functions.in(
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(columns)),
            df.getScalaDataFrame()));
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 1 byte. the sequence continues at
   * 0 after wrap-around.
   *
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq1() {
    return new Column(com.snowflake.snowpark.functions.seq1());
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 1 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around, otherwise,
   *     continues at the smallest representable number based on the given integer width.
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq1(boolean startsFromZero) {
    return new Column(com.snowflake.snowpark.functions.seq1(startsFromZero));
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 2 byte. the sequence continues at
   * 0 after wrap-around.
   *
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq2() {
    return new Column(com.snowflake.snowpark.functions.seq2());
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 2 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around, otherwise,
   *     continues at the smallest representable number based on the given integer width.
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq2(boolean startsFromZero) {
    return new Column(com.snowflake.snowpark.functions.seq2(startsFromZero));
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 4 byte. the sequence continues at
   * 0 after wrap-around.
   *
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq4() {
    return new Column(com.snowflake.snowpark.functions.seq4());
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 4 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around, otherwise,
   *     continues at the smallest representable number based on the given integer width.
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq4(boolean startsFromZero) {
    return new Column(com.snowflake.snowpark.functions.seq4(startsFromZero));
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 8 byte. the sequence continues at
   * 0 after wrap-around.
   *
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq8() {
    return new Column(com.snowflake.snowpark.functions.seq8());
  }

  /**
   * Generates a sequence of monotonically increasing integers, with wrap-around. Wrap-around occurs
   * after the largest representable integer of the integer width 8 byte.
   *
   * @param startsFromZero if true, the sequence continues at 0 after wrap-around, otherwise,
   *     continues at the smallest representable number based on the given integer width.
   * @since 0.12.0
   * @return The result column
   */
  public static Column seq8(boolean startsFromZero) {
    return new Column(com.snowflake.snowpark.functions.seq8(startsFromZero));
  }

  /**
   * Returns a uniformly random number, in the inclusive range (`min`, `max`)
   *
   * <p>For example: {{{ import com.snowflake.snowpark_java.Functions; session.generator(10,
   * Functions.seq4(), Functions.uniform(Functions.lit(1), Functions.lit(5),
   * Functions.random())).show() }}}
   *
   * @param min The lower bound
   * @param max The upper bound
   * @param gen The generator expression for the function. for more information, see <a href =
   *     "https://docs.snowflake.com/en/sql-reference/functions-data-generation.html#label-rand-dist-functions">
   *     here</a>
   * @since 0.12.0
   * @return The result column
   */
  public static Column uniform(Column min, Column max, Column gen) {
    return new Column(
        com.snowflake.snowpark.functions.uniform(
            min.toScalaColumn(), max.toScalaColumn(), gen.toScalaColumn()));
  }

  /**
   * Returns the concatenated input values, separated by `delimiter` string.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.groupBy(df.col("col1")).agg(Functions.listagg(df.col("col2"), ",")
   *      .withinGroup(df.col("col2").asc()))
   *
   *
   * df.select(Functions.listagg(df.col("col2"), ",", false))
   * }</pre>
   *
   * @param col The expression (typically a Column) that determines the values to be put into the
   *     list. The expression should evaluate to a string, or to a data type that can be cast to
   *     string.
   * @param delimiter A string delimiter.
   * @param isDistinct Whether the input expression is distinct.
   * @since 1.1.0
   * @return The result Column
   */
  public static Column listagg(Column col, String delimiter, boolean isDistinct) {
    return new Column(
        com.snowflake.snowpark.functions.listagg(col.toScalaColumn(), delimiter, isDistinct));
  }

  /**
   * Returns the concatenated input values, separated by `delimiter` string.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.groupBy(df.col("col1")).agg(Functions.listagg(df.col("col2"), ",")
   *      .withinGroup(df.col("col2").asc()))
   *
   *
   * df.select(Functions.listagg(df.col("col2"), ",", false))
   * }</pre>
   *
   * @param col The expression (typically a Column) that determines the values to be put into the
   *     list. The expression should evaluate to a string, or to a data type that can be cast to
   *     string.
   * @param delimiter A string delimiter.
   * @since 1.1.0
   * @return The result Column
   */
  public static Column listagg(Column col, String delimiter) {
    return new Column(com.snowflake.snowpark.functions.listagg(col.toScalaColumn(), delimiter));
  }

  /**
   * Returns the concatenated input values, separated by empty string.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.groupBy(df.col("col1")).agg(Functions.listagg(df.col("col2"), ",")
   *      .withinGroup(df.col("col2").asc()))
   *
   *
   * df.select(Functions.listagg(df.col("col2"), ",", false))
   * }</pre>
   *
   * @param col The expression (typically a Column) that determines the values to be put into the
   *     list. The expression should evaluate to a string, or to a data type that can be cast to
   *     string.
   * @since 1.1.0
   * @return The result Column
   */
  public static Column listagg(Column col) {
    return new Column(com.snowflake.snowpark.functions.listagg(col.toScalaColumn()));
  }

  /**
   * Calls a user-defined function (UDF) by name.
   *
   * @since 0.12.0
   * @param udfName The name of UDF
   * @param cols The list of parameters
   * @return The result column
   */
  public static Column callUDF(String udfName, Column... cols) {
    return new Column(
        com.snowflake.snowpark.functions.callUDF(
            udfName, JavaUtils.columnArrayToAnySeq(Column.toScalaColumnArray(cols))));
  }

  //  Code below for udf 0-22 generated by this script

  //  (0 to 22).foreach { x =>
  //    val types = (1 to x).foldLeft("?")((i, _) => {s"$i, ?"})
  //    val (input, doc) = x match {
  //      case 0 => ("", "")
  //      case 1 => (" DataType input,", "@param input the UDF input {@code types.DataType}")
  //      case _ => (" DataType[] input,", "@param input the UDF input {@code types.DataType}s")
  //    }
  //    val s = if(x > 1) "s" else ""
  //    println(s"""
  //    |/**
  //    |* Registers a Java Lambda of $x argument$s as a Snowflake UDF and returns the UDF.
  //    |*
  //    |* @since 0.12.0
  //    |* @param func the Java lambda to be registered
  //    |* $doc
  //    |* @param output the UDF return {@code types.DataType}
  //    |* @return The result UserDefinedFunction reference
  //    |*/
  //    |public static UserDefinedFunction udf(JavaUDF$x<$types> func,$input DataType output) {
  //    |  return getActiveSession().udf()
  //    |    .registerTemporary(func,${if(x == 0) "" else " input,"} output);
  //    |}
  //    |""".stripMargin)
  //  }

  /**
   * Registers a Java Lambda of 0 argument as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(JavaUDF0<?> func, DataType output) {
    return getActiveSession().udf().registerTemporary(func, output);
  }

  /**
   * Registers a Java Lambda of 1 argument as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(JavaUDF1<?, ?> func, DataType input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 2 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(JavaUDF2<?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 3 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF3<?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 4 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF4<?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 5 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF5<?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 6 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF6<?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 7 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF7<?, ?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 8 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF8<?, ?, ?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 9 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF9<?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 10 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF10<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 11 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF11<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 12 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF12<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 13 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF13<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func, DataType[] input, DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 14 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF14<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 15 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF15<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 16 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF16<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 17 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF17<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 18 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF18<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 19 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF19<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 20 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF20<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 21 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF21<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  /**
   * Registers a Java Lambda of 22 arguments as a Snowflake UDF and returns the UDF.
   *
   * @since 0.12.0
   * @param func the Java lambda to be registered
   * @param input the UDF input {@code types.DataType}s
   * @param output the UDF return {@code types.DataType}
   * @return The result UserDefinedFunction reference
   */
  public static UserDefinedFunction udf(
      JavaUDF22<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> func,
      DataType[] input,
      DataType output) {
    return getActiveSession().udf().registerTemporary(func, input, output);
  }

  private static Session getActiveSession() {
    return new Session(JavaUtils.getActiveSession());
  }
}
