package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{ErrorMessage, Logging, OpenTelemetry}
import com.snowflake.snowpark.functions.{
  approx_percentile_accumulate,
  approx_percentile_estimate,
  count,
  covar_samp,
  col => Col,
  corr => corr_func
}

/** Provides eagerly computed statistical functions for DataFrames.
  *
  * To access an object of this class, use [[DataFrame.stat]].
  *
  * @since 0.2.0
  */
final class DataFrameStatFunctions private[snowpark] (df: DataFrame) extends Logging {

  // Used as temporary column name in approxQuantile
  // Will not conflict with column names in user's dataframe
  private val tempColumnName = "t"

  // crosstab execution time: 1000 -> 25s, 3000 -> 2.5 min, 5000 -> 10 min.
  private val maxColumnsPerTable = 1000

  /** Calculates the correlation coefficient for non-null pairs in two numeric columns.
    *
    * For example, the following code:
    * {{{
    *   import session.implicits._
    *   val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    *   double res = df.stat.corr("a", "b").get
    * }}}
    *
    * prints out the following result:
    * {{{
    *   res: 0.9999999999999991
    * }}}
    *
    * @param col1
    *   The name of the first numeric column to use.
    * @param col2
    *   The name of the second numeric column to use.
    * @since 0.2.0
    * @return
    *   The correlation of the two numeric columns. If there is not enough data to generate the
    *   correlation, the method returns None.
    */
  def corr(col1: String, col2: String): Option[Double] = action("corr") {
    val res = df.select(corr_func(Col(col1), Col(col2))).limit(1).collect().head
    if (res.isNullAt(0)) None else Some(res.getDouble(0))
  }

  /** Calculates the sample covariance for non-null pairs in two numeric columns.
    *
    * For example, the following code:
    * {{{
    *   import session.implicits._
    *   val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    *   double res = df.stat.cov("a", "b").get
    * }}}
    *
    * prints out the following result:
    * {{{
    *   res: 0.010000000000000037
    * }}}
    *
    * @param col1
    *   The name of the first numeric column to use.
    * @param col2
    *   The name of the second numeric column to use.
    * @since 0.2.0
    * @return
    *   The sample covariance of the two numeric columns, If there is not enough data to generate
    *   the covariance, the method returns None.
    */
  def cov(col1: String, col2: String): Option[Double] = action("cov") {
    val res = df.select(covar_samp(Col(col1), Col(col2))).limit(1).collect().head
    if (res.isNullAt(0)) None else Some(res.getDouble(0))
  }

  /** For a specified numeric column and an array of desired quantiles, returns an approximate value
    * for the column at each of the desired quantiles.
    *
    * This function uses the t-Digest algorithm.
    *
    * For example, the following code:
    * {{{
    *   import session.implicits._
    *   val df = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 0).toDF("a")
    *   val res = df.stat.approxQuantile("a", Array(0, 0.1, 0.4, 0.6, 1))
    * }}}
    *
    * prints out the following result:
    * {{{
    *   res: Array(Some(-0.5), Some(0.5), Some(3.5), Some(5.5), Some(9.5))
    * }}}
    *
    * @param col
    *   The name of the numeric column.
    * @param percentile
    *   An array of double values greater than or equal to 0.0 and less than 1.0.
    * @since 0.2.0
    * @return
    *   An array of approximate percentile values, If there is not enough data to calculate the
    *   quantile, the method returns None.
    */
  def approxQuantile(col: String, percentile: Array[Double]): Array[Option[Double]] =
    action("approxQuantile") {
      if (percentile.isEmpty) {
        return Array[Option[Double]]()
      }
      val res = df
        .select(approx_percentile_accumulate(Col(col)).as(tempColumnName))
        .select(percentile.map(p => approx_percentile_estimate(Col(tempColumnName), p)))
        .limit(1)
        .collect()
        .head
      res.toSeq.map {
        case d: Double => Some(d)
        case _ => None
      }.toArray
    }

  /** For an array of numeric columns and an array of desired quantiles, returns a matrix of
    * approximate values for each column at each of the desired quantiles. For example,
    * `result(0)(1)` contains the approximate value for column `cols(0)` at quantile
    * `percentile(1)`.
    *
    * This function uses the t-Digest algorithm.
    *
    * For example, the following code:
    * {{{
    *   import session.implicits._
    *   val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    *   val res = double2.stat.approxQuantile(Array("a", "b"), Array(0, 0.1, 0.6))
    * }}}
    *
    * prints out the following result:
    * {{{
    *   res: Array(Array(Some(0.05), Some(0.15000000000000002), Some(0.25)),
    *              Array(Some(0.45), Some(0.55), Some(0.6499999999999999)))
    * }}}
    *
    * @param cols
    *   An array of column names.
    * @param percentile
    *   An array of double values greater than or equal to 0.0 and less than 1.0.
    * @since 0.2.0
    * @return
    *   A matrix with the dimensions `(cols.size * percentile.size)` containing the approximate
    *   percentile values. If there is not enough data to calculate the quantile, the method returns
    *   None.
    */
  def approxQuantile(cols: Array[String], percentile: Array[Double]): Array[Array[Option[Double]]] =
    action("approxQuantile") {
      if (cols.isEmpty || percentile.isEmpty) {
        return Array[Array[Option[Double]]]()
      }
      // Apply approx_percentile_accumulate function to each input column, them rename the generated
      // temporary column as t1, t2 ...
      val tempColumns = cols.zipWithIndex.map { case (c, i) =>
        approx_percentile_accumulate(Col(c)).as(tempColumnName + i)
      }

      // Apply approx_percentile_estimate to all (percentile, temp column) pairs:
      // (p1, t1), (p2, t1) ... (p_percentile.size, t_col.size)
      val outputColumns = Array
        .range(0, cols.length)
        .map { i =>
          percentile.map(p => approx_percentile_estimate(Col(tempColumnName + i), p))
        }
        .flatMap(_.toList)
      val res = df.select(tempColumns).select(outputColumns).limit(1).collect().head

      // First map Any to Option[Double], then convert Array to matrix
      res.toSeq
        .map {
          case d: Double => Some(d)
          case _ => None
        }
        .toArray
        .grouped(percentile.length)
        .toArray
    }

  /** Computes a pair-wise frequency table (a ''contingency table'') for the specified columns. The
    * method returns a DataFrame containing this table.
    *
    * In the returned contingency table:
    *
    *   - The first column of each row contains the distinct values of {@code col1} .
    *   - The name of the first column is the name of {@code col1} .
    *   - The rest of the column names are the distinct values of {@code col2} .
    *   - The counts are returned as Longs.
    *   - For pairs that have no occurrences, the contingency table contains 0 as the count.
    *
    * Note: The number of distinct values in {@code col2} should not exceed 1000.
    *
    * For example, the following code:
    * {{{
    *   import session.implicits._
    *   val df = Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3)).toDF("key", "value")
    *   val ct = df.stat.crosstab("key", "value")
    *   ct.show()
    * }}}
    *
    * prints out the following result:
    * {{{
    *   ---------------------------------------------------------------------------------------------
    *   |"KEY"  |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |"CAST(3 AS NUMBER(38,0))"  |
    *   ---------------------------------------------------------------------------------------------
    *   |1      |1                          |1                          |0                          |
    *   |2      |2                          |0                          |1                          |
    *   |3      |0                          |1                          |1                          |
    *   ---------------------------------------------------------------------------------------------
    * }}}
    *
    * @param col1
    *   The name of the first column to use.
    * @param col2
    *   The name of the second column to use.
    * @since 0.2.0
    * @return
    *   A DataFrame containing the contingency table.
    */
  def crosstab(col1: String, col2: String): DataFrame = action("crosstab") {
    // Limit the distinct values of col2 to maxColumnsPerTable.
    val rowCount =
      df.select(col2).distinct().select(count(Col(col2))).limit(1).collect().head.getLong(0)
    if (rowCount > maxColumnsPerTable) {
      throw ErrorMessage.DF_CROSS_TAB_COUNT_TOO_LARGE(rowCount, maxColumnsPerTable)
    }

    // Column name have a maximum limit of 255 characters. Server will check this and throw
    // meaningful error if value in this Array exceeds that.
    val columnNames =
      df.select(col2).distinct().limit(maxColumnsPerTable).collect().map(row => row.get(0))
    df.select(col1, col2).pivot(col2, columnNames).agg(count(Col(col2)))
  }

  /** Returns a DataFrame containing a stratified sample without replacement, based on a Map that
    * specifies the fraction for each stratum.
    *
    * For example, the following code:
    * {{{
    *   import session.implicits._
    *   val df = Seq(("Bob", 17), ("Alice", 10), ("Nico", 8), ("Bob", 12)).toDF("name", "age")
    *   val fractions = Map("Bob" -> 0.5, "Nico" -> 1.0)
    *   df.stat.sampleBy(col("name"), fractions).show()
    * }}}
    *
    * prints out the following result:
    * {{{
    *   ------------------
    *   |"NAME"  |"AGE"  |
    *   ------------------
    *   |Bob     |17     |
    *   |Nico    |8      |
    *   ------------------
    * }}}
    *
    * @param col
    *   An expression for the column that defines the strata.
    * @param fractions
    *   A Map that specifies the fraction to use for the sample for each stratum. If a stratum is
    *   not specified in the Map, the method uses 0 as the fraction.
    * @tparam T
    *   The type of the stratum.
    * @since 0.2.0
    * @return
    *   A new DataFrame that contains the stratified sample.
    */
  def sampleBy[T](col: Column, fractions: Map[T, Double]): DataFrame =
    transformation("sampleBy") {
      if (fractions.isEmpty) {
        return df.limit(0)
      }
      val (k, v) = fractions.head
      var resDF = df.where(col === k).sample(v)
      for ((k, v) <- fractions.tail) {
        resDF = resDF.unionAll(df.where(col === k).sample(v))
      }
      resDF
    }

  /** Returns a DataFrame containing a stratified sample without replacement, based on a Map that
    * specifies the fraction for each stratum.
    *
    * For example, the following code:
    * {{{
    *   import session.implicits._
    *   val df = Seq(("Bob", 17), ("Alice", 10), ("Nico", 8), ("Bob", 12)).toDF("name", "age")
    *   val fractions = Map("Bob" -> 0.5, "Nico" -> 1.0)
    *   df.stat.sampleBy("name", fractions).show()
    * }}}
    *
    * prints out the following result:
    * {{{
    *   ------------------
    *   |"NAME"  |"AGE"  |
    *   ------------------
    *   |Bob     |17     |
    *   |Nico    |8      |
    *   ------------------
    * }}}
    *
    * @param col
    *   The name of the column that defines the strata.
    * @param fractions
    *   A Map that specifies the fraction to use for the sample for each stratum. If a stratum is
    *   not specified in the Map, the method uses 0 as the fraction.
    * @tparam T
    *   The type of the stratum.
    * @since 0.2.0
    * @return
    *   A new DataFrame that contains the stratified sample.
    */
  def sampleBy[T](col: String, fractions: Map[T, Double]): DataFrame =
    transformation("sampleBy") {
      sampleBy(Col(col), fractions)
    }

  @inline protected def action[T](funcName: String)(func: => T): T = {
    OpenTelemetry.action("DataFrameStatFunctions", funcName, df.methodChainString + ".stat")(func)
  }
  @inline protected def transformation(funcName: String)(func: => DataFrame): DataFrame =
    DataFrame.buildMethodChain(this.df.methodChain :+ "stat", funcName)(func)
}
