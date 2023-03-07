package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;
import java.util.Optional;

/**
 * Provides eagerly computed statistical functions for DataFrames.
 *
 * <p>To access an object of this class, use {@code DataFrame.stat()}.
 *
 * @since 1.1.0
 */
public class DataFrameStatFunctions {
  private final com.snowflake.snowpark.DataFrameStatFunctions func;

  DataFrameStatFunctions(com.snowflake.snowpark.DataFrameStatFunctions func) {
    this.func = func;
  }

  /**
   * Calculates the correlation coefficient for non-null pairs in two numeric columns.
   *
   * @param col1 The name of the first numeric column to use.
   * @param col2 The name of the second numeric column to use.
   * @since 1.1.0
   * @return The correlation of the two numeric columns. If there is not enough data to generate the
   *     correlation, the method returns None.
   */
  public Optional<Double> corr(String col1, String col2) {
    return toJavaOptional(func.corr(col1, col2));
  }

  /**
   * Calculates the sample covariance for non-null pairs in two numeric columns.
   *
   * @param col1 The name of the first numeric column to use.
   * @param col2 The name of the second numeric column to use.
   * @since 1.1.0
   * @return The sample covariance of the two numeric columns, If there is not enough data to
   *     generate the covariance, the method returns None.
   */
  public Optional<Double> cov(String col1, String col2) {
    return toJavaOptional(func.cov(col1, col2));
  }

  /**
   * For a specified numeric column and an array of desired quantiles, returns an approximate value
   * for the column at each of the desired quantiles.
   *
   * <p>This function uses the t-Digest algorithm.
   *
   * @param col The name of the numeric column.
   * @param percentile An array of double values greater than or equal to 0.0 and less than 1.0.
   * @since 1.1.0
   * @return An array of approximate percentile values, If there is not enough data to calculate the
   *     quantile, the method returns None.
   */
  public Optional<Double>[] approxQuantile(String col, double[] percentile) {
    scala.Option<Object>[] result = func.approxQuantile(col, percentile);
    Optional<Double>[] javaResult = new Optional[result.length];
    for (int i = 0; i < result.length; i++) {
      javaResult[i] = toJavaOptional(result[i]);
    }
    return javaResult;
  }

  /**
   * For an array of numeric columns and an array of desired quantiles, returns a matrix of
   * approximate values for each column at each of the desired quantiles. For example,
   * `result(0)(1)` contains the approximate value for column `cols(0)` at quantile `percentile(1)`.
   *
   * <p>This function uses the t-Digest algorithm.
   *
   * @param cols An array of column names.
   * @param percentile An array of double values greater than or equal to 0.0 and less than 1.0.
   * @since 1.1.0
   * @return A matrix with the dimensions `(cols.size * percentile.size)` containing the approximate
   *     percentile values. If there is not enough data to calculate the quantile, the method
   *     returns None.
   */
  public Optional<Double>[][] approxQuantile(String[] cols, double[] percentile) {
    scala.Option<Object>[][] result = func.approxQuantile(cols, percentile);
    Optional<Double>[][] javaResult = new Optional[result.length][];
    for (int i = 0; i < result.length; i++) {
      javaResult[i] = new Optional[result[i].length];
      for (int j = 0; j < result[i].length; j++) {
        javaResult[i][j] = toJavaOptional(result[i][j]);
      }
    }
    return javaResult;
  }

  /**
   * Computes a pair-wise frequency table (a ''contingency table'') for the specified columns. The
   * method returns a DataFrame containing this table.
   *
   * <p>In the returned contingency table:
   *
   * <p>- The first column of each row contains the distinct values of {@code col1}. - The name of
   * the first column is the name of {@code col1}. - The rest of the column names are the distinct
   * values of {@code col2}. - The counts are returned as Longs. - For pairs that have no
   * occurrences, the contingency table contains 0 as the count.
   *
   * <p>Note: The number of distinct values in {@code col2} should not exceed 1000.
   *
   * @param col1 The name of the first column to use.
   * @param col2 The name of the second column to use.
   * @since 1.1.0
   * @return A DataFrame containing the contingency table.
   */
  public DataFrame crosstab(String col1, String col2) {
    return new DataFrame(this.func.crosstab(col1, col2));
  }

  /**
   * Returns a DataFrame containing a stratified sample without replacement, based on a Map that
   * specifies the fraction for each stratum.
   *
   * @param col An expression for the column that defines the strata.
   * @param fractions A Map that specifies the fraction to use for the sample for each stratum. If a
   *     stratum is not specified in the Map, the method uses 0 as the fraction.
   * @since 1.1.0
   * @return A new DataFrame that contains the stratified sample.
   */
  public DataFrame sampleBy(Column col, Map<?, Double> fractions) {
    return new DataFrame(JavaUtils.sampleBy(col.toScalaColumn(), fractions, this.func));
  }

  /**
   * Returns a DataFrame containing a stratified sample without replacement, based on a Map that
   * specifies the fraction for each stratum.
   *
   * @param colName The name of the column that defines the strata.
   * @param fractions A Map that specifies the fraction to use for the sample for each stratum. If a
   *     stratum is not specified in the Map, the method uses 0 as the fraction.
   * @since 1.1.0
   * @return A new DataFrame that contains the stratified sample.
   */
  public DataFrame sampleBy(String colName, Map<?, Double> fractions) {
    return new DataFrame(JavaUtils.sampleBy(colName, fractions, this.func));
  }

  private static Optional<Double> toJavaOptional(scala.Option<Object> input) {
    if (input.isDefined()) {
      return Optional.of((Double) input.get());
    }
    return Optional.empty();
  }
}
