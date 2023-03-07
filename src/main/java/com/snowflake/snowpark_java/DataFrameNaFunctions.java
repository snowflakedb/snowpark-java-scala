package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * Provides functions for handling missing values in a DataFrame.
 *
 * @since 1.1.0
 */
public class DataFrameNaFunctions {
  private final com.snowflake.snowpark.DataFrameNaFunctions func;

  DataFrameNaFunctions(com.snowflake.snowpark.DataFrameNaFunctions func) {
    this.func = func;
  }

  /**
   * Returns a new DataFrame that excludes all rows containing fewer than {@code minNonNullsPerRow}
   * non-null and non-NaN values in the specified columns {@code cols}.
   *
   * <p>If {@code minNonNullsPerRow} is greater than the number of the specified columns, the method
   * returns an empty DataFrame. If {@code minNonNullsPerRow} is less than 1, the method returns the
   * original DataFrame. If {@code cols} is empty, the method returns the original DataFrame.
   *
   * @param minNonNullsPerRow The minimum number of non-null and non-NaN values that should be in
   *     the specified columns in order for the row to be included.
   * @param cols A sequence of the names of columns to check for null and NaN values.
   * @return A {@code DataFrame}
   * @since 1.1.0
   */
  public DataFrame drop(int minNonNullsPerRow, String[] cols) {
    return new DataFrame(this.func.drop(minNonNullsPerRow, JavaUtils.stringArrayToStringSeq(cols)));
  }

  /**
   * Returns a new DataFrame that replaces all null and NaN values in the specified columns with the
   * values provided.
   *
   * <p>{@code valueMap} describes which columns will be replaced and what the replacement values
   * are.
   *
   * <p>It only supports Long, Int, short, byte, String, Boolean, float, and Double values. If the
   * type of the given value doesn't match the column type (e.g. a Long value for a StringType
   * column), the replacement in this column will be skipped.
   *
   * @param valueMap A Map that associates the names of columns with the values that should be used
   *     to replace null and NaN values in those columns.
   * @return A DataFrame
   * @since 1.1.0
   */
  public DataFrame fill(Map<String, ?> valueMap) {
    return new DataFrame(JavaUtils.fill(valueMap, this.func));
  }

  /**
   * Returns a new DataFrame that replaces values in a specified column.
   *
   * <p>Use the {@code replacement} parameter to specify a Map that associates the values to replace
   * with new values.
   *
   * <p>For example, suppose that you pass `col1` for {@code colName} and {@code Map(2 -> 3, None ->
   * 2, 4 -> null)} for {@code replacement}. In `col1`, this function replaces: `2` with `3`, null
   * with `2`, `4` with null.
   *
   * @param colName The name of the column in which the values should be replaced.
   * @param replacement A Map that associates the original values with the replacement values.
   * @since 1.1.0
   * @return The result DataFrame
   */
  public DataFrame replace(String colName, Map<?, ?> replacement) {
    return new DataFrame(JavaUtils.replacement(colName, replacement, this.func));
  }
}
