package com.snowflake.snowpark_java;

/**
 * Result of merging a DataFrame into an Updatable DataFrame
 *
 * @since 1.1.0
 */
public class MergeResult {
  private final com.snowflake.snowpark.MergeResult result;

  MergeResult(com.snowflake.snowpark.MergeResult result) {
    this.result = result;
  }

  /**
   * Retrieves the number of rows have been inserted.
   *
   * @return a long number
   * @since 1.1.0
   */
  public long getRowsInserted() {
    return this.result.rowsInserted();
  }

  /**
   * Retrieves the number of rows have been updated.
   *
   * @return a long number
   * @since 1.1.0
   */
  public long getRowsUpdated() {
    return this.result.rowsUpdated();
  }

  /**
   * Retrieves the number of rows have been deleted.
   *
   * @return a long number
   * @since 1.1.0
   */
  public long getRowsDeleted() {
    return this.result.rowsDeleted();
  }
}
