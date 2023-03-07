package com.snowflake.snowpark_java;

/**
 * Result of deleting rows in an Updatable
 *
 * @since 1.1.0
 */
public class DeleteResult {
  private final com.snowflake.snowpark.DeleteResult result;

  DeleteResult(com.snowflake.snowpark.DeleteResult result) {
    this.result = result;
  }

  /**
   * Retrieves the number of rows have deleted.
   *
   * @return a long number
   * @since 1.1.0
   */
  public long getRowsDeleted() {
    return this.result.rowsDeleted();
  }
}
