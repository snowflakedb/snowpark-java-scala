package com.snowflake.snowpark_java;

/**
 * Result of updating rows in an Updatable
 *
 * @since 1.1.0
 */
public class UpdateResult {

  private final com.snowflake.snowpark.UpdateResult result;

  UpdateResult(com.snowflake.snowpark.UpdateResult result) {
    this.result = result;
  }

  /**
   * Retrieves the number of row has been updated.
   *
   * @since 1.1.0
   * @return a long number
   */
  public long getRowsUpdated() {
    return result.rowsUpdated();
  }

  /**
   * Retrieves the number of multi-joined row has been updated.
   *
   * @since 1.1.0
   * @return a long number
   */
  public long getMultiJoinedRowsUpdated() {
    return result.multiJoinedRowsUpdated();
  }
}
