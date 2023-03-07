package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;

/**
 * Provides APIs to execute DataFrameWriter actions asynchronously.
 *
 * @since 1.2.0
 */
public class DataFrameWriterAsyncActor {
  private final com.snowflake.snowpark.DataFrameWriterAsyncActor asyncActor;
  private final com.snowflake.snowpark.Session session;

  DataFrameWriterAsyncActor(
      com.snowflake.snowpark.DataFrameWriterAsyncActor asyncActor,
      com.snowflake.snowpark.Session session) {
    this.asyncActor = asyncActor;
    this.session = session;
  }

  /**
   * Executes `DataFrameWriter.saveAsTable` asynchronously.
   *
   * @param tableName Name of the table where the data should be saved.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Void> saveAsTable(String tableName) {
    return TypedAsyncJob.createVoidJob(asyncActor.saveAsTable(tableName), session);
  }

  /**
   * Executes `DataFrameWriter.saveAsTable` asynchronously.
   *
   * @param multipartIdentifier An array of strings that specify the database name, schema name, and
   *     table name.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Void> saveAsTable(String[] multipartIdentifier) {
    return TypedAsyncJob.createVoidJob(
        asyncActor.saveAsTable(JavaUtils.stringArrayToStringSeq(multipartIdentifier)), session);
  }

  /**
   * Executes `DataFrameWriter.csv()` asynchronously.
   *
   * @param path The path (including the stage name) to the CSV file.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.5.0
   */
  public TypedAsyncJob<WriteFileResult> csv(String path) {
    return TypedAsyncJob.createWriteFileResultJob(asyncActor.csv(path), session);
  }

  /**
   * Executes `DataFrameWriter.json()` asynchronously.
   *
   * @param path The path (including the stage name) to the JSON file.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.5.0
   */
  public TypedAsyncJob<WriteFileResult> json(String path) {
    return TypedAsyncJob.createWriteFileResultJob(asyncActor.json(path), session);
  }

  /**
   * Executes `DataFrameWriter.parquet()` asynchronously.
   *
   * @param path The path (including the stage name) to the PARQUET file.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.5.0
   */
  public TypedAsyncJob<WriteFileResult> parquet(String path) {
    return TypedAsyncJob.createWriteFileResultJob(asyncActor.parquet(path), session);
  }
}
