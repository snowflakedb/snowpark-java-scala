package com.snowflake.snowpark_java;

import java.util.Iterator;

/**
 * Provides APIs to execute DataFrame actions asynchronously.
 *
 * @since 1.2.0
 */
public class DataFrameAsyncActor {
  private final com.snowflake.snowpark.DataFrameAsyncActor dfAsync;
  protected final com.snowflake.snowpark.Session session;

  DataFrameAsyncActor(DataFrame df) {
    this.dfAsync = df.getScalaDataFrame().async();
    this.session = df.getScalaDataFrame().session();
  }

  /**
   * Executes {@code DataFrame.collect} asynchronously.
   *
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Row[]> collect() {
    return TypedAsyncJob.createRowArrayJob(dfAsync.collect(), session);
  }

  /**
   * Executes {@code DataFrame.toLocalIterator} asynchronously.
   *
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Iterator<Row>> toLocalIterator() {
    return TypedAsyncJob.createRowIteratorJob(dfAsync.toLocalIterator(), session);
  }

  /**
   * Executes {@code DataFrame.count} asynchronously.
   *
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Long> count() {
    return TypedAsyncJob.createLongJob(dfAsync.count(), session);
  }
}
