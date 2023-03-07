package com.snowflake.snowpark_java;

/**
 * Provides APIs to execute MergeBuilder actions asynchronously.
 *
 * @since 1.3.0
 */
public class MergeBuilderAsyncActor {
  private final com.snowflake.snowpark.MergeBuilderAsyncActor mbAsync;
  private final com.snowflake.snowpark.Session session;

  MergeBuilderAsyncActor(
      com.snowflake.snowpark.MergeBuilderAsyncActor mbAsync,
      com.snowflake.snowpark.Session session) {
    this.mbAsync = mbAsync;
    this.session = session;
  }

  /**
   * Executes `MergeBuilder.collect()` asynchronously.
   *
   * @return A {@code TypedAsyncJob} object that you can use to check the status of the action and
   *     get the results.
   * @since 1.3.0
   */
  public TypedAsyncJob<MergeResult> collect() {
    return TypedAsyncJob.createMergeResultJob(mbAsync.collect(), session);
  }
}
