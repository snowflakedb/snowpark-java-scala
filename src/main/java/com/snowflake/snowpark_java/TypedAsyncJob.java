package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Iterator;

/**
 * Provides a way to track an asynchronously executed action in a DataFrame.
 *
 * <p>To get the result of the action (e.g. the number of results from a `count()` action or an
 * Array of Row objects from the `collect()` action), call the getResult method.
 *
 * <p>To perform an action on a DataFrame asynchronously, call an action method on the
 * DataFrameAsyncActor object returned by DataFrame.async.
 *
 * @since 1.2.0
 */
public class TypedAsyncJob<T> extends AsyncJob {
  private final com.snowflake.snowpark.TypedAsyncJob<?> typedJob;
  private final AsyncJobDataType type;

  private TypedAsyncJob(
      com.snowflake.snowpark.TypedAsyncJob<?> typedJob,
      com.snowflake.snowpark.Session session,
      AsyncJobDataType type) {
    super(typedJob, session);
    this.typedJob = typedJob;
    this.type = type;
  }

  /**
   * Returns the result for the specific DataFrame action.
   *
   * @since 1.2.0
   * @param maxWaitTimeInSeconds The maximum number of seconds to wait for the query to complete
   *     before attempting to retrieve the results.
   * @return The result
   */
  public T getResult(int maxWaitTimeInSeconds) {
    Object result = typedJob.getResult(maxWaitTimeInSeconds);
    return toJavaResult(result);
  }

  /**
   * Returns the result for the specific DataFrame action.
   *
   * <p>The max waiting time is the value of `snowpark_request_timeout_in_seconds` configuration
   * property.
   *
   * @since 1.2.0
   * @return The result
   */
  public T getResult() {
    return getResult(JavaUtils.session_requestTimeoutInSeconds(session));
  }

  private T toJavaResult(Object scalaResult) {
    switch (type) {
      case ArrayOfRow:
        assert scalaResult instanceof com.snowflake.snowpark.Row[];
        com.snowflake.snowpark.Row[] rows = (com.snowflake.snowpark.Row[]) scalaResult;
        Row[] javaResult = new Row[rows.length];
        for (int i = 0; i < javaResult.length; i++) {
          javaResult[i] = new Row(rows[i]);
        }
        return (T) javaResult;

      case IteratorOfRow:
        assert scalaResult instanceof scala.collection.Iterator;
        return (T)
            DataFrame.toJavaIterator(
                (scala.collection.Iterator<com.snowflake.snowpark.Row>) scalaResult);

      case Long:
        assert scalaResult instanceof Long;
        return (T) scalaResult;

      case Void:
        return (T) null; // return nothing

      case UpdateResult:
        assert scalaResult instanceof com.snowflake.snowpark.UpdateResult;
        return (T) new UpdateResult((com.snowflake.snowpark.UpdateResult) scalaResult);

      case DeleteResult:
        assert scalaResult instanceof com.snowflake.snowpark.DeleteResult;
        return (T) new DeleteResult((com.snowflake.snowpark.DeleteResult) scalaResult);

      case MergeResult:
        assert scalaResult instanceof com.snowflake.snowpark.MergeResult;
        return (T) new MergeResult((com.snowflake.snowpark.MergeResult) scalaResult);

      case WriteFileResult:
        assert scalaResult instanceof com.snowflake.snowpark.WriteFileResult;
        return (T) new WriteFileResult((com.snowflake.snowpark.WriteFileResult) scalaResult);

      default:
        // we have listed all types, should never reach here.
        // However, the default case is required by compiler.
        throw new RuntimeException("Unexpected DataType");
    }
  }

  static TypedAsyncJob<Row[]> createRowArrayJob(
      com.snowflake.snowpark.TypedAsyncJob<com.snowflake.snowpark.Row[]> job,
      com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.ArrayOfRow);
  }

  static TypedAsyncJob<Iterator<Row>> createRowIteratorJob(
      com.snowflake.snowpark.TypedAsyncJob<scala.collection.Iterator<com.snowflake.snowpark.Row>>
          job,
      com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.IteratorOfRow);
  }

  static TypedAsyncJob<Long> createLongJob(
      com.snowflake.snowpark.TypedAsyncJob<Object> job, com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.Long);
  }

  static TypedAsyncJob<Void> createVoidJob(
      com.snowflake.snowpark.TypedAsyncJob<?> job, com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.Void);
  }

  static TypedAsyncJob<UpdateResult> createUpdateResultJob(
      com.snowflake.snowpark.TypedAsyncJob<com.snowflake.snowpark.UpdateResult> job,
      com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.UpdateResult);
  }

  static TypedAsyncJob<DeleteResult> createDeleteResultJob(
      com.snowflake.snowpark.TypedAsyncJob<com.snowflake.snowpark.DeleteResult> job,
      com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.DeleteResult);
  }

  static TypedAsyncJob<MergeResult> createMergeResultJob(
      com.snowflake.snowpark.TypedAsyncJob<com.snowflake.snowpark.MergeResult> job,
      com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.MergeResult);
  }

  static TypedAsyncJob<WriteFileResult> createWriteFileResultJob(
      com.snowflake.snowpark.TypedAsyncJob<com.snowflake.snowpark.WriteFileResult> job,
      com.snowflake.snowpark.Session session) {
    return new TypedAsyncJob<>(job, session, AsyncJobDataType.WriteFileResult);
  }

  private enum AsyncJobDataType {
    ArrayOfRow,
    IteratorOfRow,
    Long,
    Void,
    UpdateResult,
    DeleteResult,
    MergeResult,
    WriteFileResult
  }
}
