package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Iterator;

/**
 * Provides a way to track an asynchronous query in Snowflake.
 *
 * <p>You can use this object to check the status of an asynchronous query and retrieve the results.
 *
 * <p>To check the status of an asynchronous query that you submitted earlier, call {@code
 * Session.createAsyncJob}, and pass in the query ID. This returns an `AsyncJob` object that you can
 * use to check the status of the query and retrieve the query results.
 *
 * @since 1.2.0
 */
public class AsyncJob {
  com.snowflake.snowpark.AsyncJob job;
  com.snowflake.snowpark.Session session;

  AsyncJob(com.snowflake.snowpark.AsyncJob job, com.snowflake.snowpark.Session session) {
    this.job = job;
    this.session = session;
  }

  /**
   * Get the query ID for the underlying query.
   *
   * @since 1.2.0
   * @return a query ID
   */
  public String getQueryId() {
    return job.getQueryId();
  }

  /**
   * Returns an iterator of Row objects that you can use to retrieve the results for the underlying
   * query.
   *
   * <p>Unlike the getRows method, this method does not load all data into memory at once.
   *
   * @since 1.2.0
   * @param maxWaitTimeInSeconds The maximum number of seconds to wait for the query to complete
   *     before attempting to retrieve the results.
   * @return An Iterator of Row objects
   */
  public Iterator<Row> getIterator(int maxWaitTimeInSeconds) {
    return DataFrame.toJavaIterator(job.getIterator(maxWaitTimeInSeconds));
  }

  /**
   * Returns an iterator of Row objects that you can use to retrieve the results for the underlying
   * query.
   *
   * <p>Unlike the getRows method, this method does not load all data into memory at once.
   *
   * <p>The max waiting time is the value of `snowpark_request_timeout_in_seconds` configuration
   * property.
   *
   * @since 1.2.0
   * @return An Iterator of Row objects
   */
  public Iterator<Row> getIterator() {
    return getIterator(JavaUtils.session_requestTimeoutInSeconds(session));
  }

  /**
   * Returns an Array of Row objects that represent the results of the underlying query.
   *
   * @since 1.2.0
   * @param maxWaitTimeInSeconds The maximum number of seconds to wait for the query to complete
   *     before attempting to retrieve the results.
   * @return An Array of Row objects
   */
  public Row[] getRows(int maxWaitTimeInSeconds) {
    com.snowflake.snowpark.Row[] results = job.getRows(maxWaitTimeInSeconds);
    Row[] rows = new Row[results.length];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = new Row(results[i]);
    }
    return rows;
  }

  /**
   * Returns an Array of Row objects that represent the results of the underlying query.
   *
   * <p>The max waiting time is the value of the `snowpark_request_timeout_in_seconds` configuration
   * property.
   *
   * @since 1.2.0
   * @return An Array of Row objects
   */
  public Row[] getRows() {
    return getRows(JavaUtils.session_requestTimeoutInSeconds(session));
  }

  /**
   * Returns true if the underlying query completed.
   *
   * <p>Completion may be due to query success, cancellation or failure, in all of these cases, this
   * method will return true.
   *
   * @since 1.2.0
   * @return true if this query completed.
   */
  public boolean isDone() {
    return job.isDone();
  }

  /**
   * Cancel the underlying query if it is running.
   *
   * @since 1.2.0
   */
  public void cancel() {
    job.cancel();
  }
}
