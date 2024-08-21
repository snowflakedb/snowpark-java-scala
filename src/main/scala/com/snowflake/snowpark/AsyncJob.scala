package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{CloseableIterator, ErrorMessage}
import com.snowflake.snowpark.internal.analyzer.SnowflakePlan
import scala.reflect.runtime.universe.{TypeTag, typeOf}

/** Provides a way to track an asynchronous query in Snowflake.
  *
  * You can use this object to check the status of an asynchronous query and retrieve the results.
  *
  * To check the status of an asynchronous query that you submitted earlier, call
  * [[Session.createAsyncJob]], and pass in the query ID. This returns an `AsyncJob` object that you
  * can use to check the status of the query and retrieve the query results.
  *
  * Example 1: Create an AsyncJob by specifying a valid `<query_id>`, check whether the query is
  * running or not, and get the result rows.
  * {{{
  *   val asyncJob = session.createAsyncJob(<query_id>)
  *   println(s"Is query \${asyncJob.getQueryId()} running? \${asyncJob.isRunning()}")
  *   val rows = asyncJob.getRows()
  * }}}
  *
  * Example 2: Create an AsyncJob by specifying a valid `<query_id>` and cancel the query if it is
  * still running.
  * {{{
  *   session.createAsyncJob(<query_id>).cancel()
  * }}}
  *
  * @since 0.11.0
  */
class AsyncJob private[snowpark] (queryID: String, session: Session, plan: Option[SnowflakePlan]) {

  /** Get the query ID for the underlying query.
    *
    * @since 0.11.0
    * @return
    *   a query ID
    */
  def getQueryId(): String = queryID

  /** Returns an iterator of [[Row]] objects that you can use to retrieve the results for the
    * underlying query.
    *
    * Unlike the [[getRows]] method, this method does not load all data into memory at once.
    *
    * @since 0.11.0
    * @param maxWaitTimeInSeconds
    *   The maximum number of seconds to wait for the query to complete before attempting to
    *   retrieve the results. The default value is the value of the
    *   `snowpark_request_timeout_in_seconds` configuration property.
    * @return
    *   An Iterator of [[Row]] objects
    */
  def getIterator(maxWaitTimeInSeconds: Int = session.requestTimeoutInSeconds): Iterator[Row] =
    session.conn.getAsyncResult(queryID, maxWaitTimeInSeconds, plan)._1

  /** Returns an Array of [[Row]] objects that represent the results of the underlying query.
    *
    * @since 0.11.0
    * @param maxWaitTimeInSeconds
    *   The maximum number of seconds to wait for the query to complete before attempting to
    *   retrieve the results. The default value is the value of the
    *   `snowpark_request_timeout_in_seconds` configuration property.
    * @return
    *   An Array of [[Row]] objects
    */
  def getRows(maxWaitTimeInSeconds: Int = session.requestTimeoutInSeconds): Array[Row] =
    getIterator(maxWaitTimeInSeconds).toArray

  /** Returns true if the underlying query completed.
    *
    * Completion may be due to query success, cancellation or failure, in all of these cases, this
    * method will return true.
    *
    * @since 0.11.0
    * @return
    *   true if this query completed.
    */
  def isDone(): Boolean = session.conn.isDone(queryID)

  /** Cancel the underlying query if it is running.
    *
    * @since 0.11.0
    */
  def cancel(): Unit = session.conn.runQuery(s"SELECT SYSTEM$$CANCEL_QUERY('$queryID')")
}

/** Provides a way to track an asynchronously executed action in a DataFrame.
  *
  * To get the result of the action (e.g. the number of results from a `count()` action or an Array
  * of [[Row]] objects from the `collect()` action), call the [[getResult]] method.
  *
  * To perform an action on a DataFrame asynchronously, call an action method on the
  * [[DataFrameAsyncActor]] object returned by [[DataFrame.async]]. For example:
  * {{{
  *   val asyncJob1 = df.async.collect()
  *   val asyncJob2 = df.async.toLocalIterator()
  *   val asyncJob3 = df.async.count()
  * }}}
  * Each of these methods returns a TypedAsyncJob object that you can use to get the results of the
  * action.
  *
  * @since 0.11.0
  */
class TypedAsyncJob[T: TypeTag] private[snowpark] (
    queryID: String,
    session: Session,
    plan: Option[SnowflakePlan])
    extends AsyncJob(queryID, session, plan) {

  /** Returns the result for the specific DataFrame action.
    *
    * Example 1: Create a TypedAsyncJob by asynchronously executing a DataFrame action `collect()`,
    * check whether the job is running or not, and get the action result with [[getResult]]. NOTE:
    * The returned type for [[getResult]] in this example is `Array[Row]`.
    * {{{
    *   val df = session.table("t1")
    *   val asyncJob = df.async.collect()
    *   println(s"Is query \${asyncJob.getQueryId()} running? \${asyncJob.isRunning()}")
    *   val rowResult = asyncJob.getResult()
    * }}}
    *
    * Example 2: Create a TypedAsyncJob by asynchronously executing a DataFrame action count() and
    * get the action result with [[getResult]]. NOTE: The returned type for [[getResult]] in this
    * example is `Long`.
    * {{{
    *   val asyncJob = df.async.count()
    *   val longResult = asyncJob.getResult()
    * }}}
    *
    * @since 0.11.0
    * @param maxWaitTimeInSeconds
    *   The maximum number of seconds to wait for the query to complete before attempting to
    *   retrieve the results. The default value is the value of the
    *   `snowpark_request_timeout_in_seconds` configuration property.
    * @return
    *   The result for the specific action
    */
  def getResult(maxWaitTimeInSeconds: Int = session.requestTimeoutInSeconds): T = {
    val tpe = typeOf[T]
    tpe match {
      // typeArgs are the general type arguments in class declaration,
      // for example, class Test[A, B], A and B are typeArgs.
      case t if t <:< typeOf[Array[Row]] => getRows(maxWaitTimeInSeconds).asInstanceOf[T]
      case t if t <:< typeOf[Iterator[Row]] => getIterator(maxWaitTimeInSeconds).asInstanceOf[T]
      case t if t <:< typeOf[Long] => getLong(maxWaitTimeInSeconds).asInstanceOf[T]
      case t if t <:< typeOf[Unit] => processWithoutReturn(maxWaitTimeInSeconds).asInstanceOf[T]
      case t if t <:< typeOf[UpdateResult] =>
        getUpdateResult(maxWaitTimeInSeconds).asInstanceOf[T]
      case t if t <:< typeOf[DeleteResult] =>
        getDeleteResult(maxWaitTimeInSeconds).asInstanceOf[T]
      case t if t <:< typeOf[WriteFileResult] =>
        getWriteFileResult(maxWaitTimeInSeconds).asInstanceOf[T]
      // Unsupported cases
      case _ =>
        throw ErrorMessage.PLAN_CANNOT_SUPPORT_TYPE_FOR_ASYNC_JOB(tpe.toString)
    }
  }

  private[snowpark] def getLong(maxWaitTimeInSeconds: Int): Long = {
    val rows = getRows(maxWaitTimeInSeconds)
    if (rows.length == 1 && rows.head.length == 1) {
      rows.head.getLong(0)
    } else {
      throw ErrorMessage.PLAN_CANNOT_GET_ASYNC_JOB_RESULT("Long", "getLong()")
    }
  }

  private[snowpark] def processWithoutReturn(maxWaitTimeInSeconds: Int): Unit =
    getIterator(maxWaitTimeInSeconds).asInstanceOf[CloseableIterator[Row]].close()

  private def getUpdateResult(maxWaitTimeInSeconds: Int): UpdateResult =
    Updatable.getUpdateResult(getRows(maxWaitTimeInSeconds))

  private def getDeleteResult(maxWaitTimeInSeconds: Int): DeleteResult =
    Updatable.getDeleteResult(getRows(maxWaitTimeInSeconds))

  private def getWriteFileResult(maxWaitTimeInSeconds: Int): WriteFileResult = {
    val (iterator, schema) = session.conn.getAsyncResult(queryID, maxWaitTimeInSeconds, plan)
    WriteFileResult(iterator.toArray, schema)
  }
}

/** Provides a way to track an asynchronously executed action in a MergeBuilder.
  *
  * @since 1.3.0
  */
class MergeTypedAsyncJob private[snowpark] (
    queryID: String,
    session: Session,
    plan: Option[SnowflakePlan],
    mergeBuilder: MergeBuilder)
    extends TypedAsyncJob[MergeResult](queryID, session, plan) {

  /** Returns the MergeResult for the MergeBuilder's action
    *
    * @since 1.3.0
    * @param maxWaitTimeInSeconds
    *   The maximum number of seconds to wait for the query to complete before attempting to
    *   retrieve the results. The default value is the value of the
    *   `snowpark_request_timeout_in_seconds` configuration property.
    * @return
    *   The [[MergeResult]]
    */
  override def getResult(maxWaitTimeInSeconds: Int = session.requestTimeoutInSeconds): MergeResult =
    MergeBuilder.getMergeResult(getRows(maxWaitTimeInSeconds), mergeBuilder)
}
