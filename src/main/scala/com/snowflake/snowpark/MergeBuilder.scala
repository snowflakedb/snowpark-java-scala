package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{ErrorMessage, OpenTelemetry}
import com.snowflake.snowpark.internal.analyzer.{MergeExpression, TableMerge}

/**
 * Result of merging a DataFrame into an Updatable DataFrame
 *
 * @since 0.7.0
 */
case class MergeResult(rowsInserted: Long, rowsUpdated: Long, rowsDeleted: Long)

private[snowpark] object MergeBuilder {
  private[snowpark] def apply(
      target: Updatable,
      source: DataFrame,
      joinExpr: Column,
      clauses: Seq[MergeExpression],
      inserted: Boolean,
      updated: Boolean,
      deleted: Boolean): MergeBuilder = {
    new MergeBuilder(target, source, joinExpr, clauses, inserted, updated, deleted)
  }

  // Generate MergeResult from query result rows
  private[snowpark] def getMergeResult(
      rows: Array[Row],
      mergeBuilder: MergeBuilder): MergeResult = {
    if (rows.length != 1) {
      throw ErrorMessage.PLAN_MERGE_RETURN_WRONG_ROWS(1, rows.length)
    }
    val result = rows.head

    var index = -1
    val rowsInserted = if (mergeBuilder.inserted) {
      index += 1
      result.getLong(index)
    } else 0
    val rowsUpdated = if (mergeBuilder.updated) {
      index += 1
      result.getLong(index)
    } else 0
    val rowsDeleted = if (mergeBuilder.deleted) {
      index += 1
      result.getLong(index)
    } else 0

    MergeResult(rowsInserted, rowsUpdated, rowsDeleted)
  }
}

/**
 * Builder for a merge action. It provides APIs to build matched and not matched clauses.
 *
 * @groupname actions Actions
 * @groupname transform Transformations
 *
 * @since 0.7.0
 */
class MergeBuilder private[snowpark] (
    private[snowpark] val target: Updatable,
    private[snowpark] val source: DataFrame,
    private[snowpark] val joinExpr: Column,
    private[snowpark] val clauses: Seq[MergeExpression],
    private[snowpark] val inserted: Boolean,
    private[snowpark] val updated: Boolean,
    private[snowpark] val deleted: Boolean) {

  /**
   * Adds a matched clause into the merge action. It matches all remaining rows in target
   * that satisfy <joinExpr>. Returns a [[MatchedClauseBuilder]] which provides APIs to
   * define actions to take when a row is matched.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id")).whenMatched
   * }}}
   *
   * Adds a matched clause where a row in the [[Updatable]] target is matched if its id equals
   * the id of a row in the [[DataFrame]] source.
   *
   * Caution: Since it matches all remaining rows, no more whenMatched calls will be accepted
   * beyond this call.
   *
   * @group transform
   * @since 0.7.0
   * @return [[MatchedClauseBuilder]]
   */
  def whenMatched: MatchedClauseBuilder = whenMatched(None)

  /**
   * Adds a matched clause into the merge action. It matches all rows in target that satisfy
   * <joinExpr> while also satisfying <condition>. Returns a [[MatchedClauseBuilder]] which provides
   * APIs to define actions to take when a row is matched.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id")).whenMatched(target("value") === lit(0))
   * }}}
   *
   * Adds a matched clause where a row in the [[Updatable]] target is matched if its id equals the
   * id of a row in the [[DataFrame]] source and its value equals 0.
   *
   * @group transform
   * @since 0.7.0
   * @return [[MatchedClauseBuilder]]
   */
  def whenMatched(condition: Column): MatchedClauseBuilder =
    whenMatched(Some(condition))

  private[snowpark] def whenMatched(condition: Option[Column]): MatchedClauseBuilder = {
    MatchedClauseBuilder(this, condition)
  }

  /**
   * Adds a not matched clause into the merge action. It matches all remaining rows in source
   * that do not satisfy <joinExpr>. Returns a [[MatchedClauseBuilder]] which provides APIs to
   * define actions to take when a row is not matched.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id")).whenNotMatched
   * }}}
   *
   * Adds a not matched clause where a row in the [[DataFrame]] source is not matched if its id
   * does not equal the id of any row in the [[Updatable]] target.
   *
   * Caution: Since it matches all remaining rows, no more whenNotMatched calls will be accepted
   * beyond this call.
   *
   * @group transform
   * @since 0.7.0
   * @return [[NotMatchedClauseBuilder]]
   */
  def whenNotMatched: NotMatchedClauseBuilder = whenNotMatched(None)

  /**
   * Adds a not matched clause into the merge action. It matches all rows in source that do not
   * satisfy <joinExpr> but satisfy <condition>. Returns a [[MatchedClauseBuilder]] which provides
   * APIs to define actions to take when a row is matched.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   *   .whenNotMatched(source("value") === lit(0))
   * }}}
   *
   * Adds a not matched clause where a row in the [[DataFrame]] source is not matched if its id
   * does not equal the id of any row in the [[Updatable]] source and its value equals 0.
   *
   * @group transform
   * @since 0.7.0
   * @return [[NotMatchedClauseBuilder]]
   */
  def whenNotMatched(condition: Column): NotMatchedClauseBuilder =
    whenNotMatched(Some(condition))

  private[snowpark] def whenNotMatched(condition: Option[Column]): NotMatchedClauseBuilder = {
    NotMatchedClauseBuilder(this, condition)
  }

  /**
   * Executes the merge action and returns a [[MergeResult]], representing number of rows inserted,
   * updated and deleted by this merge action.
   *
   * @group action
   * @since 0.7.0
   * @return [[MergeResult]]
   */
  def collect(): MergeResult = action("collect") {
    val rows = getMergeDataFrame().collect()
    MergeBuilder.getMergeResult(rows, this)
  }

  // Get the DataFrame for the merge query.
  private[snowpark] def getMergeDataFrame(): DataFrame = {
    target.session.conn.telemetry.reportActionMerge()
    DataFrame(target.session, TableMerge(target.tableName, source.plan, joinExpr.expr, clauses))
  }

  /**
   * Returns a [[MergeBuilderAsyncActor]] object that can be used to execute
   * MergeBuilder actions asynchronously.
   *
   * Example:
   * {{{
   *   val target = session.table(tableName)
   *   val source = Seq((10, "new")).toDF("id", "desc")
   *   val asyncJob = target
   *     .merge(source, target("id") === source("id"))
   *     .whenMatched
   *     .update(Map(target("desc") -> source("desc")))
   *     .async
   *     .collect()
   *   // At this point, the thread is not blocked. You can perform additional work before
   *   // calling asyncJob.getResult() to retrieve the results of the action.
   *   // NOTE: getResult() is a blocking call.
   *   val mergeResult = asyncJob.getResult()
   * }}}
   *
   * @since 1.3.0
   * @return A [[MergeBuilderAsyncActor]] object
   */
  def async: MergeBuilderAsyncActor = new MergeBuilderAsyncActor(this)

  @inline protected def action[T](funcName: String)(func: => T): T = {
    val isScala: Boolean = target.session.conn.isScalaAPI
    OpenTelemetry.action("MergeBuilder", funcName, target.methodChainString + ".merge", isScala)(
      func)
  }
}

/**
 * Provides APIs to execute MergeBuilder actions asynchronously.
 *
 * @since 1.3.0
 */
class MergeBuilderAsyncActor private[snowpark] (mergeBuilder: MergeBuilder) {

  /**
   * Executes `MergeBuilder.collect()` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 1.3.0
   */
  def collect(): TypedAsyncJob[MergeResult] = action("collect") {
    val newDf = mergeBuilder.getMergeDataFrame()
    mergeBuilder.target.session.conn
      .executeAsync[MergeResult](newDf.snowflakePlan, Some(mergeBuilder))
  }

  @inline protected def action[T](funcName: String)(func: => T): T = {
    val isScala: Boolean = mergeBuilder.target.session.conn.isScalaAPI
    OpenTelemetry.action(
      "MergeBuilderAsyncActor",
      funcName,
      mergeBuilder.target.methodChainString + ".merge.async",
      isScala)(func)
  }
}
