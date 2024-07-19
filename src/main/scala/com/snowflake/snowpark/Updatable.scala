package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{Logging, OpenTelemetry}
import com.snowflake.snowpark.internal.analyzer._

import scala.reflect.ClassTag

private[snowpark] object Updatable extends Logging {
  def apply(tableName: String, session: Session): Updatable =
    new Updatable(tableName, session, DataFrame.methodChainCache.value)

  private[snowpark] def getUpdateResult(rows: Array[Row]): UpdateResult =
    UpdateResult(rows.head.getLong(0), rows.head.getLong(1))

  private[snowpark] def getDeleteResult(rows: Array[Row]): DeleteResult =
    DeleteResult(rows.head.getLong(0))

}

/**
 * Result of updating rows in an Updatable
 *
 * @since 0.7.0
 */
case class UpdateResult(rowsUpdated: Long, multiJoinedRowsUpdated: Long)

/**
 * Result of deleting rows in an Updatable
 *
 * @since 0.7.0
 */
case class DeleteResult(rowsDeleted: Long)

/**
 * Represents a lazily-evaluated Updatable. It extends [[DataFrame]] so all
 * [[DataFrame]] operations can be applied on it.
 *
 * '''Creating an Updatable'''
 *
 * You can create an Updatable by calling [[Session.table(name* session.table]] with the name of
 * the Updatable.
 *
 * Example 1: Creating a Updatable by reading a table.
 * {{{
 *   val dfPrices = session.table("itemsdb.publicschema.prices")
 * }}}
 *
 * @groupname actions Actions
 * @groupname basic Basic DataFrame Functions
 *
 * @since 0.7.0
 */
class Updatable private[snowpark] (
    private[snowpark] val tableName: String,
    override private[snowpark] val session: Session,
    override private[snowpark] val methodChain: Seq[String])
    extends DataFrame(
      session,
      session.analyzer.resolve(UnresolvedRelation(tableName)),
      methodChain) {

  /**
   * Updates all rows in the Updatable with specified assignments and returns a [[UpdateResult]],
   * representing number of rows modified and number of multi-joined rows modified.
   *
   * For example:
   * {{{
   *   updatable.update(Map(col("b") -> lit(0)))
   * }}}
   *
   * Assign value 0 to column b in all rows in updatable.
   *
   * {{{
   *   updatable.update(Map(col("c") -> (col("a") + col("b"))))
   * }}}
   *
   * Assign the sum of column a and column b to column c in all rows in updatable
   *
   * @group actions
   * @since 0.7.0
   * @return [[UpdateResult]]
   */
  def update(assignments: Map[Column, Column]): UpdateResult = action("update", 2) {
    val newDf = getUpdateDataFrameWithColumn(assignments, None, None)
    Updatable.getUpdateResult(newDf.collect())
  }

  /**
   * Updates all rows in the updatable with specified assignments and returns a [[UpdateResult]],
   * representing number of rows modified and number of multi-joined rows modified.
   *
   * For example:
   * {{{
   *   updatable.update(Map("b" -> lit(0)))
   * }}}
   *
   * Assign value 0 to column b in all rows in updatable.
   *
   * {{{
   *   updatable.update(Map("c" -> (col("a") + col("b"))))
   * }}}
   *
   * Assign the sum of column a and column b to column c in all rows in updatable
   *
   * @group actions
   * @since 0.7.0
   * @return [[UpdateResult]]
   */
  def update[T: ClassTag](assignments: Map[String, Column]): UpdateResult = action("update", 2) {
    val newDf = getUpdateDataFrameWithString(assignments, None, None)
    Updatable.getUpdateResult(newDf.collect())
  }

  /**
   * Updates all rows in the updatable that satisfy specified condition with specified assignments
   * and returns a [[UpdateResult]], representing number of rows modified and number of
   * multi-joined rows modified.
   *
   * For example:
   * {{{
   *   updatable.update(Map(col("b") -> lit(0)), col("a") === 1)
   * }}}
   *
   * Assign value 0 to column b in all rows where column a has value 1.
   *
   * @group actions
   * @since 0.7.0
   * @return [[UpdateResult]]
   */
  def update(assignments: Map[Column, Column], condition: Column): UpdateResult =
    action("update", 2) {
      val newDf = getUpdateDataFrameWithColumn(assignments, Some(condition), None)
      Updatable.getUpdateResult(newDf.collect())
    }

  /**
   * Updates all rows in the updatable that satisfy specified condition with specified assignments
   * and returns a [[UpdateResult]], representing number of rows modified and number of
   * multi-joined rows modified.
   *
   * For example:
   * {{{
   *   updatable.update(Map("b" -> lit(0)), col("a") === 1)
   * }}}
   *
   * Assign value 0 to column b in all rows where column a has value 1.
   *
   * @group actions
   * @since 0.7.0
   * @return [[UpdateResult]]
   */
  def update[T: ClassTag](assignments: Map[String, Column], condition: Column): UpdateResult =
    action("update", 2) {
      val newDf = getUpdateDataFrameWithString(assignments, Some(condition), None)
      Updatable.getUpdateResult(newDf.collect())
    }

  /**
   * Updates all rows in the updatable that satisfy specified condition where condition includes
   * columns in other [[DataFrame]], and returns a [[UpdateResult]], representing number of rows
   * modified and number of multi-joined rows modified.
   *
   * For example:
   * {{{
   *   t1.update(Map(col("b") -> lit(0)), t1("a") === t2("a"), t2)
   * }}}
   *
   * Assign value 0 to column b in all rows in t1 where column a in t1 equals column a in t2.
   *
   * @group actions
   * @since 0.7.0
   * @return [[UpdateResult]]
   */
  def update(
      assignments: Map[Column, Column],
      condition: Column,
      sourceData: DataFrame): UpdateResult = action("update", 2) {
    val newDf = getUpdateDataFrameWithColumn(assignments, Some(condition), Some(sourceData))
    Updatable.getUpdateResult(newDf.collect())
  }

  /**
   * Updates all rows in the updatable that satisfy specified condition where condition includes
   * columns in other [[DataFrame]], and returns a [[UpdateResult]], representing number of rows
   * modified and number of multi-joined rows modified.
   *
   * For example:
   * {{{
   *   t1.update(Map("b" -> lit(0)), t1("a") === t2("a"), t2)
   * }}}
   *
   * Assign value 0 to column b in all rows in t1 where column a in t1 equals column a in t2.
   *
   * @group actions
   * @since 0.7.0
   * @return [[UpdateResult]]
   */
  def update[T: ClassTag](
      assignments: Map[String, Column],
      condition: Column,
      sourceData: DataFrame): UpdateResult = action("update", 2) {
    val newDf = getUpdateDataFrameWithString(assignments, Some(condition), Some(sourceData))
    Updatable.getUpdateResult(newDf.collect())
  }

  private[snowpark] def getUpdateDataFrameWithString(
      assignments: Map[String, Column],
      condition: Option[Column],
      sourceData: Option[DataFrame]): DataFrame =
    getUpdateDataFrameWithColumn(
      assignments.map { case (k, v) => (col(k), v) },
      condition,
      sourceData)

  private[snowpark] def getUpdateDataFrameWithColumn(
      assignments: Map[Column, Column],
      condition: Option[Column],
      sourceData: Option[DataFrame]): DataFrame = {
    session.conn.telemetry.reportActionUpdate()
    withPlan(
      TableUpdate(
        tableName,
        assignments.map { case (k, v) => (k.expr, v.expr) },
        condition.map(_.expr),
        sourceData.map(disambiguate(this, _, JoinType("left"), Seq.empty)._2.plan)))
  }

  /**
   * Deletes all rows in the updatable and returns a [[DeleteResult]], representing number of rows
   * deleted.
   *
   * For example:
   * {{{
   *   updatable.delete()
   * }}}
   *
   * Deletes all rows in updatable.
   *
   * @group actions
   * @since 0.7.0
   * @return [[DeleteResult]]
   */
  def delete(): DeleteResult = action("delete") {
    val newDf = getDeleteDataFrame(None, None)
    Updatable.getDeleteResult(newDf.collect())
  }

  /**
   * Deletes all rows in the updatable that satisfy specified condition and returns a
   * [[DeleteResult]], representing number of rows deleted.
   *
   * For example:
   * {{{
   *   updatable.delete(col("a") === 1)
   * }}}
   *
   * Deletes all rows where column a has value 1.
   *
   * @group actions
   * @since 0.7.0
   * @return [[DeleteResult]]
   */
  def delete(condition: Column): DeleteResult = action("delete") {
    val newDf = getDeleteDataFrame(Some(condition), None)
    Updatable.getDeleteResult(newDf.collect())
  }

  /**
   * Deletes all rows in the updatable that satisfy specified condition where condition includes
   * columns in other [[DataFrame]], and returns a [[DeleteResult]], representing number of rows
   * deleted.
   *
   * For example:
   * {{{
   *   t1.delete(t1("a") === t2("a"), t2)
   * }}}
   *
   * Deletes all rows in t1 where column a in t1 equals column a in t2.
   *
   * @group actions
   * @since 0.7.0
   * @return [[DeleteResult]]
   */
  def delete(condition: Column, sourceData: DataFrame): DeleteResult = action("delete") {
    val newDf = getDeleteDataFrame(Some(condition), Some(sourceData))
    Updatable.getDeleteResult(newDf.collect())
  }

  private[snowpark] def getDeleteDataFrame(
      condition: Option[Column],
      sourceData: Option[DataFrame]): DataFrame = {
    session.conn.telemetry.reportActionDelete()
    withPlan(
      TableDelete(
        tableName,
        condition.map(_.expr),
        sourceData.map(disambiguate(this, _, JoinType("left"), Seq.empty)._2.plan)))
  }

  /**
   * Initiates a merge action for this updatable with [[DataFrame]] source on specified
   * join expression. Returns a [[MergeBuilder]] which provides APIs to define merge clauses.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   * }}}
   *
   * Initiates a merge action for target with source where the expression target.id = source.id
   * is used to join target and source.
   *
   * @group actions
   * @since 0.7.0
   * @return [[MergeBuilder]]
   */
  def merge(source: DataFrame, joinExpr: Column): MergeBuilder = {
    session.conn.telemetry.reportActionMerge()
    MergeBuilder(
      this,
      disambiguate(this, source, JoinType("left"), Seq.empty)._2,
      joinExpr,
      Seq.empty,
      inserted = false,
      updated = false,
      deleted = false)
  }

  /**
   * Returns a clone of this Updatable.
   *
   * @return A [[Updatable]]
   * @since 0.10.0
   * @group basic
   */
  override def clone: Updatable = action("clone", 2) {
    new Updatable(tableName, session, Seq())
  }

  /**
   * Returns an [[UpdatableAsyncActor]] object that can be used to execute
   * Updatable actions asynchronously.
   *
   * Example:
   * {{{
   *   val updatable = session.table(tableName)
   *   val asyncJob = updatable.async.update(Map(col("b") -> lit(0)), col("a") === 1)
   *   // At this point, the thread is not blocked. You can perform additional work before
   *   // calling asyncJob.getResult() to retrieve the results of the action.
   *   // NOTE: getResult() is a blocking call.
   *   val updateResult = asyncJob.getResult()
   * }}}
   *
   * @since 0.11.0
   * @return A [[UpdatableAsyncActor]] object
   */
  override def async: UpdatableAsyncActor = new UpdatableAsyncActor(this)

  @inline override protected def action[T](funcName: String)(func: => T): T = {
    val isScala: Boolean = this.session.conn.isScalaAPI
    OpenTelemetry.action("Updatable", funcName, methodChainString, isScala)(func)
  }

  @inline protected def action[T](funcName: String, javaOffset: Int)(func: => T): T = {
    val isScala: Boolean = this.session.conn.isScalaAPI
    OpenTelemetry.action("Updatable", funcName, methodChainString, isScala, javaOffset)(func)
  }

}

/**
 * Provides APIs to execute Updatable actions asynchronously.
 *
 * @since 0.11.0
 */
class UpdatableAsyncActor private[snowpark] (updatable: Updatable)
    extends DataFrameAsyncActor(updatable) {

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def update(assignments: Map[Column, Column]): TypedAsyncJob[UpdateResult] =
    action("update", 2) {
      val newDf = updatable.getUpdateDataFrameWithColumn(assignments, None, None)
      updatable.session.conn.executeAsync[UpdateResult](newDf.snowflakePlan)
    }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def update[T: ClassTag](assignments: Map[String, Column]): TypedAsyncJob[UpdateResult] =
    action("update", 2) {
      val newDf = updatable.getUpdateDataFrameWithString(assignments, None, None)
      updatable.session.conn.executeAsync[UpdateResult](newDf.snowflakePlan)
    }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def update(assignments: Map[Column, Column], condition: Column): TypedAsyncJob[UpdateResult] =
    action("update", 2) {
      val newDf = updatable.getUpdateDataFrameWithColumn(assignments, Some(condition), None)
      updatable.session.conn.executeAsync[UpdateResult](newDf.snowflakePlan)
    }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def update[T: ClassTag](
      assignments: Map[String, Column],
      condition: Column): TypedAsyncJob[UpdateResult] =
    action("update", 2) {
      val newDf = updatable.getUpdateDataFrameWithString(assignments, Some(condition), None)
      updatable.session.conn.executeAsync[UpdateResult](newDf.snowflakePlan)
    }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def update(
      assignments: Map[Column, Column],
      condition: Column,
      sourceData: DataFrame): TypedAsyncJob[UpdateResult] = action("update", 2) {
    val newDf =
      updatable.getUpdateDataFrameWithColumn(assignments, Some(condition), Some(sourceData))
    updatable.session.conn.executeAsync[UpdateResult](newDf.snowflakePlan)
  }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def update[T: ClassTag](
      assignments: Map[String, Column],
      condition: Column,
      sourceData: DataFrame): TypedAsyncJob[UpdateResult] = action("update", 2) {
    val newDf =
      updatable.getUpdateDataFrameWithString(assignments, Some(condition), Some(sourceData))
    updatable.session.conn.executeAsync[UpdateResult](newDf.snowflakePlan)
  }

  /**
   * Executes `Updatable.delete` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def delete(): TypedAsyncJob[DeleteResult] = action("delete") {
    val newDf = updatable.getDeleteDataFrame(None, None)
    updatable.session.conn.executeAsync[DeleteResult](newDf.snowflakePlan)
  }

  /**
   * Executes `Updatable.delete` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def delete(condition: Column): TypedAsyncJob[DeleteResult] = action("delete") {
    val newDf = updatable.getDeleteDataFrame(Some(condition), None)
    updatable.session.conn.executeAsync[DeleteResult](newDf.snowflakePlan)
  }

  /**
   * Executes `Updatable.delete` asynchronously.
   *
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def delete(condition: Column, sourceData: DataFrame): TypedAsyncJob[DeleteResult] =
    action("delete") {
      val newDf = updatable.getDeleteDataFrame(Some(condition), Some(sourceData))
      updatable.session.conn.executeAsync[DeleteResult](newDf.snowflakePlan)
    }

  @inline override protected def action[T](funcName: String)(func: => T): T = {
    val isScala: Boolean = updatable.session.conn.isScalaAPI
    OpenTelemetry.action(
      "UpdatableAsyncActor",
      funcName,
      updatable.methodChainString + ".async",
      isScala)(func)
  }

  @inline protected def action[T](funcName: String, javaOffset: Int)(func: => T): T = {
    val isScala: Boolean = updatable.session.conn.isScalaAPI
    OpenTelemetry.action(
      "UpdatableAsyncActor",
      funcName,
      updatable.methodChainString + ".async",
      isScala,
      javaOffset)(func)
  }
}
