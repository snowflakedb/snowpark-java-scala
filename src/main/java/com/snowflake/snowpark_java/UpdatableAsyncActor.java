package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * Provides APIs to execute Updatable actions asynchronously.
 *
 * @since 1.2.0
 */
public class UpdatableAsyncActor extends DataFrameAsyncActor {
  private final com.snowflake.snowpark.UpdatableAsyncActor uDfAsync;

  UpdatableAsyncActor(Updatable updatable) {
    super(updatable);
    this.uDfAsync = updatable.getScalaUpdatable().async();
  }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @param assignments A map contains the column being updated and the new value.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<UpdateResult> update(Map<Column, Column> assignments) {
    return TypedAsyncJob.createUpdateResultJob(
        JavaUtils.async_updatable_update(Updatable.toScalaColumnMap(assignments), uDfAsync),
        session);
  }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @param assignments A map contains the column being updated and the new value.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<UpdateResult> updateColumn(Map<String, Column> assignments) {
    return TypedAsyncJob.createUpdateResultJob(
        JavaUtils.async_updatable_updateColumn(
            Updatable.toScalaStringColumnMap(assignments), uDfAsync),
        session);
  }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<UpdateResult> update(Map<Column, Column> assignments, Column condition) {
    return TypedAsyncJob.createUpdateResultJob(
        JavaUtils.async_updatable_update(
            Updatable.toScalaColumnMap(assignments), condition.toScalaColumn(), uDfAsync),
        session);
  }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<UpdateResult> updateColumn(
      Map<String, Column> assignments, Column condition) {
    return TypedAsyncJob.createUpdateResultJob(
        JavaUtils.async_updatable_updateColumn(
            Updatable.toScalaStringColumnMap(assignments), condition.toScalaColumn(), uDfAsync),
        session);
  }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @param sourceData Another DataFrame being joined.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<UpdateResult> update(
      Map<Column, Column> assignments, Column condition, DataFrame sourceData) {
    return TypedAsyncJob.createUpdateResultJob(
        JavaUtils.async_updatable_update(
            Updatable.toScalaColumnMap(assignments),
            condition.toScalaColumn(),
            sourceData.getScalaDataFrame(),
            uDfAsync),
        session);
  }

  /**
   * Executes `Updatable.update` asynchronously.
   *
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @param sourceData Another DataFrame being joined.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<UpdateResult> updateColumn(
      Map<String, Column> assignments, Column condition, DataFrame sourceData) {
    return TypedAsyncJob.createUpdateResultJob(
        JavaUtils.async_updatable_updateColumn(
            Updatable.toScalaStringColumnMap(assignments),
            condition.toScalaColumn(),
            sourceData.getScalaDataFrame(),
            uDfAsync),
        session);
  }

  /**
   * Executes `Updatable.delete` asynchronously.
   *
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<DeleteResult> delete() {
    return TypedAsyncJob.createDeleteResultJob(uDfAsync.delete(), session);
  }

  /**
   * Executes `Updatable.delete` asynchronously.
   *
   * @param condition The condition expression
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<DeleteResult> delete(Column condition) {
    return TypedAsyncJob.createDeleteResultJob(uDfAsync.delete(condition.toScalaColumn()), session);
  }

  /**
   * Executes `Updatable.delete` asynchronously.
   *
   * @param condition The condition expression
   * @param sourceData The source DataFrame
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<DeleteResult> delete(Column condition, DataFrame sourceData) {
    return TypedAsyncJob.createDeleteResultJob(
        uDfAsync.delete(condition.toScalaColumn(), sourceData.getScalaDataFrame()), session);
  }
}
