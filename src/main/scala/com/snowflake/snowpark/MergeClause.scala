package com.snowflake.snowpark

import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.internal.analyzer.{
  DeleteMergeExpression,
  InsertMergeExpression,
  UpdateMergeExpression
}

import scala.reflect.ClassTag

private[snowpark] object NotMatchedClauseBuilder {
  private[snowpark] def apply(
      mergeBuilder: MergeBuilder,
      condition: Option[Column]): NotMatchedClauseBuilder =
    new NotMatchedClauseBuilder(mergeBuilder, condition)
}

/**
 * Builder for a not matched clause. It provides APIs to build insert actions
 *
 * @since 0.7.0
 */
class NotMatchedClauseBuilder private[snowpark] (
    mergeBuilder: MergeBuilder,
    condition: Option[Column]) {

  /**
   * Defines an insert action for the not matched clause, when a row in source is not matched,
   * insert a row in target with <values>. Returns an updated [[MergeBuilder]] with the new clause
   * added.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   *   .whenNotMatched.insert(Seq(source("id"), source("value")))
   * }}}
   *
   * Adds a not matched clause where a row in source is not matched if its id does not equal the id
   * of any row in the [[Updatable]] target. For all such rows, insert a row into target whose id
   * and value are assigned to the id and value of the not matched row.
   *
   * Note: This API inserts into all columns in target with values, so the length of <values> must
   * equal the number of columns in target.
   *
   * @group transform
   * @since 0.7.0
   * @return
   *   [[MergeBuilder]]
   */
  def insert(values: Seq[Column]): MergeBuilder = {
    MergeBuilder(
      mergeBuilder.target,
      mergeBuilder.source,
      mergeBuilder.joinExpr,
      mergeBuilder.clauses :+ InsertMergeExpression(
        condition.map(_.expr),
        Seq.empty,
        values.map(_.expr)),
      inserted = true,
      mergeBuilder.updated,
      mergeBuilder.deleted)
  }

  /**
   * Defines an insert action for the not matched clause, when a row in source is not matched,
   * insert a row in target with <assignments>, where the key specifies column name and value
   * specifies its assigned value. All unspecified columns are set to NULL. Returns an updated
   * [[MergeBuilder]] with the new clause added.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   *   .whenNotMatched.insert(Map("id" -> source("id")))
   * }}}
   *
   * Adds a not matched clause where a row in source is not matched if its id does not equal the id
   * of any row in the [[Updatable]] target. For all such rows, insert a row into target whose id is
   * assigned to the id of the not matched row.
   *
   * @group transform
   * @since 0.7.0
   * @return
   *   [[MergeBuilder]]
   */
  def insert[T: ClassTag](assignments: Map[String, Column]): MergeBuilder = {
    insert(assignments.map { case (k, v) => (col(k), v) })
  }

  /**
   * Defines an insert action for the not matched clause, when a row in source is not matched,
   * insert a row in target with <assignments>, where the key specifies column name and value
   * specifies its assigned value. All unspecified columns are set to NULL. Returns an updated
   * [[MergeBuilder]] with the new clause added.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   *   .whenNotMatched.insert(Map(target("id") -> source("id")))
   * }}}
   *
   * Adds a not matched clause where a row in source is not matched if its id does not equal the id
   * of any row in the [[Updatable]] target. For all such rows, insert a row into target whose id is
   * assigned to the id of the not matched row.
   *
   * @group transform
   * @since 0.7.0
   * @return
   *   [[MergeBuilder]]
   */
  def insert(assignments: Map[Column, Column]): MergeBuilder = {
    val assignmentSeq = assignments.toSeq
    MergeBuilder(
      mergeBuilder.target,
      mergeBuilder.source,
      mergeBuilder.joinExpr,
      mergeBuilder.clauses :+ InsertMergeExpression(
        condition.map(_.expr),
        assignmentSeq.map(_._1.expr),
        assignmentSeq.map(_._2.expr)),
      inserted = true,
      mergeBuilder.updated,
      mergeBuilder.deleted)
  }
}

private[snowpark] object MatchedClauseBuilder {
  private[snowpark] def apply(
      mergeBuilder: MergeBuilder,
      condition: Option[Column]): MatchedClauseBuilder =
    new MatchedClauseBuilder(mergeBuilder, condition)
}

/**
 * Builder for a matched clause. It provides APIs to build update and delete actions
 *
 * @since 0.7.0
 */
class MatchedClauseBuilder private[snowpark] (
    mergeBuilder: MergeBuilder,
    condition: Option[Column]) {

  /**
   * Defines an update action for the matched clause, when a row in target is matched, update the
   * row in target with <assignments>, where the key specifies column name and value specifies its
   * assigned value. Returns an updated [[MergeBuilder]] with the new clause added.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   *   .whenMatched.update(Map("value" -> source("value")))
   * }}}
   *
   * Adds a matched clause where a row in target is matched if its id equals the id of a row in the
   * [[DataFrame]] source. For all such rows, update its value to the value of the corresponding row
   * in source.
   *
   * @group transform
   * @since 0.7.0
   * @return
   *   [[MergeBuilder]]
   */
  def update[T: ClassTag](assignments: Map[String, Column]): MergeBuilder =
    update(assignments.map { case (k, v) => (col(k), v) })

  /**
   * Defines an update action for the matched clause, when a row in target is matched, update the
   * row in target with <assignments>, where the key specifies column name and value specifies its
   * assigned value. Returns an updated [[MergeBuilder]] with the new clause added.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   *   .whenMatched.update(Map(target("value") -> source("value")))
   * }}}
   *
   * Adds a matched clause where a row in target is matched if its id equals the id of a row in the
   * [[DataFrame]] source. For all such rows, update its value to the value of the corresponding row
   * in source.
   *
   * @group transform
   * @since 0.7.0
   * @return
   *   [[MergeBuilder]]
   */
  def update(assignments: Map[Column, Column]): MergeBuilder = {
    MergeBuilder(
      mergeBuilder.target,
      mergeBuilder.source,
      mergeBuilder.joinExpr,
      mergeBuilder.clauses :+ UpdateMergeExpression(
        condition.map(_.expr),
        assignments.map { case (k, v) =>
          (k.expr, v.expr)
        }),
      mergeBuilder.inserted,
      updated = true,
      mergeBuilder.deleted)
  }

  /**
   * Defines a delete action for the matched clause, when a row in target is matched, delete it from
   * target. Returns an updated [[MergeBuilder]] with the new clause added.
   *
   * For example:
   * {{{
   *   target.merge(source, target("id") === source("id"))
   *   .whenMatched.delete()
   * }}}
   *
   * Adds a matched clause where a row in target is matched if its id equals the id of a row in the
   * [[DataFrame]] source. For all such rows, delete it from target.
   *
   * @group transform
   * @since 0.7.0
   * @return
   *   [[MergeBuilder]]
   */
  def delete(): MergeBuilder = {
    MergeBuilder(
      mergeBuilder.target,
      mergeBuilder.source,
      mergeBuilder.joinExpr,
      mergeBuilder.clauses :+ DeleteMergeExpression(condition.map(_.expr)),
      mergeBuilder.inserted,
      mergeBuilder.updated,
      deleted = true)
  }
}
