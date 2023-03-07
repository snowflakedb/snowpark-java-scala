package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a lazily-evaluated Updatable. It extends {@code DataFrame} so all {@code DataFrame}
 * operations can be applied on it.
 *
 * <p>'''Creating an Updatable'''
 *
 * <p>You can create an Updatable by calling {@code Session.table} with the name of the Updatable.
 *
 * @since 1.1.0
 */
public class Updatable extends DataFrame {
  private final com.snowflake.snowpark.Updatable updatable;

  Updatable(com.snowflake.snowpark.Updatable updatable) {
    super(updatable);
    this.updatable = updatable;
  }

  /**
   * Updates all rows in the Updatable with specified assignments and returns a {@code
   * UpdateResult}, representing number of rows modified and number of multi-joined rows modified.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<Column, Column> assignments = new HashMap<>;
   * assignments.put(Functions.col("col1"), Functions.lit(1);
   * // Assign value 1 to column col1 in all rows in updatable.
   * session.table("tableName").update(assignment);
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @return {@code UpdateResult}
   */
  public UpdateResult update(Map<Column, Column> assignments) {
    return new UpdateResult(JavaUtils.updatable_update(toScalaColumnMap(assignments), updatable));
  }

  /**
   * Updates all rows in the updatable with specified assignments and returns a {@code
   * UpdateResult}, representing number of rows modified and number of multi-joined rows modified.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, Column> assignments = new HashMap<>;
   * assignments.put("col1", Functions.lit(1);
   * // Assign value 1 to column col1 in all rows in updatable.
   * session.table("tableName").updateColumn(assignment);
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @return {@code UpdateResult}
   */
  public UpdateResult updateColumn(Map<String, Column> assignments) {
    return new UpdateResult(
        JavaUtils.updatable_updateColumn(toScalaStringColumnMap(assignments), updatable));
  }

  /**
   * Updates all rows in the updatable that satisfy specified condition with specified assignments
   * and returns a {@code UpdateResult}, representing number of rows modified and number of
   * multi-joined rows modified.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<Column, Column> assignments = new HashMap<>;
   * assignments.put(Functions.col("col1"), Functions.lit(1);
   * // Assign value 1 to column col1 in the rows if col2 is true in updatable.
   * session.table("tableName").updateColumn(assignment,
   *   Functions.col("col2").equal_to(Functions.lit(true)));
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @return {@code UpdateResult}
   */
  public UpdateResult update(Map<Column, Column> assignments, Column condition) {
    return new UpdateResult(
        JavaUtils.updatable_update(
            toScalaColumnMap(assignments), condition.toScalaColumn(), updatable));
  }

  /**
   * Updates all rows in the updatable that satisfy specified condition with specified assignments
   * and returns a {@code UpdateResult}, representing number of rows modified and number of
   * multi-joined rows modified.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, Column> assignments = new HashMap<>;
   * assignments.put("col1", Functions.lit(1);
   * // Assign value 1 to column col1 in the rows if col3 is true in updatable.
   * session.table("tableName").updateColumn(assignment,
   *   Functions.col("col2").equal_to(Functions.lit(true)));
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @return {@code UpdateResult}
   */
  public UpdateResult updateColumn(Map<String, Column> assignments, Column condition) {
    return new UpdateResult(
        JavaUtils.updatable_updateColumn(
            toScalaStringColumnMap(assignments), condition.toScalaColumn(), updatable));
  }

  /**
   * Updates all rows in the updatable that satisfy specified condition where condition includes
   * columns in other {@code DataFrame}, and returns a {@code UpdateResult}, representing number of
   * rows modified and number of multi-joined rows modified.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<Column, Column> assignments = new HashMap<>;
   * assignments.put(Functions.col("col1"), Functions.lit(1);
   * session.table("tableName").update(assignment,
   *   Functions.col("col2").equal_to(df.col("col_a")), df);
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @param sourceData Another DataFrame being joined.
   * @return {@code UpdateResult}
   */
  public UpdateResult update(
      Map<Column, Column> assignments, Column condition, DataFrame sourceData) {
    return new UpdateResult(
        JavaUtils.updatable_update(
            toScalaColumnMap(assignments),
            condition.toScalaColumn(),
            sourceData.getScalaDataFrame(),
            updatable));
  }

  /**
   * Updates all rows in the updatable that satisfy specified condition where condition includes
   * columns in other {@code DataFrame}, and returns a {@code UpdateResult}, representing number of
   * rows modified and number of multi-joined rows modified.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, Column> assignments = new HashMap<>;
   * assignments.put("col1", Functions.lit(1);
   * session.table("tableName").updateColumn(assignment,
   *   Functions.col("col2").equal_to(df.col("col_a")), df);
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @param condition The condition of the Column being updated.
   * @param sourceData Another DataFrame being joined.
   * @return {@code UpdateResult}
   */
  public UpdateResult updateColumn(
      Map<String, Column> assignments, Column condition, DataFrame sourceData) {
    return new UpdateResult(
        JavaUtils.updatable_updateColumn(
            toScalaStringColumnMap(assignments),
            condition.toScalaColumn(),
            sourceData.getScalaDataFrame(),
            updatable));
  }

  /**
   * Deletes all rows in the updatable and returns a {@code DeleteResult}, representing number of
   * rows deleted.
   *
   * @since 1.1.0
   * @return {@code DeleteResult}
   */
  public DeleteResult delete() {
    return new DeleteResult(updatable.delete());
  }

  /**
   * Deletes all rows in the updatable that satisfy specified condition and returns a {@code
   * DeleteResult}, representing number of rows deleted.
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.table("tableName").delete(Functions.col("col1").equal_to(Functions.lit(1)));
   * }</pre>
   *
   * @since 1.1.0
   * @param condition The condition expression
   * @return {@code DeleteResult}
   */
  public DeleteResult delete(Column condition) {
    return new DeleteResult(updatable.delete(condition.toScalaColumn()));
  }

  /**
   * Deletes all rows in the updatable that satisfy specified condition where condition includes
   * columns in other {@code DataFrame}, and returns a {@code DeleteResult}, representing number of
   * rows deleted.
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.table(tableName).delete(Functions.col("col1").equal_to(df.col("col2")), df);
   * }</pre>
   *
   * @since 1.1.0
   * @param condition The condition expression
   * @param sourceData The source DataFrame
   * @return {@code DeleteResult}
   */
  public DeleteResult delete(Column condition, DataFrame sourceData) {
    return new DeleteResult(
        updatable.delete(condition.toScalaColumn(), sourceData.getScalaDataFrame()));
  }

  /**
   * Initiates a merge action for this updatable with {@code DataFrame} source on specified join
   * expression. Returns a {@code MergeBuilder} which provides APIs to define merge clauses.
   *
   * @since 1.1.0
   * @param source The source DataFrame
   * @param joinExpr The join expression
   * @return {@code MergeBuilder}
   */
  public MergeBuilder merge(DataFrame source, Column joinExpr) {
    return new MergeBuilder(
        this.updatable.merge(source.getScalaDataFrame(), joinExpr.toScalaColumn()));
  }

  /**
   * Returns a clone of this Updatable.
   *
   * @return {@code Updatable}
   * @since 1.1.0
   */
  @Override
  public Updatable clone() {
    // invoke super.clone to remove compiler warning
    super.clone();
    return new Updatable(updatable.clone());
  }

  /**
   * Returns an UpdatableAsyncActor object that can be used to execute Updatable actions
   * asynchronously.
   *
   * @since 1.2.0
   * @return A UpdatableAsyncActor object
   */
  @Override
  public UpdatableAsyncActor async() {
    return new UpdatableAsyncActor(this);
  }

  com.snowflake.snowpark.Updatable getScalaUpdatable() {
    return this.updatable;
  }

  static Map<com.snowflake.snowpark.Column, com.snowflake.snowpark.Column> toScalaColumnMap(
      Map<Column, Column> input) {
    Map<com.snowflake.snowpark.Column, com.snowflake.snowpark.Column> result = new HashMap<>();
    for (Map.Entry<Column, Column> entry : input.entrySet()) {
      result.put(entry.getKey().toScalaColumn(), entry.getValue().toScalaColumn());
    }
    return result;
  }

  static Map<String, com.snowflake.snowpark.Column> toScalaStringColumnMap(
      Map<String, Column> input) {
    Map<String, com.snowflake.snowpark.Column> result = new HashMap<>();
    for (Map.Entry<String, Column> entry : input.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toScalaColumn());
    }
    return result;
  }
}
