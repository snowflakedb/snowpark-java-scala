package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * Builder for a not matched clause. It provides APIs to build insert actions
 *
 * @since 1.1.0
 */
public class NotMatchedClauseBuilder {
  private final com.snowflake.snowpark.NotMatchedClauseBuilder builder;

  NotMatchedClauseBuilder(com.snowflake.snowpark.NotMatchedClauseBuilder builder) {
    this.builder = builder;
  }

  /**
   * Defines an insert action for the not matched clause, when a row in source is not matched,
   * insert a row in target with 'values'. Returns an updated {@code MergeBuilder} with the new
   * clause added.
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.table("tableName").merge(df, Functions.col("col_1").equal_to(df.col("col_a")))
   *   .whenNotMatched().insert(new Column[]{df.col("col_b"), Functions.lit("c"), Functions.lit(true)})
   *   .collect();
   * }</pre>
   *
   * Note: This API inserts into all columns in target with values, so the length of 'values' must
   * equal the number of columns in target.
   *
   * @since 1.1.0
   * @param values The valued being inserted
   * @return {@code MergeBuilder}
   */
  public MergeBuilder insert(Column[] values) {
    return new MergeBuilder(
        this.builder.insert(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(values))));
  }

  /**
   * Defines an insert action for the not matched clause, when a row in source is not matched,
   * insert a row in target with 'assignments', where the key specifies column name and value
   * specifies its assigned value. All unspecified columns are set to NULL. Returns an updated
   * {@code MergeBuilder} with the new clause added.
   *
   * <pre>{@code
   * Map<Column, Column> assignments = new HashMap<>();
   * assignments.put(Functions.col("col_1"), df.col("col_b"));
   * session.table("tableName").merge(df, Functions.col("col_1").equal_to(df.col("col_a")))
   *   .whenNotMatched(df.col("col_a").equal_to(Functions.lit(3)))
   *   .insert(assignments).collect();
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains row data.
   * @return {@code MergeBuilder}
   */
  public MergeBuilder insert(Map<Column, Column> assignments) {
    return new MergeBuilder(
        JavaUtils.notMatchedClauseBuilder_insert(
            Updatable.toScalaColumnMap(assignments), this.builder));
  }

  /**
   * Defines an insert action for the not matched clause, when a row in source is not matched,
   * insert a row in target with 'assignments', where the key specifies column name and value
   * specifies its assigned value. All unspecified columns are set to NULL. Returns an updated
   * {@code MergeBuilder} with the new clause added.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, Column> assignments = new HashMap<>();
   * assignments.put("col_1", df.col("col_b"));
   * session.table("tableName").merge(df, Functions.col("col_1").equal_to(df.col("col_a")))
   *   .whenNotMatched(df.col("col_a").equal_to(Functions.lit(3)))
   *   .insertRow(assignments).collect();
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains row data.
   * @return {@code MergeBuilder}
   */
  public MergeBuilder insertRow(Map<String, Column> assignments) {
    return new MergeBuilder(
        JavaUtils.notMatchedClauseBuilder_insertRow(
            Updatable.toScalaStringColumnMap(assignments), this.builder));
  }
}
