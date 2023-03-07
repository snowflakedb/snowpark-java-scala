package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * Builder for a matched clause. It provides APIs to build update and delete actions
 *
 * @since 1.1.0
 */
public class MatchedClauseBuilder {
  private final com.snowflake.snowpark.MatchedClauseBuilder builder;

  MatchedClauseBuilder(com.snowflake.snowpark.MatchedClauseBuilder builder) {
    this.builder = builder;
  }

  /**
   * Defines an update action for the matched clause, when a row in target is matched, update the
   * row in target with {@code assignments}, where the key specifies column name and value specifies
   * its assigned value. Returns an updated {@code MergeBuilder} with the new clause added.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<Column, Column> assignments = new HashMap<>();
   * assignments.put(Functions.col("col_1"), df.col("col_b"));
   * session.table("table").merge(df, Functions.col("col_1").equal_to(df.col("col_a")))
   *   .whenMatched().update(assignments).collect();
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @return {@code MergeBuilder}
   */
  public MergeBuilder update(Map<Column, Column> assignments) {
    return new MergeBuilder(
        JavaUtils.matchedClauseBuilder_update(Updatable.toScalaColumnMap(assignments), builder));
  }

  /**
   * Defines an update action for the matched clause, when a row in target is matched, update the
   * row in target with {@code assignments}, where the key specifies column name and value specifies
   * its assigned value. Returns an updated {@code MergeBuilder} with the new clause added.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, Column> assignments = new HashMap<>();
   * assignments.put("col_1", df.col("col_b"));
   * session.table("table").merge(df, Functions.col("col_1").equal_to(df.col("col_a")))
   *   .whenMatched(Functions.col("col_2").equal_to(Functions.lit(true)))
   *   .update(assignments).collect();
   * }</pre>
   *
   * @since 1.1.0
   * @param assignments A map contains the column being updated and the new value.
   * @return {@code MergeBuilder}
   */
  public MergeBuilder updateColumn(Map<String, Column> assignments) {
    return new MergeBuilder(
        JavaUtils.matchedClauseBuilder_updateColumn(
            Updatable.toScalaStringColumnMap(assignments), builder));
  }

  /**
   * Defines a delete action for the matched clause, when a row in target is matched, delete it from
   * target. Returns an updated {@code MergeBuilder} with the new clause added.
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.table("table").merge(df, Functions.col("col_1").equal_to(df.col("col_a")))
   *   .whenMatched(Functions.col("col_2").equal_to(Functions.lit(true)))
   *   .delete().collect();
   * }</pre>
   *
   * @since 1.1.0
   * @return {@code MergeBuilder}
   */
  public MergeBuilder delete() {
    return new MergeBuilder(this.builder.delete());
  }
}
