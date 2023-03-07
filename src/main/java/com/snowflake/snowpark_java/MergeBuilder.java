package com.snowflake.snowpark_java;

/**
 * Builder for a merge action. It provides APIs to build matched and not matched clauses.
 *
 * @since 1.1.0
 */
public class MergeBuilder {
  private final com.snowflake.snowpark.MergeBuilder builder;

  MergeBuilder(com.snowflake.snowpark.MergeBuilder builder) {
    this.builder = builder;
  }

  /**
   * Adds a matched clause into the merge action. It matches all remaining rows in target that
   * satisfy 'joinExpr'. Returns a {@code MatchedClauseBuilder} which provides APIs to define
   * actions to take when a row is matched.
   *
   * <p>Caution: Since it matches all remaining rows, no more whenMatched calls will be accepted
   * beyond this call.
   *
   * @since 1.1.0
   * @return {@code MatchedClauseBuilder}
   */
  public MatchedClauseBuilder whenMatched() {
    return new MatchedClauseBuilder(builder.whenMatched());
  }

  /**
   * Adds a matched clause into the merge action. It matches all rows in target that satisfy
   * 'joinExpr' while also satisfying 'condition'. Returns a {@code MatchedClauseBuilder} which
   * provides APIs to define actions to take when a row is matched.
   *
   * @since 1.1.0
   * @param condition The condition expression
   * @return {@code MatchedClauseBuilder}
   */
  public MatchedClauseBuilder whenMatched(Column condition) {
    return new MatchedClauseBuilder(builder.whenMatched(condition.toScalaColumn()));
  }

  /**
   * Adds a not matched clause into the merge action. It matches all remaining rows in source that
   * do not satisfy 'joinExpr'. Returns a {@code MatchedClauseBuilder} which provides APIs to define
   * actions to take when a row is not matched.
   *
   * <p>Caution: Since it matches all remaining rows, no more whenNotMatched calls will be accepted
   * beyond this call.
   *
   * @since 1.1.0
   * @return {@code NotMatchedClauseBuilder}
   */
  public NotMatchedClauseBuilder whenNotMatched() {
    return new NotMatchedClauseBuilder(builder.whenNotMatched());
  }

  /**
   * Adds a matched clause into the merge action. It matches all rows in target that satisfy
   * 'joinExpr' while also satisfying 'condition'. Returns a {@code MatchedClauseBuilder} which
   * provides APIs to define actions to take when a row is matched.
   *
   * @since 1.1.0
   * @param condition The condition expression
   * @return {@code MatchedClauseBuilder}
   */
  public NotMatchedClauseBuilder whenNotMatched(Column condition) {
    return new NotMatchedClauseBuilder(builder.whenNotMatched(condition.toScalaColumn()));
  }

  /**
   * Executes the merge action and returns a {@code MergeResult}, representing number of rows
   * inserted, updated and deleted by this merge action.
   *
   * @since 1.1.0
   * @return {@code MergeResult}
   */
  public MergeResult collect() {
    return new MergeResult(this.builder.collect());
  }

  /**
   * Returns a {@code MergeBuilderAsyncActor} object that can be used to execute MergeBuilder
   * actions asynchronously.
   *
   * @since 1.3.0
   * @return A {@code MergeBuilderAsyncActor} object
   */
  public MergeBuilderAsyncActor async() {
    return new MergeBuilderAsyncActor(this.builder.async(), builder.target().session());
  }
}
