package com.snowflake.snowpark_java;

/**
 * A DataFrame that returns cached data. Repeated invocations of actions on this type of dataframe
 * are guaranteed to produce the same results. It is returned from `cacheResult` functions (e.g.
 * {@code DataFrame.cacheResult}).
 *
 * @since 0.12.0
 */
public class HasCachedResult extends DataFrame {
  HasCachedResult(com.snowflake.snowpark.HasCachedResult df) {
    super(df);
  }
}
