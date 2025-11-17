package com.snowflake.snowpark_java.udaf;

/**
 * A Java UDAF interface that has 0 arguments.
 *
 * <p>Example use case: COUNT(*)
 *
 * @param <S> Type of the intermediate state
 * @param <RT> Type of the return value
 * @since 1.16.0
 */
public interface JavaUDAF0<S, RT> extends JavaUDAF<S, RT> {
  /**
   * Accumulate one row into the state.
   *
   * @param state Current aggregation state
   * @return Updated state
   */
  S accumulate(S state);
}
