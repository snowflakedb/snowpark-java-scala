package com.snowflake.snowpark_java.udaf;

/**
 * A Java UDAF interface that has 2 arguments.
 *
 * <p>Example use cases:
 *
 * <ul>
 *   <li>COVAR_POP(x, y)
 *   <li>CORR(x, y)
 *   <li>Custom weighted average: WEIGHTED_AVG(value, weight)
 * </ul>
 *
 * @param <S> Type of the intermediate state
 * @param <A1> Type of the first argument
 * @param <A2> Type of the second argument
 * @param <RT> Type of the return value
 * @since 1.16.0
 */
public interface JavaUDAF2<S, A1, A2, RT> extends JavaUDAF<S, RT> {
  /**
   * Accumulate one row with two values into the state.
   *
   * @param state Current aggregation state
   * @param arg1 First value from the input row
   * @param arg2 Second value from the input row
   * @return Updated state
   */
  S accumulate(S state, A1 arg1, A2 arg2);
}
