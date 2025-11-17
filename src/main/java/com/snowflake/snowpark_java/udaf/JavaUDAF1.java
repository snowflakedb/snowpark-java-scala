package com.snowflake.snowpark_java.udaf;

/**
 * A Java UDAF interface that has 1 argument.
 *
 * <p>This is the most common UDAF signature, covering functions like:
 *
 * <ul>
 *   <li>SUM(column)
 *   <li>AVG(column)
 *   <li>MAX(column)
 *   <li>MIN(column)
 *   <li>COUNT(column)
 *   <li>STDDEV(column)
 * </ul>
 *
 * @param <S> Type of the intermediate state
 * @param <A1> Type of the first argument
 * @param <RT> Type of the return value
 * @since 1.16.0
 */
public interface JavaUDAF1<S, A1, RT> extends JavaUDAF<S, RT> {
  /**
   * Accumulate one row with a value into the state.
   *
   * @param state Current aggregation state
   * @param arg1 Value from the input row
   * @return Updated state
   */
  S accumulate(S state, A1 arg1);
}
