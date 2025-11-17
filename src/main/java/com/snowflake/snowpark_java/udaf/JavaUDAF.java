package com.snowflake.snowpark_java.udaf;

import java.io.Serializable;

/**
 * The base interface for JavaUDAF[N].
 *
 * <p>A User-Defined Aggregate Function (UDAF) processes multiple input rows and produces a single
 * aggregated result.
 *
 * <p>Unlike UDF (single row -> single value) or UDTF (single row -> multiple rows), UDAF aggregates
 * multiple rows into a single value.
 *
 * <h3>UDAF Execution Model</h3>
 *
 * <ol>
 *   <li><b>initialize()</b> - Initialize aggregation state (called once per partition)
 *   <li><b>accumulate(state, values...)</b> - Process each input row (called for each row)
 *   <li><b>merge(state1, state2)</b> - Combine states from partitions (for parallel execution)
 *   <li><b>finish(state)</b> - Produce final result from state (called once)
 * </ol>
 *
 * @param <S> Type of the intermediate state
 * @param <RT> Type of the return value
 * @since 1.16.0
 */
public interface JavaUDAF<S, RT> extends Serializable {

  /**
   * Initialize the aggregation state. Called once at the beginning of each partition.
   *
   * @return Initial state
   */
  S initialize();

  /**
   * Merge two states from different partitions or groups. This enables parallel aggregation across
   * multiple partitions.
   *
   * @param state1 First state
   * @param state2 Second state
   * @return Merged state
   */
  S merge(S state1, S state2);

  /**
   * Finalize the aggregation and produce the result. Called once after all rows have been
   * accumulated.
   *
   * @param state Final aggregation state
   * @return Final result
   */
  RT finish(S state);
}
