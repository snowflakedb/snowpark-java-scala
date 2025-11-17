package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.udaf.*;

/**
 * Provides methods to register a UDAF (user-defined aggregate function) in Snowflake. {@code
 * Session.udaf()} returns an object of this class.
 *
 * <p>To register a UDAF, you must:
 *
 * <ol>
 *   <li>Define a UDAF class implementing one of the JavaUDAF[N] interfaces
 *   <li>Create an instance of that class and register it
 * </ol>
 *
 * <h2>Defining the UDAF Class</h2>
 *
 * <p>Define a class that implements one of the JavaUDAF[N] interfaces (e.g. {@link JavaUDAF0},
 * {@link JavaUDAF1}, etc.), where "N" specifies the number of input arguments.
 *
 * <p>In your class, implement the following methods:
 *
 * <ul>
 *   <li>initialize() - Initialize aggregation state
 *   <li>accumulate(state, args...) - Process each input row
 *   <li>merge(state1, state2) - Merge states from different partitions
 *   <li>finish(state) - Produce final result
 * </ul>
 *
 * <h3>Example: Custom AVG Function</h3>
 *
 * <pre>{@code
 * public class MyAvgUDAF implements JavaUDAF1<MyAvgUDAF.State, Double, Double> {
 *   public static class State implements Serializable {
 *     public double sum;
 *     public long count;
 *
 *     public State(double sum, long count) {
 *       this.sum = sum;
 *       this.count = count;
 *     }
 *   }
 *
 *   public State initialize() {
 *     return new State(0.0, 0);
 *   }
 *
 *   public State accumulate(State state, Double value) {
 *     if (value != null) {
 *       return new State(state.sum + value, state.count + 1);
 *     }
 *     return state;
 *   }
 *
 *   public State merge(State s1, State s2) {
 *     return new State(s1.sum + s2.sum, s1.count + s2.count);
 *   }
 *
 *   public Double finish(State state) {
 *     return state.count > 0 ? state.sum / state.count : null;
 *   }
 * }
 * }</pre>
 *
 * <h2>Registering and Calling the UDAF</h2>
 *
 * <h3>Registering a Temporary UDAF By Name</h3>
 *
 * <pre>{@code
 * Column myAvg = session.udaf().registerTemporary("my_avg", new MyAvgUDAF());
 * DataFrame df = session.table("sales")
 *   .groupBy("category")
 *   .agg(myAvg.apply(Functions.col("amount")).as("avg_amount"));
 * df.show();
 * }</pre>
 *
 * <h3>Registering a Permanent UDAF</h3>
 *
 * <pre>{@code
 * session.udaf().registerPermanent("my_avg", new MyAvgUDAF(), "@my_stage");
 * DataFrame df = session.table("sales")
 *   .agg(Functions.callUDAF("my_avg", Functions.col("amount")));
 * df.show();
 * }</pre>
 *
 * @since 1.16.0
 */
public class UDAFRegistration {
  private final com.snowflake.snowpark.UDAFRegistration udafRegistration;

  UDAFRegistration(com.snowflake.snowpark.UDAFRegistration udafRegistration) {
    this.udafRegistration = udafRegistration;
  }

  /**
   * Registers a UDAF instance as a temporary anonymous UDAF scoped to this session.
   *
   * @param udaf The UDAF instance to be registered
   * @param <S> State type
   * @param <RT> Return type
   * @return A Column that can be used in aggregations
   * @since 1.16.0
   */
  public <S, RT> Column registerTemporary(JavaUDAF<S, RT> udaf) {
    return new Column(JavaUtils.registerJavaUDAF(this.udafRegistration, null, udaf, null));
  }

  /**
   * Registers a UDAF instance as a temporary Snowflake UDAF that you plan to use in the session.
   *
   * @param funcName The name to refer to the UDAF
   * @param udaf The UDAF instance to be registered
   * @param <S> State type
   * @param <RT> Return type
   * @return A Column that can be used in aggregations
   * @since 1.16.0
   */
  public <S, RT> Column registerTemporary(String funcName, JavaUDAF<S, RT> udaf) {
    return new Column(JavaUtils.registerJavaUDAF(this.udafRegistration, funcName, udaf, null));
  }

  /**
   * Registers a UDAF instance as a permanent Snowflake UDAF.
   *
   * <p>The function uploads the JAR files that the UDAF depends upon to the specified stage.
   *
   * @param funcName The name to refer to the UDAF
   * @param udaf The UDAF instance to be registered
   * @param stageLocation Stage location where the JAR files will be uploaded
   * @param <S> State type
   * @param <RT> Return type
   * @return A Column that can be used in aggregations
   * @since 1.16.0
   */
  public <S, RT> Column registerPermanent(
      String funcName, JavaUDAF<S, RT> udaf, String stageLocation) {
    return new Column(
        JavaUtils.registerJavaUDAF(this.udafRegistration, funcName, udaf, stageLocation));
  }
}
