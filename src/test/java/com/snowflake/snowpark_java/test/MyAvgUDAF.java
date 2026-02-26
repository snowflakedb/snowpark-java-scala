package com.snowflake.snowpark_java.test;

import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.udaf.JavaUDAF1;
import java.io.Serializable;

/**
 * Java UDAF that computes average using a custom state class. Demonstrates using a class to hold
 * aggregate state instead of primitive arrays.
 */
public class MyAvgUDAF implements JavaUDAF1<MyAvgUDAF.AvgState, Double, Integer> {

  /**
   * State class to hold sum and count for computing average. Must be Serializable and have a no-arg
   * constructor for Kryo deserialization.
   */
  public static class AvgState implements Serializable {
    public long sum = 0L;
    public long count = 0L;

    public AvgState() {}

    public AvgState(long sum, long count) {
      this.sum = sum;
      this.count = count;
    }
  }

  @Override
  public DataType outputType() {
    return DataTypes.DoubleType;
  }

  @Override
  public AvgState initialize() {
    return new AvgState();
  }

  @Override
  public AvgState accumulate(AvgState state, Integer input) {
    if (input != null) {
      state.sum += input;
      state.count += 1;
    }
    return state;
  }

  @Override
  public AvgState merge(AvgState state1, AvgState state2) {
    state1.sum += state2.sum;
    state1.count += state2.count;
    return state1;
  }

  @Override
  public Double terminate(AvgState state) {
    if (state.count == 0) {
      return 0.0;
    }
    return (double) state.sum / state.count;
  }
}
