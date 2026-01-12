package com.snowflake.snowpark_java.test;

import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.udaf.JavaUDAF1;

public class MySumUDAF implements JavaUDAF1<long[], Long, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public long[] initialize() {
    return new long[] {0L};
  }

  @Override
  public long[] accumulate(long[] state, Integer input) {
    if (input != null) {
      state[0] += input;
    }
    return state;
  }

  @Override
  public long[] merge(long[] state1, long[] state2) {
    state1[0] += state2[0];
    return state1;
  }

  @Override
  public Long terminate(long[] state) {
    return state[0];
  }
}
