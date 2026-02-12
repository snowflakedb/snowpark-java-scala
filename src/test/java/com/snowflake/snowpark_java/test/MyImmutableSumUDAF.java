package com.snowflake.snowpark_java.test;

import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.udaf.JavaUDAF1;

/**
 * A sum UDAF that uses Long (immutable) as state instead of long[] (mutable). This tests that the
 * generated wrapper code correctly uses the return value of accumulate() and merge() rather than
 * assuming the state is mutated in place.
 */
public class MyImmutableSumUDAF implements JavaUDAF1<Long, Long, Integer> {
  @Override
  public DataType outputType() {
    return DataTypes.LongType;
  }

  @Override
  public Long initialize() {
    return 0L;
  }

  @Override
  public Long accumulate(Long state, Integer input) {
    return state + (input != null ? input : 0);
  }

  @Override
  public Long merge(Long state1, Long state2) {
    return state1 + state2;
  }

  @Override
  public Long terminate(Long state) {
    return state;
  }
}
