package com.snowflake.snowpark_java.test;

import com.snowflake.snowpark_java.types.DataType;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.udaf.JavaUDAF1;

/**
 * A sum UDAF with a large captured array, causing the serialized closure to exceed
 * MAX_INLINE_CLOSURE_SIZE_BYTES (8KB). This tests the large closure file upload path in the
 * generated UDAF wrapper code.
 */
public class MyLargeClosureUDAF implements JavaUDAF1<Long, Long, Integer> {
  // Large array to inflate the serialized size beyond 8KB
  private final Integer[] largeData;

  public MyLargeClosureUDAF(int count) {
    largeData = new Integer[count];
    for (int i = 0; i < count; i++) {
      largeData[i] = i;
    }
  }

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
