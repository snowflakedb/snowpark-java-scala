package com.snowflake.snowpark_java.udaf;

public interface JavaUDAF3<S, O, A0, A1, A2> extends JavaUDAF {
  S initialize();

  S accumulate(S state, A0 arg0, A1 arg1, A2 arg2);

  S merge(S state1, S state2);

  O terminate(S state);
}
