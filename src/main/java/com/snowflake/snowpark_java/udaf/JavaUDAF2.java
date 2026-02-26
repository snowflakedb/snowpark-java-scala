package com.snowflake.snowpark_java.udaf;

public interface JavaUDAF2<S, O, A0, A1> extends JavaUDAF {
  S initialize();

  S accumulate(S state, A0 arg0, A1 arg1);

  S merge(S state1, S state2);

  O terminate(S state);
}
