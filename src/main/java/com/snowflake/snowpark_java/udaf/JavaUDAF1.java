package com.snowflake.snowpark_java.udaf;

public interface JavaUDAF1<S, O, A0> extends JavaUDAF {
  S initialize();

  S accumulate(S state, A0 arg0);

  S merge(S state1, S state2);

  O terminate(S state);
}
