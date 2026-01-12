package com.snowflake.snowpark_java.udaf;

public interface JavaUDAF0<S, O> extends JavaUDAF {
  S initialize();

  S accumulate(S state);

  S merge(S state1, S state2);

  O terminate(S state);
}
