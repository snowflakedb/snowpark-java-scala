package com.snowflake.snowpark_java.udaf;

public interface JavaUDAF9<S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8> extends JavaUDAF {
  S initialize();

  S accumulate(
      S state, A0 arg0, A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5, A6 arg6, A7 arg7, A8 arg8);

  S merge(S state1, S state2);

  O terminate(S state);
}
