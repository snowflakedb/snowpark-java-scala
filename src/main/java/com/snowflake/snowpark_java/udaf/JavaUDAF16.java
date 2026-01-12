package com.snowflake.snowpark_java.udaf;

public interface JavaUDAF16<
        S, O, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15>
    extends JavaUDAF {
  S initialize();

  S accumulate(
      S state,
      A0 arg0,
      A1 arg1,
      A2 arg2,
      A3 arg3,
      A4 arg4,
      A5 arg5,
      A6 arg6,
      A7 arg7,
      A8 arg8,
      A9 arg9,
      A10 arg10,
      A11 arg11,
      A12 arg12,
      A13 arg13,
      A14 arg14,
      A15 arg15);

  S merge(S state1, S state2);

  O terminate(S state);
}
