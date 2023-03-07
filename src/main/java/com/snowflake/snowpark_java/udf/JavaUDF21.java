package com.snowflake.snowpark_java.udf;

import com.snowflake.snowpark_java.internal.JavaUDF;

/** A Java UDF interface that has 21 arguments. */
@FunctionalInterface
public interface JavaUDF21<
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11,
        A12,
        A13,
        A14,
        A15,
        A16,
        A17,
        A18,
        A19,
        A20,
        A21,
        RT>
    extends JavaUDF {
  RT call(
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
      A15 arg15,
      A16 arg16,
      A17 arg17,
      A18 arg18,
      A19 arg19,
      A20 arg20,
      A21 arg21);
}
