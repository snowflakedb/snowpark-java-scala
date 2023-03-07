package com.snowflake.snowpark_java.udf;

import com.snowflake.snowpark_java.internal.JavaUDF;

/** A Java UDF interface that has 9 arguments. */
@FunctionalInterface
public interface JavaUDF9<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> extends JavaUDF {
  RT call(A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5, A6 arg6, A7 arg7, A8 arg8, A9 arg9);
}
