package com.snowflake.snowpark_java.udf;

import com.snowflake.snowpark_java.internal.JavaUDF;

/** A Java UDF interface that has 5 arguments. */
@FunctionalInterface
public interface JavaUDF5<A1, A2, A3, A4, A5, RT> extends JavaUDF {
  RT call(A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5);
}
