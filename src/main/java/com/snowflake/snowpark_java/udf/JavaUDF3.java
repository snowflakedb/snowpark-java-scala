package com.snowflake.snowpark_java.udf;

import com.snowflake.snowpark_java.internal.JavaUDF;

/** A Java UDF interface that has 3 arguments. */
@FunctionalInterface
public interface JavaUDF3<A1, A2, A3, RT> extends JavaUDF {
  RT call(A1 arg1, A2 arg2, A3 arg3);
}
