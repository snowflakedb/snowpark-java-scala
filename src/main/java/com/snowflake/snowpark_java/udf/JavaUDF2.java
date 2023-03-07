package com.snowflake.snowpark_java.udf;

import com.snowflake.snowpark_java.internal.JavaUDF;

/** A Java UDF interface that has 2 arguments. */
@FunctionalInterface
public interface JavaUDF2<A1, A2, RT> extends JavaUDF {
  RT call(A1 arg1, A2 arg2);
}
