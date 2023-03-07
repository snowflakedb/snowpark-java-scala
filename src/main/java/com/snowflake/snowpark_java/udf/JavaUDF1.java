package com.snowflake.snowpark_java.udf;

import com.snowflake.snowpark_java.internal.JavaUDF;

/** A Java UDF interface that has 1 argument. */
@FunctionalInterface
public interface JavaUDF1<A1, RT> extends JavaUDF {
  RT call(A1 arg1);
}
