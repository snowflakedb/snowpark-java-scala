package com.snowflake.snowpark_java.sproc;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.internal.JavaSProc;

/** A Java SProc interface that has 3 arguments. */
@FunctionalInterface
public interface JavaSProc3<A1, A2, A3, RT> extends JavaSProc {
  RT call(Session session, A1 arg1, A2 arg2, A3 arg3);
}
