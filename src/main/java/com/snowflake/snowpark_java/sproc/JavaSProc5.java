package com.snowflake.snowpark_java.sproc;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.internal.JavaSProc;

/** A Java SProc interface that has 5 arguments. */
@FunctionalInterface
public interface JavaSProc5<A1, A2, A3, A4, A5, RT> extends JavaSProc {
  RT call(Session session, A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5);
}
