package com.snowflake.snowpark_java.sproc;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.internal.JavaSProc;

/** A Java SProc interface that has 6 arguments. */
@FunctionalInterface
public interface JavaSProc6<A1, A2, A3, A4, A5, A6, RT> extends JavaSProc {
  RT call(Session session, A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5, A6 arg6);
}
