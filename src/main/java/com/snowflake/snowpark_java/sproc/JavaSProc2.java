package com.snowflake.snowpark_java.sproc;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.internal.JavaSProc;

/** A Java SProc interface that has 2 arguments. */
@FunctionalInterface
public interface JavaSProc2<A1, A2, RT> extends JavaSProc {
  RT call(Session session, A1 arg1, A2 arg2);
}
