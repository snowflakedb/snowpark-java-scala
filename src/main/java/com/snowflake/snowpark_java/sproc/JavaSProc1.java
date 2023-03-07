package com.snowflake.snowpark_java.sproc;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.internal.JavaSProc;

/** A Java SProc interface that has 1 argument. */
@FunctionalInterface
public interface JavaSProc1<A1, RT> extends JavaSProc {
  RT call(Session session, A1 arg1);
}
