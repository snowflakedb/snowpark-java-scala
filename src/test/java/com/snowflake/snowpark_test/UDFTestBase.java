package com.snowflake.snowpark_test;

import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.Session;
import java.util.HashMap;
import java.util.Map;

public abstract class UDFTestBase extends TestFunctions {
  protected String defaultProfile = TestUtils.defaultProfile();

  private Session session = null;

  // Session instance needs to created in getSession() so that
  // it can be overridden when running the suite in stored proc.
  public Session getSession() {
    if (session == null) {
      session = createSession();
    }
    return session;
  }

  protected Session createSession() {
    Session newSession = Session.builder().configFile(defaultProfile).create();
    if (JavaUtils.snowparkScalaCompatVersion().equals("2.13")) {
      newSession.sql("alter session set ENABLE_SCALA_UDF_RUNTIME_2_13=true").collect();
    }
    return newSession;
  }

  protected void runQuery(String sql) {
    getSession().sql(sql).collect();
  }

  protected void createStage(String stageName, boolean isTemporary) {
    String sql = "create or replace ";
    if (isTemporary) {
      sql += "temporary ";
    }
    sql += "stage " + stageName;
    runQuery(sql);
  }

  protected void dropStage(String stageName) {
    String sql = "drop stage if exists " + stageName;
    runQuery(sql);
  }

  protected void addDepsToClassPath(Session session) {
    TestUtils.addDepsToClassPathJava(session, null);
  }

  protected void addDepsToClassPath(Session session, String stageName) {
    TestUtils.addDepsToClassPathJava(session, stageName);
  }

  protected void udafTest(TestMethod thunk, Session session) {
    Map<String, String> params = new HashMap<>();
    params.put("ENABLE_JAVA_UDAF", "TRUE");
    try {
      params.forEach(
          (name, value) -> session.sql("alter session set " + name + " = " + value).collect());
      thunk.run();
    } finally {
      params.forEach((name, value) -> session.sql("alter session unset " + name).collect());
    }
  }
}
