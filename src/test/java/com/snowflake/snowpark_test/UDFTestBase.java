package com.snowflake.snowpark_test;

import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark_java.Session;

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
    return Session.builder().configFile(defaultProfile).create();
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
}
