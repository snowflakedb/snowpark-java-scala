package com.snowflake.snowpark_test;

import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark_java.JavaToScalaConvertor;
import com.snowflake.snowpark_java.Session;

public abstract class TestBase extends TestFunctions {

  protected static String defaultProfile = TestUtils.defaultProfile();

  // don't initialize it, otherwise it will
  // try to create a session on server side in UDF tests
  private static Session _session = null;

  protected Session getSession() {
    if (_session == null) {
      TestUtils.tryToLoadFipsProvider();
      _session = Session.builder().configFile(defaultProfile).create();
    }
    return _session;
  }

  protected void runQuery(String sql) {
    getSession().sql(sql).collect();
  }

  protected void createTempStage(String stageName) {
    runQuery("create or replace temp stage " + stageName);
  }

  protected void dropStage(String stageName) {
    runQuery("drop stage if exists " + stageName);
  }

  protected void dropTable(String tableName) {
    runQuery("drop table if exists " + tableName);
  }

  protected void dropView(String viewName) {
    runQuery("drop view if exists " + viewName);
  }

  protected void createTable(String tableName, String schema, boolean isTemp) {
    String sql = "create or replace ";
    if (isTemp) {
      sql += "temporary ";
    }
    sql += "table " + tableName + " (" + schema + ")";
    runQuery(sql);
  }

  protected void createTable(String tableName, String schema) {
    createTable(tableName, schema, true);
  }

  protected void uploadFileToStage(String stageName, String fileName, boolean compress) {
    TestUtils.uploadFileToStage(
        stageName, fileName, compress, JavaToScalaConvertor.javaToScalaSession(getSession()));
  }
}
