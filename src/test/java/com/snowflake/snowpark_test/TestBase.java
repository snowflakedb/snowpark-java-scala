package com.snowflake.snowpark_test;

import com.snowflake.snowpark.TestMethod;
import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark_java.JavaToScalaConvertor;
import com.snowflake.snowpark_java.Session;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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

  Optional<Boolean> isPreprodAccount = Optional.empty();

  protected boolean isPreprodAccount() {
    if (!isPreprodAccount.isPresent()) {
      isPreprodAccount =
          Optional.of(
              !getSession()
                  .sql("select current_account()")
                  .collect()[0]
                  .getString(0)
                  .contains("SFCTEST0"));
    }
    return isPreprodAccount.get();
  }

  protected void structuredTypeTest(TestMethod thunk, Session session) {
    Map<String, String> map = new HashMap<>();
    map.put("ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE", "true");
    map.put("IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE", "true");
    map.put("FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT", "true");
    map.put("ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT", "true");
    map.put("ENABLE_STRUCTURED_TYPES_IN_BINDS", "enable");
    // disable these tests on preprod daily tests until these parameters are enabled by default.
    withSessionParameters(map, session, true, thunk);
  }

  protected void withSessionParameters(
      Map<String, String> params, Session session, boolean skipPreprod, TestMethod thunk) {
    if (!(skipPreprod && isPreprodAccount())) {
      try {
        params.forEach(
            (name, value) -> session.sql("alter session set " + name + " = " + value).collect());
        thunk.run();
      } finally {
        params.forEach((name, value) -> session.sql("alter session unset " + name).collect());
      }
    }
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
