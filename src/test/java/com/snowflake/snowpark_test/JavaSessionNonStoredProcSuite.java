// creating API test in different package,
// to make sure all API can be accessed from public
package com.snowflake.snowpark_test;

import static org.junit.Assert.assertThrows;

import com.snowflake.snowpark.SnowparkClientException;
import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark_java.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.Test;

// This suite includes test case which can't be run in java stored proc
public class JavaSessionNonStoredProcSuite extends TestBase {

  // For stored proc, the caller setup the session. So the closure cleaner is default.
  // But for SP, snowpark will skip closure cleaner anyway, so just run this test case
  // in JavaSessionNonStoredProcSuite
  @Test
  public void disableClosureCleaner() {
    assert TestUtils.closureCleanerDisabled(JavaToScalaConvertor.javaToScalaSession(getSession()));
  }

  @Test
  public void simpleRoundTrip() {
    Session newSession = Session.builder().configFile(defaultProfile).create();
    DataFrame df = newSession.sql("select 1");
    assert df.count() == 1;
  }

  @Test
  public void getDependencies() {
    Session newSession = Session.builder().configFile(defaultProfile).create();
    String classDir = Session.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    newSession.addDependency(classDir);
    Set<URI> set = newSession.getDependencies();
    // set should be a copy but not a point
    newSession.removeDependency(classDir);
    assert set.size() == 1;
    set.forEach(
        uri -> {
          assert classDir.equals(uri.getPath());
        });
    assert newSession.getDependencies().isEmpty();
  }

  @Test
  public void tags() {
    String tag = randomName();
    getSession().setQueryTag(tag);
    assert getSession().getQueryTag().isPresent();
    assert getSession().getQueryTag().get().equals(tag);
    getSession().unsetQueryTag();
    assert !getSession().getQueryTag().isPresent();
  }

  @Test
  public void updateQueryTagAddNewKeyValuePairs() {
    String queryTag1 = "{\"key1\":\"value1\"}";
    getSession().setQueryTag(queryTag1);

    String queryTag2 = "{\"key2\":\"value2\",\"key3\":{\"key4\":0},\"key5\":{\"key6\":\"value6\"}}";
    getSession().updateQueryTag(queryTag2);

    String expected =
        "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":{\"key4\":0},\"key5\":{\"key6\":\"value6\"}}";
    assert getSession().getQueryTag().isPresent();
    assert getSession().getQueryTag().get().equals(expected);
  }

  @Test
  public void updateQueryTagUpdateKeyValuePairs() {
    String queryTag1 = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}";
    getSession().setQueryTag(queryTag1);

    String queryTag2 = "{\"key2\":\"newValue2\"}";
    getSession().updateQueryTag(queryTag2);

    String expected = "{\"key1\":\"value1\",\"key2\":\"newValue2\",\"key3\":\"value3\"}";
    assert getSession().getQueryTag().isPresent();
    assert getSession().getQueryTag().get().equals(expected);
  }

  @Test
  public void updateQueryTagEmptySessionQueryTag() {
    getSession().setQueryTag("");

    String queryTag = "{\"key1\":\"value1\"}";
    getSession().updateQueryTag(queryTag);

    assert getSession().getQueryTag().isPresent();
    assert getSession().getQueryTag().get().equals(queryTag);
  }

  @Test
  public void updateQueryTagInvalidInputQueryTag() {
    String queryTag = "tag1";

    SnowparkClientException exception =
        assertThrows(SnowparkClientException.class, () -> getSession().updateQueryTag(queryTag));
    assert exception
        .getMessage()
        .equals(
            "Error Code: 0426, Error message: The given query tag must be a valid JSON string."
                + " Ensure it's correctly formatted as JSON.");
  }

  @Test
  public void updateQueryTagInvalidSessionQueryTag() {
    String queryTag1 = "tag1";
    getSession().setQueryTag(queryTag1);

    String queryTag2 = "{\"key1\":\"value1\"}";
    SnowparkClientException exception =
        assertThrows(SnowparkClientException.class, () -> getSession().updateQueryTag(queryTag2));
    assert exception
        .getMessage()
        .equals(
            "Error Code: 0427, Error message: The query tag of the current session must be a valid"
                + " JSON string. Current query tag: tag1");
  }

  @Test
  public void updateQueryTagFromAlterSession() {
    getSession().sql("ALTER SESSION SET QUERY_TAG = '{\"key1\":\"value1\"}'").collect();

    String queryTag2 = "{\"key2\":\"value2\"}";
    getSession().updateQueryTag(queryTag2);

    String expected = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
    assert getSession().getQueryTag().isPresent();
    assert getSession().getQueryTag().get().equals(expected);
  }

  @Test
  public void dbAndSchema() {
    assert getSession()
        .getCurrentSchema()
        .get()
        .equalsIgnoreCase(getSession().getDefaultSchema().get());
    assert getSession()
        .getCurrentDatabase()
        .get()
        .equalsIgnoreCase(getSession().getDefaultDatabase().get());

    Session newSession = Session.builder().configFile(defaultProfile).create();
    String randomSchema = randomName();
    try {
      newSession.sql("create schema " + randomSchema).collect();
      assert newSession.getCurrentSchema().get().equalsIgnoreCase("\"" + randomSchema + "\"");
      String fullName = getSession().getDefaultDatabase().get() + ".\"" + randomSchema + "\"";
      assert newSession.getFullyQualifiedCurrentSchema().equalsIgnoreCase(fullName);
    } finally {
      newSession.sql("drop schema if exists " + randomSchema).collect();
    }
  }

  @Test(expected = SnowparkClientException.class)
  public void close() {
    Session newSession = Session.builder().configFile(defaultProfile).create();
    newSession.close();
    newSession.sql("select 1").show();
  }

  @Test
  public void sessionBuilderConfig() {
    boolean hasError = false;
    try {
      Session.builder().configFile(defaultProfile).config("DB", "badNotExistingDB").create();
    } catch (Exception ex) {
      assert ex instanceof SnowflakeSQLException;
      assert ex.getMessage().contains("does not exist");
      hasError = true;
    }
    assert hasError;
  }

  @Test
  public void sessionBuilderConfigs() {
    boolean hasError = false;
    Map<String, String> options = new HashMap<>();
    options.put("DB", "badNotExistingDB");
    try {
      Session.builder().configFile(defaultProfile).configs(options).create();
    } catch (Exception ex) {
      assert ex instanceof SnowflakeSQLException;
      assert ex.getMessage().contains("does not exist");
      hasError = true;
    }
    assert hasError;
  }

  @Test
  public void appName() {
    String appName = "my-app";
    String expectedAppName = String.format("{\"APPNAME\":\"%s\"}", appName);
    Session session = Session.builder().configFile(defaultProfile).appName(appName).create();
    assert (expectedAppName.equals(session.getQueryTag().get()));
  }
}
