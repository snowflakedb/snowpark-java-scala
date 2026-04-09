package com.snowflake.snowpark_test;

import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.JavaToScalaConvertor;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public abstract class TestFunctions {

  protected void withTimeZoneTest(TestMethod thunk, Session session) {
    String sysTimeZone = System.getProperty("user.timezone", "");
    TimeZone oldTimeZone = TimeZone.getDefault();
    boolean isStoredProc =
        (Boolean) JavaToScalaConvertor.javaToScalaSession(session).conn().isStoredProc();
    String oldSfTimezone =
        isStoredProc
            ? session.sql("select CURRENT_TIMEZONE()").collect()[0].getString(0)
            : session.sql("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION").collect()[0].getString(1);
    String testTimezone = isStoredProc ? oldSfTimezone : "America/Los_Angeles";
    try {
      System.setProperty("user.timezone", testTimezone);
      TimeZone.setDefault(TimeZone.getTimeZone(testTimezone));
      if (!isStoredProc)
        session.sql(String.format("alter session set TIMEZONE = '%s'", testTimezone)).collect();
      thunk.run();
    } finally {
      System.setProperty("user.timezone", sysTimeZone);
      TimeZone.setDefault(oldTimeZone);
      if (!isStoredProc)
        session.sql("alter session set TIMEZONE = '" + oldSfTimezone + "'").collect();
    }
  }

  private void checkAnswer(Row[] result, List<Row> expected, boolean sort) {
    com.snowflake.snowpark.Row[] scalaRows = new com.snowflake.snowpark.Row[result.length];
    for (int i = 0; i < result.length; i++) {
      scalaRows[i] = JavaToScalaConvertor.javaToScalaRow(result[i]);
    }

    List<com.snowflake.snowpark.Row> scalaList = new ArrayList<>(expected.size());
    for (Row row : expected) {
      scalaList.add(JavaToScalaConvertor.javaToScalaRow(row));
    }

    TestUtils.checkResult(scalaRows, scalaList, sort);
  }

  protected void checkAnswer(DataFrame df, Row[] expected) {
    Row[] result = df.collect();
    checkAnswer(result, Arrays.asList(expected), true);
  }

  protected void checkAnswer(DataFrame df1, DataFrame df2) {
    checkAnswer(df1, df2.collect());
  }

  protected String randomName() {
    return TestUtils.randomTableName();
  }

  protected String randomTableName() {
    return TestUtils.randomTableName();
  }

  protected String randomFunctionName() {
    return TestUtils.randomFunctionName();
  }

  protected String randomStageName() {
    return TestUtils.randomStageName();
  }
}
