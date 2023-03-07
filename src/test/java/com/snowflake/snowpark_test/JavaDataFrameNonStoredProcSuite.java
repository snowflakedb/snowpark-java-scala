package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.*;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.Test;

public class JavaDataFrameNonStoredProcSuite extends TestBase {

  @Test
  public void createOrReplaceView() {
    String viewName1 = randomName();
    String viewName2 = randomName();
    DataFrame df1 = getSession().sql("select * from values(1), (2) as t(a)");
    DataFrame df2 = getSession().sql("select * from values(3), (4) as t(a)");
    Session newSession = Session.builder().configFile(defaultProfile).create();
    try {
      df1.createOrReplaceView(viewName1);
      df2.createOrReplaceView(new String[] {viewName2});
      // change session
      DataFrame df3 = newSession.table(viewName1);
      DataFrame df4 = newSession.table(viewName2);
      checkAnswer(df3, df1.collect());
      checkAnswer(df4, df2.collect());
    } finally {
      dropView(viewName1);
      dropView(viewName2);
    }
  }

  @Test(expected = SnowflakeSQLException.class)
  public void createOrReplaceTempView1() {
    String viewName = randomName();
    DataFrame df = getSession().sql("select * from values(1), (2) as t(a)");
    df.createOrReplaceTempView(viewName);
    Session newSession = Session.builder().configFile(defaultProfile).create();
    newSession.table(viewName).collect();
  }

  @Test(expected = SnowflakeSQLException.class)
  public void createOrReplaceTempView2() {
    String viewName = randomName();
    DataFrame df = getSession().sql("select * from values(1), (2) as t(a)");
    df.createOrReplaceTempView(new String[] {viewName});
    Session newSession = Session.builder().configFile(defaultProfile).create();
    newSession.table(viewName).collect();
  }
}
