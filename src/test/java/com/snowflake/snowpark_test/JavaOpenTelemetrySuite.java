package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.DataFrame;
import org.junit.Test;

public class JavaOpenTelemetrySuite extends JavaOpenTelemetryEnabled {

  @Test
  public void cacheResult() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.cacheResult();
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "JavaOpenTelemetrySuite.java", 11, null);
  }

  @Test
  public void count() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.count();
    checkSpan("snow.snowpark.DataFrame", "count", "JavaOpenTelemetrySuite.java", 18, null);
  }

  @Test
  public void collect() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.collect();
    checkSpan("snow.snowpark.DataFrame", "collect", "JavaOpenTelemetrySuite.java", 25, null);
  }

  @Test
  public void toLocalIterator() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.toLocalIterator();
    checkSpan(
        "snow.snowpark.DataFrame", "toLocalIterator", "JavaOpenTelemetrySuite.java", 32, null);
  }

  @Test
  public void show() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.show();
    checkSpan("snow.snowpark.DataFrame", "show", "JavaOpenTelemetrySuite.java", 40, null);
    df.show(1);
    checkSpan("snow.snowpark.DataFrame", "show", "JavaOpenTelemetrySuite.java", 42, null);
    df.show(1, 100);
    checkSpan("snow.snowpark.DataFrame", "show", "JavaOpenTelemetrySuite.java", 44, null);
  }

  @Test
  public void createOrReplaceView() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    String name = randomName();
    try {
      df.createOrReplaceView(name);
      checkSpan(
          "snow.snowpark.DataFrame",
          "createOrReplaceView",
          "JavaOpenTelemetrySuite.java",
          53,
          null);
      String[] names = {name};
      df.createOrReplaceView(names);
      checkSpan(
          "snow.snowpark.DataFrame",
          "createOrReplaceView",
          "JavaOpenTelemetrySuite.java",
          61,
          null);
    } finally {
      dropView(name);
    }
  }

  @Test
  public void createOrReplaceTempView() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    String name = randomName();
    try {
      df.createOrReplaceTempView(name);
      checkSpan(
          "snow.snowpark.DataFrame",
          "createOrReplaceTempView",
          "JavaOpenTelemetrySuite.java",
          78,
          null);
      String[] names = {name};
      df.createOrReplaceTempView(names);
      checkSpan(
          "snow.snowpark.DataFrame",
          "createOrReplaceTempView",
          "JavaOpenTelemetrySuite.java",
          86,
          null);
    } finally {
      dropView(name);
    }
  }

  @Test
  public void first() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.first();
    checkSpan("snow.snowpark.DataFrame", "first", "JavaOpenTelemetrySuite.java", 101, null);
    df.first(1);
    checkSpan("snow.snowpark.DataFrame", "first", "JavaOpenTelemetrySuite.java", 103, null);
  }

  @Test
  public void randomSplit() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    double[] weight = {0.5, 0.5};
    df.randomSplit(weight);
    checkSpan("snow.snowpark.DataFrame", "randomSplit", "JavaOpenTelemetrySuite.java", 111, null);
  }
}
