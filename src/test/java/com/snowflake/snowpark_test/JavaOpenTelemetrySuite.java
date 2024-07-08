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

  @Test
  public void DataFrameAsyncActor() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.async().collect();
    checkSpan(
        "snow.snowpark.DataFrameAsyncActor", "collect", "JavaOpenTelemetrySuite.java", 118, null);
    df.async().toLocalIterator();
    checkSpan(
        "snow.snowpark.DataFrameAsyncActor",
        "toLocalIterator",
        "JavaOpenTelemetrySuite.java",
        121,
        null);
    df.async().count();
    checkSpan(
        "snow.snowpark.DataFrameAsyncActor", "count", "JavaOpenTelemetrySuite.java", 128, null);
  }

  @Test
  public void dataFrameStatFunctionsCorr() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    df.stat().corr("a", "b");
    checkSpan(
        "snow.snowpark.DataFrameStatFunctions", "corr", "JavaOpenTelemetrySuite.java", 136, null);
  }

  @Test
  public void dataFrameStatFunctionsCov() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    df.stat().cov("a", "b");
    checkSpan(
        "snow.snowpark.DataFrameStatFunctions", "cov", "JavaOpenTelemetrySuite.java", 144, null);
  }

  @Test
  public void dataFrameStatFunctionsApproxQuantile() {
    DataFrame df = getSession().sql("select * from values(1), (2) as t(a)");
    double[] values = {0, 0.1, 0.4, 0.6, 1};
    df.stat().approxQuantile("a", values);
    checkSpan(
        "snow.snowpark.DataFrameStatFunctions",
        "approxQuantile",
        "JavaOpenTelemetrySuite.java",
        153,
        null);
  }

  @Test
  public void dataFrameStatFunctionsApproxQuantile2() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    double[] values = {0, 0.1, 0.6};
    String[] cols = {"a", "b"};
    df.stat().approxQuantile(cols, values);
    checkSpan(
        "snow.snowpark.DataFrameStatFunctions",
        "approxQuantile",
        "JavaOpenTelemetrySuite.java",
        167,
        null);
  }

  @Test
  public void dataFrameStatFunctionsCrosstab() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    df.stat().crosstab("a", "b");
    checkSpan(
        "snow.snowpark.DataFrameStatFunctions",
        "crosstab",
        "JavaOpenTelemetrySuite.java",
        179,
        null);
  }

  @Test
  public void dataFrameWriterCsv() {
    String name = randomName();
    try {
      createTempStage(name);
      testSpanExporter.reset();
      DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
      df.write().csv("@" + name + "/csv");
      checkSpan("snow.snowpark.DataFrameWriter", "csv", "JavaOpenTelemetrySuite.java", 195, null);
    } finally {
      dropStage(name);
    }
  }

  @Test
  public void dataFrameWriterJson() {
    String name = randomName();
    try {
      createTempStage(name);
      testSpanExporter.reset();
      DataFrame df = getSession().sql("select * from values(1, 2) as t(a, b)");
      DataFrame df2 =
          df.select(
              com.snowflake.snowpark_java.Functions.array_construct(df.col("a"), df.col("b")));
      df2.write().json("@" + name + "/json");
      checkSpan("snow.snowpark.DataFrameWriter", "json", "JavaOpenTelemetrySuite.java", 212, null);
    } finally {
      dropStage(name);
    }
  }
}
