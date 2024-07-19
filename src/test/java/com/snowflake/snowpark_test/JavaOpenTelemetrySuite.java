package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaOpenTelemetrySuite extends JavaOpenTelemetryEnabled {

  @Test
  public void cacheResult() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.cacheResult();
    checkSpan("snow.snowpark.DataFrame", "cacheResult");
  }

  @Test
  public void count() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.count();
    checkSpan("snow.snowpark.DataFrame", "count");
  }

  @Test
  public void collect() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.collect();
    checkSpan("snow.snowpark.DataFrame", "collect");
  }

  @Test
  public void toLocalIterator() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.toLocalIterator();
    checkSpan("snow.snowpark.DataFrame", "toLocalIterator");
  }

  @Test
  public void show() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.show();
    checkSpan("snow.snowpark.DataFrame", "show");
    df.show(1);
    checkSpan("snow.snowpark.DataFrame", "show");
    df.show(1, 100);
    checkSpan("snow.snowpark.DataFrame", "show");
  }

  @Test
  public void createOrReplaceView() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    String name = randomName();
    try {
      df.createOrReplaceView(name);
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceView");
      String[] names = {name};
      df.createOrReplaceView(names);
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceView");
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
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceTempView");
      String[] names = {name};
      df.createOrReplaceTempView(names);
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceTempView");
    } finally {
      dropView(name);
    }
  }

  @Test
  public void first() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.first();
    checkSpan("snow.snowpark.DataFrame", "first");
    df.first(1);
    checkSpan("snow.snowpark.DataFrame", "first");
  }

  @Test
  public void randomSplit() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    double[] weight = {0.5, 0.5};
    df.randomSplit(weight);
    checkSpan("snow.snowpark.DataFrame", "randomSplit");
  }

  @Test
  public void DataFrameAsyncActor() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    df.async().collect();
    checkSpan("snow.snowpark.DataFrameAsyncActor", "collect");
    df.async().toLocalIterator();
    checkSpan("snow.snowpark.DataFrameAsyncActor", "toLocalIterator");
    df.async().count();
    checkSpan("snow.snowpark.DataFrameAsyncActor", "count");
  }

  @Test
  public void dataFrameStatFunctionsCorr() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    df.stat().corr("a", "b");
    checkSpan("snow.snowpark.DataFrameStatFunctions", "corr");
  }

  @Test
  public void dataFrameStatFunctionsCov() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    df.stat().cov("a", "b");
    checkSpan("snow.snowpark.DataFrameStatFunctions", "cov");
  }

  @Test
  public void dataFrameStatFunctionsApproxQuantile() {
    DataFrame df = getSession().sql("select * from values(1), (2) as t(a)");
    double[] values = {0, 0.1, 0.4, 0.6, 1};
    df.stat().approxQuantile("a", values);
    checkSpan("snow.snowpark.DataFrameStatFunctions", "approxQuantile");
  }

  @Test
  public void dataFrameStatFunctionsApproxQuantile2() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    double[] values = {0, 0.1, 0.6};
    String[] cols = {"a", "b"};
    df.stat().approxQuantile(cols, values);
    checkSpan("snow.snowpark.DataFrameStatFunctions", "approxQuantile");
  }

  @Test
  public void dataFrameStatFunctionsCrosstab() {
    DataFrame df = getSession().sql("select * from values(0.1, 0.5) as t(a, b)");
    df.stat().crosstab("a", "b");
    checkSpan("snow.snowpark.DataFrameStatFunctions", "crosstab");
  }

  @Test
  public void dataFrameWriterCsv() {
    String name = randomName();
    try {
      createTempStage(name);
      testSpanExporter.reset();
      DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
      df.write().csv("@" + name + "/csv");
      checkSpan("snow.snowpark.DataFrameWriter", "csv");
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
      checkSpan("snow.snowpark.DataFrameWriter", "json");
    } finally {
      dropStage(name);
    }
  }

  @Test
  public void dataFrameWriterParquet() {
    String name = randomName();
    try {
      createTempStage(name);
      testSpanExporter.reset();
      DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
      df.write().parquet("@" + name + "/parquet");
      checkSpan("snow.snowpark.DataFrameWriter", "parquet");
    } finally {
      dropStage(name);
    }
  }

  @Test
  public void dataFrameWriterSaveAsTable() {
    String name = randomName();
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    try {
      df.write().saveAsTable(name);
      checkSpan("snow.snowpark.DataFrameWriter", "saveAsTable");
    } finally {
      dropTable(name);
    }
    try {
      String[] names = {name};
      testSpanExporter.reset();
      df.write().saveAsTable(names);
      checkSpan("snow.snowpark.DataFrameWriter", "saveAsTable");
    } finally {
      dropTable(name);
    }
  }

  @Test
  public void dataFrameWriterAsyncActorSaveAsTable() {
    String name = randomName();
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
    try {
      df.write().async().saveAsTable(name).getResult();
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "saveAsTable");
    } finally {
      dropTable(name);
    }
    try {
      String[] names = {name};
      testSpanExporter.reset();
      df.write().async().saveAsTable(names).getResult();
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "saveAsTable");
    } finally {
      dropTable(name);
    }
  }

  @Test
  public void dataFrameWriterAsyncActorCsv() {
    String name = randomName();
    try {
      createTempStage(name);
      testSpanExporter.reset();
      DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
      df.write().async().csv("@" + name + "/csv").getResult();
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "csv");
    } finally {
      dropStage(name);
    }
  }

  @Test
  public void dataFrameWriterAsyncActorJson() {
    String name = randomName();
    try {
      createTempStage(name);
      testSpanExporter.reset();
      DataFrame df = getSession().sql("select * from values(1, 2) as t(a, b)");
      DataFrame df2 =
          df.select(
              com.snowflake.snowpark_java.Functions.array_construct(df.col("a"), df.col("b")));
      df2.write().async().json("@" + name + "/json").getResult();
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "json");
    } finally {
      dropStage(name);
    }
  }

  @Test
  public void dataFrameWriterAsyncActorParquet() {
    String name = randomName();
    try {
      createTempStage(name);
      testSpanExporter.reset();
      DataFrame df = getSession().sql("select * from values(1),(2),(3) as t(num)");
      df.write().async().parquet("@" + name + "/parquet").getResult();
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "parquet");
    } finally {
      dropStage(name);
    }
  }

  @Test
  public void copyableDataFrame() {
    String stageName = randomName();
    String tableName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTable(tableName, "a Int, b String, c Double", true);
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileCsv, false);
      testSpanExporter.reset();
      String className = "snow.snowpark.CopyableDataFrame";
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .copyInto(tableName);
      checkSpan(className, "copyInto");
      Column[] transformation = {Functions.col("$1"), Functions.col("$2"), Functions.col("$3")};
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .copyInto(tableName, transformation);
      checkSpan(className, "copyInto");
      Map<String, Object> options = new HashMap<>();
      options.put("skip_header", 1);
      options.put("FORCE", "true");
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .copyInto(tableName, transformation, options);
      checkSpan(className, "copyInto");
      String[] columns = {"a", "b", "c"};
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .copyInto(tableName, columns, transformation, options);
      checkSpan(className, "copyInto");
      getSession().read().schema(schema).csv("@" + stageName + "/" + TestFiles.testFileCsv).clone();
      checkSpan(className, "clone");
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }

  @Test
  public void copyableDataFrameAsyncActor() {
    String stageName = randomName();
    String tableName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTable(tableName, "a Int, b String, c Double", true);
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileCsv, false);
      testSpanExporter.reset();
      String className = "snow.snowpark.CopyableDataFrameAsyncActor";
      CopyableDataFrameAsyncActor df1 =
          getSession()
              .read()
              .schema(schema)
              .csv("@" + stageName + "/" + TestFiles.testFileCsv)
              .async();
      df1.copyInto(tableName).getResult();
      checkSpan(className, "copyInto");
      Column[] transformation = {Functions.col("$1"), Functions.col("$2"), Functions.col("$3")};
      CopyableDataFrameAsyncActor df2 =
          getSession()
              .read()
              .schema(schema)
              .csv("@" + stageName + "/" + TestFiles.testFileCsv)
              .async();
      df2.copyInto(tableName, transformation).getResult();
      checkSpan(className, "copyInto");
      Map<String, Object> options = new HashMap<>();
      options.put("skip_header", 1);
      options.put("FORCE", "true");
      CopyableDataFrameAsyncActor df3 =
          getSession()
              .read()
              .schema(schema)
              .csv("@" + stageName + "/" + TestFiles.testFileCsv)
              .async();
      df3.copyInto(tableName, transformation, options).getResult();
      checkSpan(className, "copyInto");
      String[] columns = {"a", "b", "c"};
      CopyableDataFrameAsyncActor df4 =
          getSession()
              .read()
              .schema(schema)
              .csv("@" + stageName + "/" + TestFiles.testFileCsv)
              .async();
      df4.copyInto(tableName, columns, transformation, options).getResult();
      checkSpan(className, "copyInto");
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }

  @Test
  public void updatable() {
    String tableName = randomName();
    Row[] data = {Row.create(1, "a", true), Row.create(2, "b", false)};
    StructType schema =
        StructType.create(
            new StructField("col1", DataTypes.IntegerType),
            new StructField("col2", DataTypes.StringType),
            new StructField("col3", DataTypes.BooleanType));
    String className = "snow.snowpark.Updatable";
    DataFrame df = getSession().sql("select * from values(1, 2), (1, 4) as t(a, b)");
    try {
      getSession().createDataFrame(data, schema).write().saveAsTable(tableName);
      testSpanExporter.reset();
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      Map<String, Column> map1 = new HashMap<>();
      map1.put("col1", Functions.lit(3));
      getSession().table(tableName).update(map);
      checkSpan(className, "update");
      getSession().table(tableName).updateColumn(map1);
      checkSpan(className, "update");
      getSession()
          .table(tableName)
          .update(map, Functions.col("col3").equal_to(Functions.lit(true)));
      checkSpan(className, "update");
      getSession()
          .table(tableName)
          .updateColumn(map1, Functions.col("col3").equal_to(Functions.lit(true)));
      checkSpan(className, "update");
      getSession().table(tableName).update(map, Functions.col("col1").equal_to(df.col("a")), df);
      checkSpan(className, "update");
      getSession()
          .table(tableName)
          .updateColumn(map1, Functions.col("col1").equal_to(df.col("a")), df);
      checkSpan(className, "update");
      getSession().table(tableName).delete();
      checkSpan(className, "delete");
      getSession().table(tableName).delete(Functions.col("col1").equal_to(Functions.lit(1)));
      checkSpan(className, "delete");
      getSession().table(tableName).delete(Functions.col("col1").equal_to(df.col("a")), df);
      checkSpan(className, "delete");
      getSession().table(tableName).clone();
      checkSpan(className, "clone");
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void updatableAsyncActor() {
    String tableName = randomName();
    Row[] data = {Row.create(1, "a", true), Row.create(2, "b", false)};
    StructType schema =
        StructType.create(
            new StructField("col1", DataTypes.IntegerType),
            new StructField("col2", DataTypes.StringType),
            new StructField("col3", DataTypes.BooleanType));
    String className = "snow.snowpark.UpdatableAsyncActor";
    DataFrame df = getSession().sql("select * from values(1, 2), (1, 4) as t(a, b)");
    try {
      getSession().createDataFrame(data, schema).write().saveAsTable(tableName);
      testSpanExporter.reset();
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      Map<String, Column> map1 = new HashMap<>();
      map1.put("col1", Functions.lit(3));
      UpdatableAsyncActor df1 = getSession().table(tableName).async();
      df1.update(map).getResult();
      checkSpan(className, "update");
      df1.updateColumn(map1).getResult();
      checkSpan(className, "update");
      df1.update(map, Functions.col("col3").equal_to(Functions.lit(true))).getResult();
      checkSpan(className, "update");
      df1.updateColumn(map1, Functions.col("col3").equal_to(Functions.lit(true))).getResult();
      checkSpan(className, "update");
      df1.update(map, Functions.col("col1").equal_to(df.col("a")), df);
      checkSpan(className, "update");
      df1.updateColumn(map1, Functions.col("col1").equal_to(df.col("a")), df);
      checkSpan(className, "update");
      df1.delete().getResult();
      checkSpan(className, "delete");
      df1.delete(Functions.col("col1").equal_to(Functions.lit(1))).getResult();
      checkSpan(className, "delete");
      df1.delete(Functions.col("col1").equal_to(df.col("a")), df).getResult();
      checkSpan(className, "delete");
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeBuilder() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
    Row[] data = {Row.create(1, "a", true), Row.create(2, "b", false)};
    StructType schema =
        StructType.create(
            new StructField("col1", DataTypes.IntegerType),
            new StructField("col2", DataTypes.StringType),
            new StructField("col3", DataTypes.BooleanType));
    try {
      getSession().createDataFrame(data, schema).write().saveAsTable(tableName);
      testSpanExporter.reset();
      Map<Column, Column> assignments = new HashMap<>();
      assignments.put(Functions.col("col1"), df.col("b"));
      getSession()
          .table(tableName)
          .merge(df, Functions.col("col1").equal_to(df.col("a")))
          .whenMatched()
          .update(assignments)
          .collect();
      checkSpan("snow.snowpark.MergeBuilder", "collect");
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void mergeBuilderAsyncActor() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
    Row[] data = {Row.create(1, "a", true), Row.create(2, "b", false)};
    StructType schema =
        StructType.create(
            new StructField("col1", DataTypes.IntegerType),
            new StructField("col2", DataTypes.StringType),
            new StructField("col3", DataTypes.BooleanType));
    try {
      getSession().createDataFrame(data, schema).write().saveAsTable(tableName);
      testSpanExporter.reset();
      Map<Column, Column> assignments = new HashMap<>();
      assignments.put(Functions.col("col1"), df.col("b"));
      MergeBuilderAsyncActor builderAsyncActor =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenMatched()
              .update(assignments)
              .async();
      builderAsyncActor.collect().getResult();
      checkSpan("snow.snowpark.MergeBuilderAsyncActor", "collect");
    } finally {
      dropTable(tableName);
    }
  }

  private void checkSpan(String className, String funcName, String methodChain) {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StackTraceElement file = stack[2];
    checkSpan(
        className, funcName, "JavaOpenTelemetrySuite.java", file.getLineNumber() - 1, methodChain);
  }

  private void checkSpan(String className, String funcName) {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StackTraceElement file = stack[2];
    checkSpan(
        className,
        funcName,
        "JavaOpenTelemetrySuite.java",
        file.getLineNumber() - 1,
        "DataFrame." + funcName);
  }
}
