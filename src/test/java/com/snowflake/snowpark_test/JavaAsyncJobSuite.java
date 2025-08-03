package com.snowflake.snowpark_test;

import static com.snowflake.snowpark_java.Functions.array_construct;
import static com.snowflake.snowpark_java.Functions.col;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.junit.Test;

public class JavaAsyncJobSuite extends TestBase {
  private int maxWaitTimeInSeconds = 60;

  @Test
  public void getQueryId() {
    assert !getSession().range(5).async().collect().getQueryId().isEmpty();
  }

  @Test
  public void createAsyncJob() {
    String id = getSession().range(5).async().collect().getQueryId();
    Row[] result = getSession().createAsyncJob(id).getRows();
    for (int i = 0; i < 5; i++) {
      assert result[i].getInt(0) == i;
    }
    assert result.length == 5;
  }

  @Test
  public void getIterator() {
    Iterator<Row> result = getSession().range(5).async().collect().getIterator();
    for (int i = 0; i < 5; i++) {
      assert result.hasNext();
      assert result.next().getInt(0) == i;
    }
    assert !result.hasNext();

    result = getSession().range(5).async().collect().getIterator(maxWaitTimeInSeconds);
    for (int i = 0; i < 5; i++) {
      assert result.hasNext();
      assert result.next().getInt(0) == i;
    }
    assert !result.hasNext();
  }

  @Test
  public void getRow() {
    Row[] result = getSession().range(5).async().collect().getRows();
    for (int i = 0; i < 5; i++) {
      assert result[i].getInt(0) == i;
    }
    assert result.length == 5;

    result = getSession().range(5).async().collect().getRows(maxWaitTimeInSeconds);
    for (int i = 0; i < 5; i++) {
      assert result[i].getInt(0) == i;
    }
    assert result.length == 5;
  }

  @Test
  public void cancel() throws InterruptedException {
    AsyncJob job = getSession().sql("select SYSTEM$WAIT(30)").async().collect();
    assert !job.isDone();
    job.cancel();
    Thread.sleep(5000);
    assert job.isDone();
  }

  @Test
  public void getResult1() {
    Row[] result = getSession().range(5).async().collect().getResult();
    for (int i = 0; i < 5; i++) {
      assert result[i].getInt(0) == i;
    }
    assert result.length == 5;

    result = getSession().range(5).async().collect().getResult(maxWaitTimeInSeconds);
    for (int i = 0; i < 5; i++) {
      assert result[i].getInt(0) == i;
    }
    assert result.length == 5;
  }

  @Test
  public void getResult2() {
    Iterator<Row> result = getSession().range(5).async().toLocalIterator().getResult();
    for (int i = 0; i < 5; i++) {
      assert result.hasNext();
      assert result.next().getInt(0) == i;
    }
    assert !result.hasNext();
  }

  @Test
  public void getResult3() {
    long result = getSession().range(5).async().count().getResult();
    assert result == 5;
  }

  // Copyable DataFrame

  @Test
  public void copyInto1() {
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
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .async()
          .copyInto(tableName)
          .getResult();
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(1, "one", 1.2), Row.create(2, "two", 2.2)});
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }

  @Test
  public void copyInto2() {
    String stageName = randomName();
    String tableName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTable(tableName, "num Int, length Int", true);
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileCsv, false);
      Column[] transformation = {Functions.col("$1"), Functions.length(Functions.col("$2"))};
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .async()
          .copyInto(tableName, transformation)
          .getResult();
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(1, 3), Row.create(2, 3)});
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }

  @Test
  public void copyInto3() {
    String stageName = randomName();
    String tableName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTable(tableName, "num Int, length Int", true);
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileCsv, false);
      Column[] transformation = {Functions.col("$1"), Functions.length(Functions.col("$2"))};
      Map<String, Object> options = new HashMap<>();
      options.put("skip_header", 1);
      options.put("FORCE", "true");
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .async()
          .copyInto(tableName, transformation, options)
          .getResult();
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, 3)});
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }

  @Test
  public void copyInto4() {
    String stageName = randomName();
    String tableName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTable(tableName, "num Int, length Int", true);
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileCsv, false);
      Column[] transformation = {Functions.length(Functions.col("$2")), Functions.col("$1")};
      Map<String, Object> options = new HashMap<>();
      options.put("skip_header", 1);
      options.put("FORCE", "true");
      String[] columns = {"length", "num"};
      getSession()
          .read()
          .schema(schema)
          .csv("@" + stageName + "/" + TestFiles.testFileCsv)
          .async()
          .copyInto(tableName, columns, transformation, options)
          .getResult();
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, 3)});
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }

  // DataFrame Writer

  @Test
  public void saveAsTable1() {
    String tableName = randomName();
    try {
      getSession().range(5).write().async().saveAsTable(tableName).getResult();
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(0), Row.create(1), Row.create(2), Row.create(3), Row.create(4)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void saveAsTable2() {
    String tableName = randomName();
    try {
      String[] name = {
        getSession().getCurrentDatabase().get(), getSession().getCurrentSchema().get(), tableName
      };
      getSession().range(5).write().async().saveAsTable(name).getResult();
      checkAnswer(
          getSession().table(name),
          new Row[] {Row.create(0), Row.create(1), Row.create(2), Row.create(3), Row.create(4)});
    } finally {
      dropTable(tableName);
    }
  }

  // updatable

  @Test
  public void update1() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      UpdateResult result = getSession().table(tableName).async().update(map).getResult();
      assert result.getRowsUpdated() == 2;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(3, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update2() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<String, Column> map = new HashMap<>();
      map.put("col1", Functions.lit(3));
      UpdateResult result = getSession().table(tableName).async().updateColumn(map).getResult();
      assert result.getRowsUpdated() == 2;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(3, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update3() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .async()
              .update(map, Functions.col("col3").equal_to(Functions.lit(true)))
              .getResult();
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update4() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      Map<String, Column> map = new HashMap<>();
      map.put("col1", Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .async()
              .updateColumn(map, Functions.col("col3").equal_to(Functions.lit(true)))
              .getResult();
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update5() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (1, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<Column, Column> map = new HashMap<>();
      map.put(Functions.col("col1"), Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .async()
              .update(map, Functions.col("col1").equal_to(df.col("a")), df)
              .getResult();
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void update6() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (1, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<String, Column> map = new HashMap<>();
      map.put("col1", Functions.lit(3));
      UpdateResult result =
          getSession()
              .table(tableName)
              .async()
              .updateColumn(map, Functions.col("col1").equal_to(df.col("a")), df)
              .getResult();
      assert result.getRowsUpdated() == 1;
      assert result.getMultiJoinedRowsUpdated() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(3, "a", true), Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void delete1() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      DeleteResult result = getSession().table(tableName).async().delete().getResult();
      assert result.getRowsDeleted() == 2;
      assert getSession().table(tableName).count() == 0;
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void delete2() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      DeleteResult result =
          getSession()
              .table(tableName)
              .async()
              .delete(Functions.col("col1").equal_to(Functions.lit(1)))
              .getResult();
      assert result.getRowsDeleted() == 1;
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void delete3() {
    String tableName = randomName();
    try {
      createTestTable(tableName);
      DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
      DeleteResult result =
          getSession()
              .table(tableName)
              .async()
              .delete(Functions.col("col1").equal_to(df.col("a")), df)
              .getResult();
      getSession().table(tableName).show();
      assert result.getRowsDeleted() == 1;
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  // Copy JavaUpdatableSuite.mergeMatchUpdate() and change to use async
  @Test
  public void mergeMatchUpdate() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      Map<Column, Column> assignments = new HashMap<>();
      assignments.put(Functions.col("col1"), df.col("b"));
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenMatched()
              .update(assignments)
              .async()
              .collect()
              .getResult();
      assert result.getRowsUpdated() == 1;
      assert result.getRowsDeleted() == 0;
      assert result.getRowsInserted() == 0;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(2, "a", true), Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  // Copy JavaUpdatableSuite.mergeMatchDelete() and change to use async
  @Test
  public void mergeMatchDelete() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (2, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenMatched(Functions.col("col3").equal_to(Functions.lit(true)))
              .delete()
              .async()
              .collect()
              .getResult();
      assert result.getRowsUpdated() == 0;
      assert result.getRowsDeleted() == 1;
      assert result.getRowsInserted() == 0;
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, "b", false)});
    } finally {
      dropTable(tableName);
    }
  }

  // Copy JavaUpdatableSuite.mergeNotMatchInsert1() and change to use async
  @Test
  public void mergeNotMatchInsert1() {
    String tableName = randomName();
    DataFrame df = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
    try {
      createTestTable(tableName);
      MergeResult result =
          getSession()
              .table(tableName)
              .merge(df, Functions.col("col1").equal_to(df.col("a")))
              .whenNotMatched()
              .insert(new Column[] {df.col("b"), Functions.lit("c"), Functions.lit(true)})
              .async()
              .collect()
              .getResult();
      assert result.getRowsUpdated() == 0;
      assert result.getRowsDeleted() == 0;
      assert result.getRowsInserted() == 1;
      checkAnswer(
          getSession().table(tableName),
          new Row[] {
            Row.create(1, "a", true), Row.create(2, "b", false), Row.create(4, "c", true)
          });
    } finally {
      dropTable(tableName);
    }
  }

  private void createTestTable(String name) {
    Row[] data = {Row.create(1, "a", true), Row.create(2, "b", false)};
    StructType schema =
        StructType.create(
            new StructField("col1", DataTypes.IntegerType),
            new StructField("col2", DataTypes.StringType),
            new StructField("col3", DataTypes.BooleanType));

    getSession().createDataFrame(data, schema).write().saveAsTable(name);
  }

  // Copy JavaDataFrameWriterSuite.saveAsCsv() and update to test async
  @Test
  public void saveAsCsvAsync() {
    String stageName = randomName();
    StructType schema =
        StructType.create(
            new StructField("a", DataTypes.IntegerType),
            new StructField("b", DataTypes.StringType));
    try {
      createTempStage(stageName);
      Row[] data = {Row.create(1, "a"), Row.create(2, "b")};
      DataFrame df = getSession().createDataFrame(data, schema);
      String path = "@" + stageName + "/p1";

      // Write file
      Map<String, Object> configs = new HashMap<>();
      configs.put("compression", "none");
      Row[] rows =
          df.write()
              .option("header", true)
              .options(configs)
              .async()
              .csv(path)
              .getResult()
              .getRows();
      // getSession().sql("get " + path + " file:///tmp/csv").collect();
      assert rows.length == 1 && rows[0].getInt(0) == 2;

      // Verify written CSV files, CSV file is not compressed
      Row[] files = getSession().sql("ls " + path).collect();
      assert files.length == 1 && files[0].getString(0).endsWith(".csv");

      // read back data to verify content
      Map<String, Object> readConfigs = new HashMap<>();
      readConfigs.put("skip_header", 1);
      DataFrame df2 = getSession().read().schema(schema).options(readConfigs).csv(path);
      checkAnswer(df2, new Row[] {Row.create(1, "a"), Row.create(2, "b")});

      // Test overwrite without HEADER
      df.write().mode(SaveMode.Overwrite).option("compression", "none").async().csv(path).getRows();
      DataFrame df4 = getSession().read().schema(schema).csv(path);
      checkAnswer(df4, new Row[] {Row.create(1, "a"), Row.create(2, "b")});
    } finally {
      dropStage(stageName);
    }
  }

  // Copy JavaDataFrameWriterSuite.saveAsJson() and update to test async
  @Test
  public void saveAsJsonAsync() {
    String stageName = randomName();
    StructType schema =
        StructType.create(
            new StructField("a", DataTypes.IntegerType),
            new StructField("b", DataTypes.StringType));
    try {
      createTempStage(stageName);
      Row[] data = {Row.create(1, "a"), Row.create(2, "b")};
      DataFrame df = getSession().createDataFrame(data, schema);
      DataFrame df2 = df.select(array_construct(col("a"), col("b")));
      String path = "@" + stageName + "/p1";

      // Write file
      Map<String, Object> configs = new HashMap<>();
      configs.put("compression", "none");
      Row[] rows = df2.write().options(configs).async().json(path).getResult().getRows();
      // getSession().sql("get " + path + " file:///tmp/json").collect();
      assert rows.length == 1 && rows[0].getInt(0) == 2;

      // Verify written JSON files, JSON file is not compressed
      Row[] files = getSession().sql("ls " + path).collect();
      assert files.length == 1 && files[0].getString(0).endsWith(".json");

      // read back data to verify content
      DataFrame df3 = getSession().read().json(path);
      checkAnswer(
          df3, new Row[] {Row.create("[\n  1,\n  \"a\"\n]"), Row.create("[\n  2,\n  \"b\"\n]")});

      // Test overwrite
      df2.write()
          .mode(SaveMode.Overwrite)
          .option("compression", "none")
          .async()
          .json(path)
          .getResult();
      DataFrame df4 = getSession().read().json(path);
      checkAnswer(
          df4, new Row[] {Row.create("[\n  1,\n  \"a\"\n]"), Row.create("[\n  2,\n  \"b\"\n]")});
    } finally {
      dropStage(stageName);
    }
  }

  // Copy JavaDataFrameWriterSuite.saveAsParquet() and update to test async
  @Test
  public void saveAsParquetAsync() {
    String stageName = randomName();
    StructType schema =
        StructType.create(
            new StructField("a", DataTypes.IntegerType),
            new StructField("b", DataTypes.StringType));
    try {
      createTempStage(stageName);
      Row[] data = {Row.create(1, "a"), Row.create(2, "b")};
      DataFrame df = getSession().createDataFrame(data, schema);
      String path = "@" + stageName + "/p1";

      // Write file
      Map<String, Object> configs = new HashMap<>();
      configs.put("compression", "lzo");
      Row[] rows = df.write().options(configs).async().parquet(path).getResult().getRows();
      // getSession().sql("get " + path + " file:///tmp/parquet").collect();
      assert rows.length == 1 && rows[0].getInt(0) == 2;

      // Verify written Parquet files
      Row[] files = getSession().sql("ls " + path).collect();
      assert files.length == 1 && files[0].getString(0).endsWith(".lzo.parquet");

      // read back data to verify content
      DataFrame df2 = getSession().read().parquet(path);
      df2.show();
      checkAnswer(
          df2,
          new Row[] {
            Row.create("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"a\"\n}"),
            Row.create("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"b\"\n}")
          });

      // Test overwrite
      df.write()
          .mode(SaveMode.Overwrite)
          .option("compression", "lzo")
          .async()
          .parquet(path)
          .getResult();
      DataFrame df4 = getSession().read().parquet(path);
      df4.show();
      checkAnswer(
          df4,
          new Row[] {
            Row.create("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"a\"\n}"),
            Row.create("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"b\"\n}")
          });
    } finally {
      dropStage(stageName);
    }
  }
}
