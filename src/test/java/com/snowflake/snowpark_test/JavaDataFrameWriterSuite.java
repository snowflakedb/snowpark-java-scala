package com.snowflake.snowpark_test;

import static com.snowflake.snowpark_java.Functions.*;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.SaveMode;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.Test;

public class JavaDataFrameWriterSuite extends TestBase {

  @Test
  public void saveAsTable() {
    String tableName = randomName();
    try {
      Row[] data = {Row.create(1, 2), Row.create(3, 4)};
      DataFrame df =
          getSession()
              .createDataFrame(
                  data,
                  StructType.create(
                      new StructField("a", DataTypes.IntegerType),
                      new StructField("b", DataTypes.IntegerType)));
      df.write().saveAsTable(tableName);
      checkAnswer(getSession().table(tableName), data);

      // Test "columnOrder" = "name"
      createTable(tableName, "b int, a int");
      df.write().option("columnOrder", "name").saveAsTable(tableName);
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, 1), Row.create(4, 3)});
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
      Row[] data = {Row.create(1, 2), Row.create(3, 4)};
      DataFrame df =
          getSession()
              .createDataFrame(
                  data,
                  StructType.create(
                      new StructField("a", DataTypes.IntegerType),
                      new StructField("b", DataTypes.IntegerType)));
      df.write().saveAsTable(name);
      checkAnswer(getSession().table(name), data);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void saveMode() {
    String tableName = randomName();
    try {
      StructType schema =
          StructType.create(
              new StructField("a", DataTypes.IntegerType),
              new StructField("b", DataTypes.IntegerType));
      getSession()
          .createDataFrame(new Row[] {Row.create(1, 2)}, schema)
          .write()
          .saveAsTable(tableName);
      getSession()
          .createDataFrame(new Row[] {Row.create(3, 4)}, schema)
          .write()
          .mode("APPEND")
          .saveAsTable(tableName);
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(1, 2), Row.create(3, 4)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void saveMode2() {
    String tableName = randomName();
    try {
      StructType schema =
          StructType.create(
              new StructField("a", DataTypes.IntegerType),
              new StructField("b", DataTypes.IntegerType));
      getSession()
          .createDataFrame(new Row[] {Row.create(1, 2)}, schema)
          .write()
          .saveAsTable(tableName);
      getSession()
          .createDataFrame(new Row[] {Row.create(3, 4)}, schema)
          .write()
          .mode(SaveMode.Append)
          .saveAsTable(tableName);
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(1, 2), Row.create(3, 4)});
      getSession()
          .createDataFrame(new Row[] {Row.create(5, 6)}, schema)
          .write()
          .mode(SaveMode.Ignore)
          .saveAsTable(tableName);
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(1, 2), Row.create(3, 4)});
      getSession()
          .createDataFrame(new Row[] {Row.create(3, 4)}, schema)
          .write()
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName);
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(3, 4)});

      boolean hasError = false;
      try {
        getSession()
            .createDataFrame(new Row[] {Row.create(3, 4)}, schema)
            .write()
            .mode(SaveMode.ErrorIfExists)
            .saveAsTable(tableName);
      } catch (Exception ex) {
        assert ex instanceof SnowflakeSQLException;
        assert ex.getMessage().contains("already exists");
        hasError = true;
      }
      assert hasError;
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void saveAsCsv() {
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
      Row[] rows = df.write().option("header", true).options(configs).csv(path).getRows();
      // getSession().sql("get " + path + " file:///tmp/csv").collect();
      assert rows.length == 1 && rows[0].getInt(0) == 2;

      // Verify written CSV files, CSV file is not compressed
      Row[] files = getSession().sql("ls " + path).collect();
      assert files.length == 1 && files[0].getString(0).endsWith(".csv");

      // read back data to verify content
      Map<String, Object> readConfigs = new HashMap<>();
      readConfigs.put("skip_header", 1);
      DataFrame df2 = getSession().read().schema(schema).options(readConfigs).csv(path);
      checkAnswer(df2, new Row[] {Row.create(1, "a"), Row.create(2, "b")}, false);

      // Test overwrite without HEADER
      df.write().mode(SaveMode.Overwrite).option("compression", "none").csv(path);
      DataFrame df4 = getSession().read().schema(schema).csv(path);
      checkAnswer(df4, new Row[] {Row.create(1, "a"), Row.create(2, "b")}, false);
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void saveAsJson() {
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
      Row[] rows = df2.write().options(configs).json(path).getRows();
      // getSession().sql("get " + path + " file:///tmp/json").collect();
      assert rows.length == 1 && rows[0].getInt(0) == 2;

      // Verify written JSON files, JSON file is not compressed
      Row[] files = getSession().sql("ls " + path).collect();
      assert files.length == 1 && files[0].getString(0).endsWith(".json");

      // read back data to verify content
      DataFrame df3 = getSession().read().json(path);
      checkAnswer(
          df3,
          new Row[] {Row.create("[\n  1,\n  \"a\"\n]"), Row.create("[\n  2,\n  \"b\"\n]")},
          false);

      // Test overwrite
      df2.write().mode(SaveMode.Overwrite).option("compression", "none").json(path);
      DataFrame df4 = getSession().read().json(path);
      checkAnswer(
          df4,
          new Row[] {Row.create("[\n  1,\n  \"a\"\n]"), Row.create("[\n  2,\n  \"b\"\n]")},
          false);
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void saveAsParquet() {
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
      Row[] rows = df.write().options(configs).parquet(path).getRows();
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
          },
          false);

      // Test overwrite
      df.write().mode(SaveMode.Overwrite).option("compression", "lzo").parquet(path);
      DataFrame df4 = getSession().read().parquet(path);
      df4.show();
      checkAnswer(
          df4,
          new Row[] {
            Row.create("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"a\"\n}"),
            Row.create("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"b\"\n}")
          },
          false);
    } finally {
      dropStage(stageName);
    }
  }
}
