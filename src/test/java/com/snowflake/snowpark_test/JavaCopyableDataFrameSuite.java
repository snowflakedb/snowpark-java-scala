package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.CopyableDataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaCopyableDataFrameSuite extends TestBase {

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
          .copyInto(tableName);
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
          .copyInto(tableName, transformation);
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
          .copyInto(tableName, transformation, options);
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
          .copyInto(tableName, columns, transformation, options);
      checkAnswer(getSession().table(tableName), new Row[] {Row.create(2, 3)});
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }

  @Test
  public void cloneTest() {
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
      CopyableDataFrame df =
          getSession()
              .read()
              .schema(schema)
              .csv("@" + stageName + "/" + TestFiles.testFileCsv)
              .clone();
      df.copyInto(tableName);
      checkAnswer(
          getSession().table(tableName),
          new Row[] {Row.create(1, "one", 1.2), Row.create(2, "two", 2.2)});
    } finally {
      dropTable(tableName);
      dropStage(stageName);
    }
  }
}
