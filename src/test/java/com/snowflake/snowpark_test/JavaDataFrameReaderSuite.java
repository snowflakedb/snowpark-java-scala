package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaDataFrameReaderSuite extends TestBase {

  @Test
  public void table() {
    String tableName = randomName();
    try {
      runQuery(
          "create or replace temp table "
              + tableName
              + " as select * from values(1,2),(3,4) as t(a,b)");
      checkAnswer(
          getSession().read().table(tableName), new Row[] {Row.create(1, 2), Row.create(3, 4)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void csv() {
    String stageName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileCsv, false);
      DataFrame df =
          getSession().read().schema(schema).csv("@" + stageName + "/" + TestFiles.testFileCsv);
      checkAnswer(df, new Row[] {Row.create(1, "one", 1.2), Row.create(2, "two", 2.2)});
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void json() {
    String stageName = randomName();
    try {
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileJson, false);
      DataFrame df = getSession().read().json("@" + stageName + "/" + TestFiles.testFileJson);
      DataFrame df1 =
          df.select(Functions.parse_json(Functions.col("$1")).as("a"))
              .select(Functions.json_extract_path_text(Functions.col("a"), Functions.lit("color")));
      checkAnswer(df1, new Row[] {Row.create("Red")});
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void avro() {
    String stageName = randomName();
    try {
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileAvro, false);
      DataFrame df = getSession().read().avro("@" + stageName + "/" + TestFiles.testFileAvro);
      checkAnswer(
          df.filter(Functions.sqlExpr("$1:num").gt(Functions.lit(1))),
          new Row[] {Row.create("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")});
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void parquet() {
    String stageName = randomName();
    try {
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileParquet, false);
      DataFrame df = getSession().read().parquet("@" + stageName + "/" + TestFiles.testFileParquet);
      checkAnswer(
          df.filter(Functions.sqlExpr("$1:num").gt(Functions.lit(1))),
          new Row[] {Row.create("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")});
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void orc() {
    String stageName = randomName();
    try {
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileOrc, false);
      DataFrame df = getSession().read().orc("@" + stageName + "/" + TestFiles.testFileOrc);
      checkAnswer(
          df.filter(Functions.sqlExpr("$1:num").gt(Functions.lit(1))),
          new Row[] {Row.create("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")});
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void xml() {
    String stageName = randomName();
    try {
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileXml, false);
      DataFrame df = getSession().read().xml("@" + stageName + "/" + TestFiles.testFileXml);
      checkAnswer(
          df.where(Functions.sqlExpr("xmlget($1, 'num', 0):\"$\"").gt(Functions.lit(1))),
          new Row[] {Row.create("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")});
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void option() {
    String stageName = randomName();
    try {
      createTempStage(stageName);
      uploadFileToStage(stageName, TestFiles.testFileCsvColon, false);
      StructType schema =
          StructType.create(
              new StructField("a", DataTypes.IntegerType),
              new StructField("b", DataTypes.StringType),
              new StructField("c", DataTypes.DoubleType));
      Row[] expected = {Row.create(1, "one", 1.2), Row.create(2, "two", 2.2)};
      DataFrame df =
          getSession()
              .read()
              .option("field_delimiter", ";")
              .option("skip_header", 1)
              .schema(schema)
              .csv("@" + stageName + "/" + TestFiles.testFileCsvColon);
      checkAnswer(df, expected);

      Map<String, Object> configs = new HashMap<>();
      configs.put("field_delimiter", ";");
      configs.put("skip_header", 1);
      DataFrame df1 =
          getSession()
              .read()
              .options(configs)
              .schema(schema)
              .csv("@" + stageName + "/" + TestFiles.testFileCsvColon);
      checkAnswer(df1, expected);
    } finally {
      dropStage(stageName);
    }
  }
}
