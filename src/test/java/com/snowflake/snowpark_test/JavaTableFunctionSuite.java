package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaTableFunctionSuite extends TestBase {
  @Test
  public void split_to_table1() {
    checkAnswer(
        getSession()
            .tableFunction(
                TableFunctions.split_to_table(),
                Functions.lit("split by space"),
                Functions.lit(" ")),
        new Row[] {Row.create(1, 1, "split"), Row.create(1, 2, "by"), Row.create(1, 3, "space")},
        false);
  }

  @Test
  public void split_to_table2() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {Row.create("[1,2]"), Row.create("[3,4]")},
                StructType.create(new StructField("a", DataTypes.StringType)));
    checkAnswer(
        df.join(TableFunctions.split_to_table(), df.col("a"), Functions.lit(",")).select("value"),
        new Row[] {Row.create("[1"), Row.create("2]"), Row.create("[3"), Row.create("4]")},
        false);
  }

  @Test
  public void flatten1() {
    Map<String, Column> args = new HashMap<>();
    args.put("input", Functions.parse_json(Functions.lit("[1,2]")));
    checkAnswer(
        getSession().tableFunction(TableFunctions.flatten(), args).select("value"),
        new Row[] {Row.create("1"), Row.create("2")});
  }

  @Test
  public void flatten2() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {Row.create("[1,2]"), Row.create("[3,4]")},
                StructType.create(new StructField("a", DataTypes.StringType)));
    Map<String, Column> args = new HashMap<>();
    args.put("input", Functions.parse_json(df.col("a")));
    checkAnswer(
        df.join(new TableFunction("flatten"), args).select("value"),
        new Row[] {Row.create("1"), Row.create("2"), Row.create("3"), Row.create("4")},
        false);
  }

  @Test
  public void tableFunctionName() {
    TableFunction tableFunction = new TableFunction("flatten");
    assert tableFunction.funcName().equals("flatten");
  }

  @Test
  public void argumentInTableFunction() {
    checkAnswer(
        getSession()
            .tableFunction(
                new TableFunction("split_to_table")
                    .call(Functions.lit("split by space"), Functions.lit(" "))),
        new Row[] {Row.create(1, 1, "split"), Row.create(1, 2, "by"), Row.create(1, 3, "space")},
        true);
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {Row.create("{\"a\":1, \"b\":[77, 88]}")},
                StructType.create(new StructField("col", DataTypes.StringType)));
    Map<String, Column> args = new HashMap<>();
    args.put("input", Functions.parse_json(df.col("col")));
    args.put("path", Functions.lit("b"));
    args.put("outer", Functions.lit(true));
    args.put("recursive", Functions.lit(true));
    args.put("mode", Functions.lit("both"));
    checkAnswer(
        getSession().tableFunction(new TableFunction("flatten").call(args)).select("value"),
        new Row[] {Row.create("77"), Row.create("88")});
  }
}
