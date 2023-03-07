// creating API test in different package,
// to make sure all API can be accessed from public
package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.sql.Connection;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.junit.Test;

public class JavaSessionSuite extends TestBase {

  @Test
  public void jdbcConnection() {
    Connection conn = getSession().jdbcConnection();
    assert conn instanceof SnowflakeConnection;
  }

  @Test
  public void table() {
    String tableName = randomName();
    try {
      createTable(tableName, "num int");
      runQuery("insert into " + tableName + " values(1),(2)");
      Row[] expected = {Row.create(1), Row.create(2)};
      checkAnswer(getSession().table(tableName), expected);
      checkAnswer(getSession().table(new String[] {tableName}), expected);
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void createDataFrame() {
    Row[] data = {Row.create(1, "a"), Row.create(2, "b")};
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType));
    DataFrame df = getSession().createDataFrame(data, schema);
    checkAnswer(df, data);
  }

  @Test
  public void range() {
    checkAnswer(
        getSession().range(5),
        new Row[] {Row.create(0), Row.create(1), Row.create(2), Row.create(3), Row.create(4)});
    checkAnswer(getSession().range(3, 5), new Row[] {Row.create(3), Row.create(4)});
    checkAnswer(
        getSession().range(3, 10, 2),
        new Row[] {Row.create(3), Row.create(5), Row.create(7), Row.create(9)});
  }

  @Test
  public void generator() {
    checkAnswer(
        getSession().generator(3, Functions.lit(1).as("a"), Functions.lit(2).as("b")),
        new Row[] {Row.create(1, 2), Row.create(1, 2), Row.create(1, 2)});
  }

  @Test
  public void getSessionStage() {
    assert getSession().getSessionStage().contains("SNOWPARK_TEMP_STAGE");
  }

  @Test
  public void flatten() {
    checkAnswer(
        getSession()
            .flatten(Functions.parse_json(Functions.lit("[\"a\",\"'\"]")))
            .select(Functions.col("value")),
        new Row[] {Row.create("\"a\""), Row.create("\"'\"")},
        false);
    checkAnswer(
        getSession()
            .flatten(Functions.parse_json(Functions.lit("{\"a\":[1,2]}")), "a", true, true, "ARRAY")
            .select(Functions.col("value")),
        new Row[] {Row.create("1"), Row.create("2")});
  }

  @Test
  public void getSessionInfo() {
    String result = getSession().getSessionInfo();
    assert result.contains("snowpark.version");
    assert result.contains("java.version");
    assert result.contains("scala.version");
    assert result.contains("jdbc.session.id");
    assert result.contains("os.name");
    assert result.contains("jdbc.version");
  }
}
