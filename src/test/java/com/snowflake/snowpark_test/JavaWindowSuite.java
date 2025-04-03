package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Window;
import com.snowflake.snowpark_java.WindowSpec;
import org.junit.Test;

public class JavaWindowSuite extends TestBase {

  @Test
  public void partitionBy() {
    DataFrame df =
        getSession().sql("select * from values(1,'1'),(2,'2'),(1,'1'),(2,'2') as T(key, value)");
    WindowSpec w = Window.orderBy(df.col("value")).partitionBy(df.col("key"));

    Row[] expected = {
      Row.create(1, "1"), Row.create(2, "2"), Row.create(null, null), Row.create(null, null)
    };
    checkAnswer(
        df.select(
            Functions.lead(df.col("key")).over(w), Functions.lead(df.col("value"), 1).over(w)),
        expected);
    checkAnswer(
        df.select(
            Functions.lead(df.col("key")).over(w),
            Functions.lead(df.col("value"), 1, Functions.lit(null)).over(w)),
        expected);
  }

  @Test
  public void orderBy_rowsBetween() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,'1'),(2,'1'),(2,'2'),(1,'1'),(2,'2') as T(key, value)");
    WindowSpec w = Window.partitionBy(df.col("value")).orderBy(df.col("key")).rowsBetween(-1, 2);

    Row[] expected1 = {
      Row.create(1, 1.333),
      Row.create(1, 1.333),
      Row.create(2, 1.500),
      Row.create(2, 2.000),
      Row.create(2, 2.000)
    };
    checkAnswer(df.select(df.col("key"), Functions.avg(df.col("key")).over(w)), expected1);
  }

  @Test
  public void rowsBetween() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,'1'),(2,'1'),(2,'2'),(1,'1'),(2,'2') as T(key, value)");
    WindowSpec w = Window.rowsBetween(Window.currentRow(), 2).orderBy(df.col("key"));

    Row[] expected1 = {
      Row.create(2, 2.00),
      Row.create(2, 2.00),
      Row.create(2, 2.00),
      Row.create(1, 1.666),
      Row.create(1, 1.333)
    };
    checkAnswer(df.select(df.col("key"), Functions.avg(df.col("key")).over(w)), expected1);
  }

  @Test
  public void emptyOver() {
    DataFrame df =
        getSession().sql("select * from values('a',1),('a',1),('a',2),('b',2) as T(key, value)");
    Row[] expected = {
      Row.create("a", 1, 6, 1.5),
      Row.create("a", 1, 6, 1.5),
      Row.create("a", 2, 6, 1.5),
      Row.create("b", 2, 6, 1.5)
    };

    checkAnswer(
        df.select(
            df.col("key"),
            df.col("value"),
            Functions.sum(df.col("value")).over(),
            Functions.avg(df.col("value")).over()),
        expected);
  }

  @Test
  public void rangeBetween() {
    DataFrame df = getSession().sql("select * from values('non_numeric') as T(value)");
    WindowSpec ws = Window.orderBy(df.col("value"));
    Row[] expected = {Row.create("non_numeric", "non_numeric")};
    checkAnswer(
        df.select(
            df.col("value"),
            Functions.min(df.col("value"))
                .over(ws.rangeBetween(Window.unboundedPreceding(), Window.unboundedFollowing()))),
        expected);

    WindowSpec ws2 =
        Window.rangeBetween(Window.unboundedPreceding(), Window.unboundedFollowing())
            .orderBy(df.col("value"));
    checkAnswer(df.select(df.col("value"), Functions.min(df.col("value")).over(ws2)), expected);
  }
}
