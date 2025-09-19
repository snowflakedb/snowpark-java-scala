package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.DataTypes;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Test;

public class JavaColumnSuite extends TestBase {

  @Test
  public void subField() {
    DataFrame data =
        getSession()
            .sql(
                "select parse_json(column1) as v from values ('{\"a\": null}'), ('{\"a\":"
                    + " \"foo\"}'), (null)");

    Row[] expected = {Row.create("null"), Row.create("\"foo\""), Row.create((Object) null)};
    checkAnswer(data.select(data.col("v").subField("a")), expected);
  }

  @Test
  public void subFieldIndex() {
    DataFrame data =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, d, e, f from"
                    + " values(1,2,3,2,'e1','[{a:1}]'),(6,7,8,1,'e2','[{a:1},{b:2}]') as"
                    + " T(a,b,c,d,e,f)");

    Row[] expected = {Row.create("1"), Row.create("6")};
    checkAnswer(data.select(data.col("arr1").subField(0)), expected);
  }

  @Test
  public void getName() {
    DataFrame data = getSession().sql("select * from values('asdFg'),('qqq'),('Qw') as T(a)");
    Optional<String> col = data.col("a").getName();

    assert col.isPresent();
    assert col.get().equalsIgnoreCase("\"A\"");
  }

  @Test
  public void toStringTest() {
    DataFrame data = getSession().sql("select * from values('asdFg'),('qqq'),('Qw') as T(a)");
    Column col = data.col("a");

    assert col.toString().equals("Column['\"A\"]");
  }

  @Test
  public void alias() {
    DataFrame data = getSession().sql("select * from values('asdFg'),('qqq'),('Qw') as T(a)");
    DataFrame data1 = data.select(data.col("a").as("b"), data.col("a").alias("c"));
    assert data1.col("b").getName().isPresent();
    assert data1.col("c").getName().isPresent();
  }

  @Test
  public void unaryMinus() {
    DataFrame data = getSession().sql("select * from values(1),(-2) as T(a)");
    Row[] expected = {Row.create(-1), Row.create(2)};
    checkAnswer(data.select(data.col("a").unary_minus()), expected);
  }

  @Test
  public void unaryNot() {
    DataFrame data = getSession().sql("select * from values(true),(false) as T(a)");
    Row[] expected = {Row.create(false), Row.create(true)};
    checkAnswer(data.select(data.col("a").unary_not()), expected);
  }

  @Test
  public void compareOps() {
    DataFrame data = getSession().sql("select * from values(1) as T(a)");
    Row[] expected = {
      Row.create(
          true, true, false, false, true, false, false, true, false, false, true, true, false,
          false, true, true, false, false)
    };
    Column a = Functions.col("a");
    checkAnswer(
        data.select(
            a.equal_to(Functions.lit(1)),
            a.equal_to(1),
            a.not_equal(Functions.lit(1)),
            a.not_equal(1),
            a.gt(Functions.lit(0)),
            a.gt(Functions.lit(1)),
            a.gt(1),
            a.lt(Functions.lit(2)),
            a.lt(Functions.lit(1)),
            a.lt(1),
            a.leq(Functions.lit(2)),
            a.leq(Functions.lit(1)),
            a.leq(Functions.lit(0)),
            a.leq(0),
            a.geq(Functions.lit(0)),
            a.geq(Functions.lit(1)),
            a.geq(Functions.lit(2)),
            a.geq(2)),
        expected);
  }

  @Test
  public void equalNull() {
    DataFrame data =
        getSession().sql("select * from values(null, 1),(2, 2),(null, null) as T(a,b)");
    Row[] expected = {Row.create(false), Row.create(true), Row.create(true)};
    checkAnswer(data.select(data.col("a").equal_null(data.col("b"))), expected);
    Row[] expected2 = {Row.create(true), Row.create(false), Row.create(true)};
    checkAnswer(data.select(data.col("a").equal_null(null)), expected2);
  }

  @Test
  public void equalNan() {
    DataFrame data = getSession().sql("select * from values(1.1),(null),('NaN' :: Float) as T(a)");
    Row[] expected = {Row.create(false), Row.create((Object) null), Row.create(true)};
    checkAnswer(data.select(data.col("a").equal_nan()), expected);
  }

  @Test
  public void isNull() {
    DataFrame data = getSession().sql("select * from values(1),(null) as T(a)");
    Row[] expected = {Row.create(false, true), Row.create(true, false)};
    checkAnswer(data.select(data.col("a").is_null(), data.col("a").is_not_null()), expected);
    checkAnswer(data.select(data.col("a").isNull(), data.col("a").is_not_null()), expected);
  }

  @Test
  public void orAnd() {
    DataFrame data =
        getSession()
            .sql(
                "select * from values(true, false),(true, true),"
                    + "(false, false), (false, true) as T(a, b)");

    Row[] expected = {
      Row.create(true, false),
      Row.create(true, true),
      Row.create(false, false),
      Row.create(true, false)
    };
    checkAnswer(
        data.select(data.col("a").or(data.col("b")), data.col("a").and(data.col("b"))), expected);
  }

  @Test
  public void between() {
    DataFrame data = getSession().sql("select * from values(1),(2),(3),(4) as T(a)");
    Row[] expected = {Row.create(2), Row.create(3)};

    checkAnswer(
        data.where(Functions.col("a").between(Functions.lit(2), Functions.lit(3))), expected);
  }

  @Test
  public void mathOps() {
    DataFrame data = getSession().sql("select * from values(1,2),(7,10),(-1,-12) as T(a,b)");
    Row[] expected = {
      Row.create(3, -1, 2, 0.5, 1),
      Row.create(17, -3, 70, 0.7, 7),
      Row.create(-13, 11, 12, 0.083333, -1)
    };

    Column a = data.col("a");
    Column b = data.col("b");
    checkAnswer(data.select(a.plus(b), a.minus(b), a.multiply(b), a.divide(b), a.mod(b)), expected);

    Row[] expected2 = {
      Row.create(11, -9, 10, 0.1, 1),
      Row.create(17, -3, 70, 0.7, 7),
      Row.create(9, -11, -10, -0.1, -1),
    };
    checkAnswer(
        data.select(a.plus(10), a.minus(10), a.multiply(10), a.divide(10), a.mod(10)), expected2);
  }

  @Test
  public void cast() {
    DataFrame data = getSession().sql("select * from values(1) as T(a)");

    data.schema().printTreeString();
    assert data.schema().get(0).dataType().equals(DataTypes.LongType);

    DataFrame data1 = data.select(data.col("a").cast(DataTypes.StringType));
    assert data1.schema().get(0).dataType().equals(DataTypes.StringType);
  }

  @Test
  public void orderExprs() {
    DataFrame data = getSession().sql("select * from values (2),(1),(null),(5),(null),(8) as T(a)");
    Row[] expected = {
      Row.create((Object) null),
      Row.create((Object) null),
      Row.create(1),
      Row.create(2),
      Row.create(5),
      Row.create(8)
    };
    checkAnswer(data.sort(data.col("a")), expected);
    checkAnswer(data.sort(data.col("a").asc()), expected);
    checkAnswer(data.sort(data.col("a").asc_nulls_first()), expected);

    Row[] expected1 = {
      Row.create(8),
      Row.create(5),
      Row.create(2),
      Row.create(1),
      Row.create((Object) null),
      Row.create((Object) null)
    };
    checkAnswer(data.sort(data.col("a").desc()), expected1);
    checkAnswer(data.sort(data.col("a").desc_nulls_last()), expected1);

    Row[] expected2 = {
      Row.create((Object) null),
      Row.create((Object) null),
      Row.create(8),
      Row.create(5),
      Row.create(2),
      Row.create(1)
    };
    checkAnswer(data.sort(data.col("a").desc_nulls_first()), expected2);

    Row[] expected3 = {
      Row.create(1),
      Row.create(2),
      Row.create(5),
      Row.create(8),
      Row.create((Object) null),
      Row.create((Object) null)
    };
    checkAnswer(data.sort(data.col("a").asc_nulls_last()), expected3);
  }

  @Test
  public void bitwise() {
    DataFrame data = getSession().sql("select * from values(1, 3) as T(a, b)");
    Row[] expected = {Row.create(3)};
    checkAnswer(data.select(data.col("a").bitor(data.col("b"))), expected);

    expected[0] = Row.create(1);
    checkAnswer(data.select(data.col("a").bitand(data.col("b"))), expected);

    expected[0] = Row.create(2);
    checkAnswer(data.select(data.col("a").bitxor(data.col("b"))), expected);
  }

  @Test
  public void like() {
    DataFrame data = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");

    Row[] expected = {Row.create("apple"), Row.create("peach")};
    checkAnswer(data.where(data.col("a").like(Functions.lit("%p%"))), expected);
  }

  @Test
  public void regexp() {
    DataFrame data = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create("apple")};

    checkAnswer(data.where(data.col("a").regexp(Functions.lit("ap.le"))), expected);
  }

  @Test
  public void collate() {
    DataFrame data = getSession().sql("select * from values('  abcba  '), (' a12321a   ') as T(a)");
    Row[] expected = {Row.create("  abcba  ")};
    checkAnswer(
        data.where(data.col("a").collate("en_US-trim").equal_to(Functions.lit("abcba"))), expected);
  }

  @Test
  public void caseWhen() {
    DataFrame df = getSession().sql("select * from values(null),(2),(1),(3),(null) as T(a)");
    checkAnswer(
        df.select(
            Functions.when(df.col("a").is_null(), Functions.lit(5))
                .when(df.col("a").equal_to(Functions.lit(1)), Functions.lit(6))
                .otherwise(Functions.lit(7))
                .as("a")),
        new Row[] {Row.create(5), Row.create(7), Row.create(6), Row.create(7), Row.create(5)});

    checkAnswer(
        df.select(
            Functions.when(df.col("a").is_null(), Functions.lit(5))
                .when(df.col("a").equal_to(Functions.lit(1)), Functions.lit(6))
                .as("a")),
        new Row[] {
          Row.create(5),
          Row.create((Object) null),
          Row.create(6),
          Row.create((Object) null),
          Row.create(5)
        });

    // Handling no column type values
    checkAnswer(
        df.select(
            Functions.when(df.col("a").is_null(), 5)
                .when(df.col("a").equal_to(Functions.lit(1)), 6)
                .otherwise(7)
                .as("a")),
        new Row[] {Row.create(5), Row.create(7), Row.create(6), Row.create(7), Row.create(5)});

    // Handling null values
    checkAnswer(
        df.select(
            Functions.when(df.col("a").is_null(), null)
                .when(df.col("a").equal_to(Functions.lit(1)), null)
                .otherwise(null)
                .as("a")),
        new Row[] {
          Row.create((Object) null),
          Row.create((Object) null),
          Row.create((Object) null),
          Row.create((Object) null),
          Row.create((Object) null)
        });
  }

  @Test
  public void in() {
    DataFrame df0 = getSession().sql("select * from values(1, 'a'), (2, 'b'), (3, 'c') as t(a, b)");
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1, 'a', 1, 1), (2, 'b', 2, 2), (3, 'b', 33, 33) as"
                    + " T(a,b,c,d)");
    DataFrame df1 =
        df.filter(
            Functions.in(
                new Column[] {df.col("a"), df.col("b")},
                Arrays.asList(
                    Arrays.asList(1, "a"), Arrays.asList(2, "b"), Arrays.asList(3, "c"))));
    Row[] expected = {Row.create(1, "a", 1, 1), Row.create(2, "b", 2, 2)};
    checkAnswer(df1, expected);

    DataFrame df2 = df.filter(Functions.in(new Column[] {df.col("a"), df.col("b")}, df0));
    checkAnswer(df2, expected);
  }

  @Test
  public void in2() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1, 'a', 1, 1), (2, 'b', 2, 2), (3, 'b', 33, 33) as"
                    + " T(a,b,c,d)");
    Row[] expected = {Row.create(2, "b", 2, 2), Row.create(3, "b", 33, 33)};
    checkAnswer(df.filter(df.col("a").in(2, 3)), expected);

    DataFrame df1 = getSession().sql("select * from values(2),(3) as t(a)");
    checkAnswer(df.filter(df.col("a").in(df1)), expected);
  }
}
