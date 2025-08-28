package com.snowflake.snowpark_test;

import static org.junit.Assert.assertThrows;

import com.snowflake.snowpark_java.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.Test;

public class JavaDataFrameSuite extends TestBase {

  @Test
  public void cacheResult() {
    String tableName = randomName();
    try {
      createTable(tableName, "num int");
      runQuery("insert into " + tableName + " values(1), (2)");
      DataFrame df = getSession().table(tableName);
      checkAnswer(df, new Row[] {Row.create(1), Row.create(2)});
      DataFrame df1 = df.filter(df.col("num").leq(Functions.lit(1))).cacheResult();
      checkAnswer(df1, new Row[] {Row.create(1)});

      runQuery("insert into " + tableName + " values(0), (3)");
      checkAnswer(df, new Row[] {Row.create(0), Row.create(1), Row.create(2), Row.create(3)});
      // cached df should not be changed.
      checkAnswer(df1, new Row[] {Row.create(1)});
    } finally {
      dropTable(tableName);
    }
  }

  @Test
  public void explain() {
    DataFrame df = getSession().sql("select * from values(1, 2),(3, 4) as t(a, b)");
    df.explain();
    // the verification test is in the `JavaAPISuite.scala` because console redirection
    // doesn't work in Java since running JUnit from Scala.
  }

  @Test
  public void toDF() {
    DataFrame df = getSession().sql("select * from values(1, 2),(3, 4) as t(a, b)");
    String[] names = df.schema().names();
    assert names.length == 2;
    assert names[0].equals("A");
    assert names[1].equals("B");
    checkAnswer(df, new Row[] {Row.create(1, 2), Row.create(3, 4)});
    DataFrame df1 = df.toDF(new String[] {"col1", "col2"});
    names = df1.schema().names();
    assert names.length == 2;
    assert names[0].equals("COL1");
    assert names[1].equals("COL2");
    checkAnswer(df1, new Row[] {Row.create(1, 2), Row.create(3, 4)});
    DataFrame df2 = df.toDF("a1", "a2");
    names = df2.schema().names();
    assert names.length == 2;
    assert names[0].equals("A1");
    assert names[1].equals("A2");
    checkAnswer(df2, new Row[] {Row.create(1, 2), Row.create(3, 4)});
  }

  @Test
  public void select() {
    DataFrame df = getSession().sql("select * from values(1, 2, 3) as t(a, b, c)");
    checkAnswer(df.select(df.col("a"), df.col("b")), new Row[] {Row.create(1, 2)});
    Column[] columns = {df.col("a"), df.col("b")};
    checkAnswer(df.select(columns), new Row[] {Row.create(1, 2)});

    checkAnswer(df.select("a", "b"), new Row[] {Row.create(1, 2)});
    String[] names = {"a", "b"};
    checkAnswer(df.select(names), new Row[] {Row.create(1, 2)});
  }

  @Test
  public void drop() {
    DataFrame df = getSession().sql("select * from values(1, 2, 3),(3, 4, 5) as t(a, b, c)");
    Row[] expected = {Row.create(1, 3), Row.create(3, 5)};
    checkAnswer(df.drop("b"), expected);
    String[] names = {"b"};
    checkAnswer(df.drop(names), expected);
    checkAnswer(df.drop(df.col("b")), expected);
    Column[] columns = {df.col("b")};
    checkAnswer(df.drop(columns), expected);
  }

  @Test
  public void agg() {
    DataFrame df = getSession().sql("select * from values(1, 2), (4, 5) as t(a, b)");
    Row[] expected = {Row.create(4, 3.5)};
    checkAnswer(df.agg(Functions.max(df.col("a")), Functions.mean(df.col("b"))), expected);
    Column[] exprs = {Functions.max(df.col("a")), Functions.mean(df.col("b"))};
    checkAnswer(df.agg(exprs), expected);
  }

  @Test
  public void distinct() {
    DataFrame df = getSession().sql("select * from values(1), (2), (3), (2), (1) as t(a)");
    checkAnswer(df.distinct(), new Row[] {Row.create(1), Row.create(2), Row.create(3)});
  }

  @Test
  public void dropDuplicates() {
    DataFrame df =
        getSession().sql("select * from values(1, 1, 1), (1, 2, 1), (3, 2, 1) as t(a, b, c)");
    checkAnswer(df.dropDuplicates("a", "c"), new Row[] {Row.create(1, 1, 1), Row.create(3, 2, 1)});
    checkAnswer(
        df.dropDuplicates(),
        new Row[] {Row.create(1, 1, 1), Row.create(1, 2, 1), Row.create(3, 2, 1)});
  }

  @Test
  public void limit() {
    DataFrame df = getSession().sql("select * from values(1), (2), (3), (2), (1) as t(a)");
    checkAnswer(df.limit(3), new Row[] {Row.create(1), Row.create(2), Row.create(3)});
  }

  @Test
  public void union() {
    DataFrame df = getSession().sql("select * from values(1), (2), (3), (2), (1) as t(a)");
    checkAnswer(df.union(df), new Row[] {Row.create(1), Row.create(2), Row.create(3)});
    checkAnswer(
        df.unionAll(df),
        new Row[] {
          Row.create(1),
          Row.create(1),
          Row.create(1),
          Row.create(1),
          Row.create(2),
          Row.create(2),
          Row.create(2),
          Row.create(2),
          Row.create(3),
          Row.create(3)
        });
  }

  @Test
  public void unionByName() {
    DataFrame df1 = getSession().sql("select * from values(1, 2), (2, 3), (1, 2) as t(a, b)");
    DataFrame df2 = getSession().sql("select * from values(2, 1), (3, 2), (2, 1) as t(b, a)");
    checkAnswer(df1.unionByName(df2), new Row[] {Row.create(1, 2), Row.create(2, 3)});
    checkAnswer(
        df1.unionAllByName(df2),
        new Row[] {
          Row.create(1, 2),
          Row.create(1, 2),
          Row.create(1, 2),
          Row.create(1, 2),
          Row.create(2, 3),
          Row.create(2, 3)
        });
  }

  @Test
  public void intersect() {
    DataFrame df1 = getSession().sql("select * from values(1), (2), (3) as t(a)");
    DataFrame df2 = getSession().sql("select * from values(4), (2), (3) as t(a)");
    checkAnswer(df1.intersect(df2), new Row[] {Row.create(2), Row.create(3)});
  }

  @Test
  public void except() {
    DataFrame df1 = getSession().sql("select * from values(1), (2), (3) as t(a)");
    DataFrame df2 = getSession().sql("select * from values(4), (2), (3) as t(a)");
    checkAnswer(df1.except(df2), new Row[] {Row.create(1)});
  }

  @Test
  public void cloneTest() {
    DataFrame df = getSession().sql("select * from values(1), (2), (3) as t(a)");
    DataFrame df1 = df.clone();
    checkAnswer(
        df.join(df1, df.col("a").equal_to(df1.col("a"))),
        new Row[] {Row.create(1, 1), Row.create(2, 2), Row.create(3, 3)});
  }

  @Test
  public void join1() {
    DataFrame df1 = getSession().sql("select * from values(1), (2) as t(a)");
    DataFrame df2 = getSession().sql("select * from values(3), (2) as t(b)");
    checkAnswer(
        df1.join(df2),
        new Row[] {Row.create(1, 2), Row.create(1, 3), Row.create(2, 2), Row.create(2, 3)});
  }

  @Test
  public void join2() {
    DataFrame df1 = getSession().sql("select * from values(1, 2), (2, 3) as t(a, b)");
    DataFrame df2 = getSession().sql("select * from values(3, 4), (2, 3) as t(b, c)");
    Row[] expected = {Row.create(2, 1, 3), Row.create(3, 2, 4)};
    checkAnswer(df1.join(df2, "b"), expected);
    checkAnswer(df1.join(df2, new String[] {"b"}), expected);
  }

  @Test
  public void join3() {
    DataFrame df1 = getSession().sql("select * from values(1, 0), (2, 3) as t(a, b)");
    DataFrame df2 = getSession().sql("select * from values(3, 4), (2, 3) as t(b, c)");
    Row[] expected = {Row.create(0, 1, null), Row.create(3, 2, 4)};
    checkAnswer(df1.join(df2, new String[] {"b"}, "left"), expected);
  }

  @Test
  public void join4() {
    DataFrame df1 = getSession().sql("select * from values(1, 0), (2, 3) as t(a, b)");
    DataFrame df2 = getSession().sql("select * from values(3, 4), (2, 3) as t(b, c)");
    checkAnswer(
        df1.join(df2, df1.col("b").equal_to(df2.col("b"))), new Row[] {Row.create(2, 3, 3, 4)});
    checkAnswer(
        df1.join(df2, df1.col("b").equal_to(df2.col("b")), "left"),
        new Row[] {Row.create(1, 0, null, null), Row.create(2, 3, 3, 4)});
  }

  @Test
  public void crossJoin() {
    DataFrame df1 = getSession().sql("select * from values(1, '1'), (3, '3') as t(int, str)");
    DataFrame df2 = getSession().sql("select * from values(2, '2'), (4, '4') as t(int, str)");

    checkAnswer(
        df1.crossJoin(df2),
        new Row[] {
          Row.create(1, "1", 2, "2"), Row.create(1, "1", 4, "4"),
          Row.create(3, "3", 2, "2"), Row.create(3, "3", 4, "4")
        });

    checkAnswer(
        df2.crossJoin(df1),
        new Row[] {
          Row.create(2, "2", 1, "1"), Row.create(2, "2", 3, "3"),
          Row.create(4, "4", 1, "1"), Row.create(4, "4", 3, "3")
        });
  }

  @Test
  public void naturalJoin() {
    DataFrame df1 = getSession().sql("select * from values(1, '1'), (3, '3') as t(a, b)");
    DataFrame df2 = getSession().sql("select * from values(1, '1'), (4, '4') as t(a, c)");

    checkAnswer(
        df1.naturalJoin(df2, "left"),
        new Row[] {Row.create(1, "1", "1"), Row.create(3, "3", null)});

    checkAnswer(df1.naturalJoin(df2), new Row[] {Row.create(1, "1", "1")});
  }

  @Test
  public void withColumns() {
    DataFrame df = getSession().sql("select * from values(1), (2) as t(a)");

    checkAnswer(
        df.withColumns(
            new String[] {"b", "c"},
            new Column[] {df.col("a").plus(Functions.lit(1)), df.col("a").minus(Functions.lit(1))}),
        new Row[] {Row.create(1, 2, 0), Row.create(2, 3, 1)});
  }

  @Test
  public void rename() {
    DataFrame df1 = getSession().sql("select * from values(1, 2), (3, 4) as t(a, b)");
    DataFrame df2 = df1.rename("c", df1.col("b"));
    checkAnswer(df2, new Row[] {Row.create(1, 2), Row.create(3, 4)});
    String[] names = df2.schema().names();
    assert names.length == 2;
    assert names[0].equalsIgnoreCase("a");
    assert names[1].equalsIgnoreCase("c");
  }

  @Test
  public void toLocalIterator() {
    DataFrame df = getSession().sql("select * from values(1), (2) as t(a)");
    Iterator<Row> it = df.toLocalIterator();
    assert it.hasNext();
    assert it.next().getInt(0) == 1;
    assert it.hasNext();
    assert it.next().getInt(0) == 2;
    assert !it.hasNext();
  }

  @Test(expected = NoSuchElementException.class)
  public void toLocalIterator2() {
    DataFrame df = getSession().sql("select * from values(1), (2) as t(a)");
    Iterator<Row> it = df.toLocalIterator();
    it.next();
    it.next();
    it.next();
  }

  @Test
  public void createOrReplaceTempView() {
    String viewName1 = randomName();
    String viewName2 = randomName();
    DataFrame df1 = getSession().sql("select * from values(1), (2) as t(a)");
    DataFrame df2 = getSession().sql("select * from values(3), (4) as t(a)");
    df1.createOrReplaceTempView(viewName1);
    df2.createOrReplaceTempView(new String[] {viewName2});
    checkAnswer(getSession().table(viewName1), df1.collect());
    checkAnswer(getSession().table(viewName2), df2.collect());
  }

  @Test
  public void first() {
    DataFrame df = getSession().sql("select * from values(1), (2), (3) as t(a)");
    Optional<Row> result = df.first();
    assert result.isPresent();
    assert result.get().getInt(0) == 1;

    result = df.filter(df.col("a").lt(Functions.lit(0))).first();
    assert !result.isPresent();

    Row[] result1 = df.first(2);
    assert result1.length == 2;
    assert result1[0].getInt(0) == 1;
    assert result1[1].getInt(0) == 2;
  }

  @Test
  public void isEmpty() {
    DataFrame populatedDf = getSession().sql("select * from values(1), (2), (3) as t(a)");
    DataFrame emptyDf = getSession().sql("select * from (select null as a) where 1 = 0");

    assert !populatedDf.isEmpty();
    assert emptyDf.isEmpty();
  }

  @Test
  public void sample() {
    long rowCount = 10000;
    DataFrame df = getSession().range(rowCount);
    assert df.sample(0).count() == 0;

    long rowCount10Percent = rowCount / 10;
    assert df.sample(rowCount10Percent).count() == rowCount10Percent;

    assert df.sample(rowCount).count() == rowCount;
    assert df.sample(rowCount + 10).count() == rowCount;

    assert df.sample(0.0).count() == 0;
    assert Math.abs(df.sample(0.5).count() - rowCount / 2) < rowCount / 2.0 * 0.4;
    assert df.sample(1.0).count() == rowCount;
  }

  @Test
  public void randomSplit() {
    long rowCount = 10000;
    DataFrame df = getSession().range(rowCount);

    DataFrame[] result = df.randomSplit(new double[] {0.2});
    assert result.length == 1;
    assert result[0].count() == rowCount;

    result = df.randomSplit(new double[] {0.2, 0.8});
    assert result.length == 2;
    assert Math.abs(result[0].count() - rowCount / 4) < rowCount / 4.0 * 0.4;
    assert Math.abs(result[1].count() - rowCount / 4 * 3) < rowCount * 0.75 * 0.4;
  }

  @Test
  public void flatten() {
    DataFrame df = getSession().sql("select parse_json(a) as a from values('[1,2]') as T(a)");
    checkAnswer(
        df.flatten(df.col("a")).select("value"), new Row[] {Row.create("1"), Row.create("2")});

    df = getSession().sql("select parse_json(value) as value from values('[1,2]') as T(value)");
    checkAnswer(
        df.flatten(df.col("value"), "", false, false, "both").select(Functions.col("value")),
        new Row[] {Row.create("1"), Row.create("2")});
  }

  @Test
  public void sortStringColumns() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values"
                    + "('Alice', 30, 'Manager'), "
                    + "('Charlie', 25, 'Designer'), "
                    + "('Bob', 25, 'Engineer') "
                    + "as t(name, age, role)");

    // Sort by single column
    checkAnswer(
        df.sort("role"),
        new Row[] {
          Row.create("Charlie", 25, "Designer"),
          Row.create("Bob", 25, "Engineer"),
          Row.create("Alice", 30, "Manager"),
        });

    // Sort by exactly two columns
    checkAnswer(
        df.sort("age", "name"),
        new Row[] {
          Row.create("Bob", 25, "Engineer"),
          Row.create("Charlie", 25, "Designer"),
          Row.create("Alice", 30, "Manager"),
        });

    // Sort by three or more columns
    checkAnswer(
        df.sort("age", "role", "name"),
        new Row[] {
          Row.create("Charlie", 25, "Designer"),
          Row.create("Bob", 25, "Engineer"),
          Row.create("Alice", 30, "Manager"),
        });

    // Negative test: column doesn't exist
    assertThrows(SnowflakeSQLException.class, () -> df.sort("non_existent_column").collect());
  }
}
