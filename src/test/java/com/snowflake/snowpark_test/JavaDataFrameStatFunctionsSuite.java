package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class JavaDataFrameStatFunctionsSuite extends TestBase {
  @Test
  public void corr() {
    DataFrame df1 = getSession().sql("select * from values(1.0, 1) as T(a, b)");
    assert !df1.stat().corr("a", "b").isPresent();
    DataFrame df2 =
        getSession()
            .sql(
                "select * from values(1.0, 1),('NaN'::Double, 2),(null, 3),"
                    + " (4.0, null), (null, null), ('NaN'::Double, null) as T(a, b)");
    assert df2.stat().corr("a", "b").get().isNaN();
    DataFrame df3 =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    assert df3.stat().corr("a", "b").get() == 0.9999999999999991;
  }

  @Test
  public void cov() {
    DataFrame df1 = getSession().sql("select * from values(1.0, 1) as T(a, b)");
    assert !df1.stat().cov("a", "b").isPresent();
    DataFrame df2 =
        getSession()
            .sql(
                "select * from values(1.0, 1),('NaN'::Double, 2),(null, 3),"
                    + " (4.0, null), (null, null), ('NaN'::Double, null) as T(a, b)");
    assert df2.stat().cov("a", "b").get().isNaN();
    DataFrame df3 =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    assert df3.stat().cov("a", "b").get() == 0.010000000000000037;
  }

  @Test
  public void approxQuantile() {
    DataFrame df =
        getSession().sql("select * from values(1),(2),(3),(4),(5),(6),(7),(8),(9),(0) as T(a)");
    assert df.stat().approxQuantile("a", new double[] {0.5})[0].get() == 4.5;
  }

  @Test
  public void approxQuantile1() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Optional<Double>[][] result =
        df.stat().approxQuantile(new String[] {"a", "b"}, new double[] {0, 0.1, 0.6});
    assert result.length == 2;
    assert result[0][0].get() == 0.05;
    assert result[0][1].get() == 0.15000000000000002;
    assert result[0][2].get() == 0.25;
    assert result[1][0].get() == 0.45;
    assert result[1][1].get() == 0.55;
    assert result[1][2].get() == 0.6499999999999999;
  }

  @Test
  public void crosstab() {
    Row[] data = {
      Row.create(1, 10000, "JAN"),
      Row.create(1, 400, "JAN"),
      Row.create(2, 4500, "JAN"),
      Row.create(2, 35000, "JAN"),
      Row.create(1, 5000, "FEB"),
      Row.create(1, 3000, "FEB"),
      Row.create(2, 200, "FEB"),
      Row.create(2, 90500, "FEB"),
      Row.create(1, 6000, "MAR"),
      Row.create(1, 5000, "MAR"),
      Row.create(2, 2500, "MAR"),
      Row.create(2, 9500, "MAR"),
      Row.create(1, 8000, "APR"),
      Row.create(1, 10000, "APR"),
      Row.create(2, 800, "APR"),
      Row.create(2, 4500, "APR")
    };
    StructType schema =
        StructType.create(
            new StructField("empid", DataTypes.IntegerType),
            new StructField("amount", DataTypes.IntegerType),
            new StructField("month", DataTypes.StringType));
    DataFrame df = getSession().createDataFrame(data, schema);
    checkAnswer(
        df.stat().crosstab("empid", "month"),
        new Row[] {Row.create(1, 2, 2, 2, 2), Row.create(2, 2, 2, 2, 2)});
  }

  @Test
  public void sampleBy() {
    Row[] data = {
      Row.create(1, 10000, "JAN"),
      Row.create(1, 400, "JAN"),
      Row.create(2, 4500, "JAN"),
      Row.create(2, 35000, "JAN"),
      Row.create(1, 5000, "FEB"),
      Row.create(1, 3000, "FEB"),
      Row.create(2, 200, "FEB"),
      Row.create(2, 90500, "FEB"),
      Row.create(1, 6000, "MAR"),
      Row.create(1, 5000, "MAR"),
      Row.create(2, 2500, "MAR"),
      Row.create(2, 9500, "MAR"),
      Row.create(1, 8000, "APR"),
      Row.create(1, 10000, "APR"),
      Row.create(2, 800, "APR"),
      Row.create(2, 4500, "APR")
    };
    StructType schema =
        StructType.create(
            new StructField("empid", DataTypes.IntegerType),
            new StructField("amount", DataTypes.IntegerType),
            new StructField("month", DataTypes.StringType));
    DataFrame df = getSession().createDataFrame(data, schema);
    Map<Integer, Double> map = new HashMap<>();
    map.put(1, 0.0);
    map.put(2, 1.0);
    Row[] expected = {
      Row.create(2, 4500, "JAN"),
      Row.create(2, 35000, "JAN"),
      Row.create(2, 200, "FEB"),
      Row.create(2, 90500, "FEB"),
      Row.create(2, 2500, "MAR"),
      Row.create(2, 9500, "MAR"),
      Row.create(2, 800, "APR"),
      Row.create(2, 4500, "APR")
    };
    checkAnswer(df.stat().sampleBy(df.col("empid"), map), expected, false);

    checkAnswer(df.stat().sampleBy("empid", map), expected, false);
  }
}
