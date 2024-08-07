package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.*;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.junit.Test;

public class JavaFunctionSuite extends TestBase {

  @Test
  public void toScalar() {
    DataFrame df1 = getSession().sql("select * from values(1,1,1),(2,2,3) as T(c1, c2, c3)");
    DataFrame df2 = getSession().sql("select * from values(2) as T(a)");

    Row[] expected = {Row.create(1, 2), Row.create(2, 2)};
    checkAnswer(df1.select(Functions.col("c1"), Functions.col(df2)), expected, false);
    checkAnswer(df1.select(Functions.col("c1"), Functions.toScalar(df2)), expected, false);
  }

  @Test
  public void sqlText() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_json(column1) as v  from values ('{\"a\": null}'),"
                    + "('{\"a\": \"foo\"}'), (null)");
    Row[] expected = {Row.create("null"), Row.create("\"foo\""), Row.create((Object) null)};
    checkAnswer(df.select(Functions.sqlExpr("v:a")), expected, false);
  }

  @Test
  public void approx_count_distinct() {
    DataFrame df = getSession().sql("select * from values(3),(2),(1),(3),(2) as T(a)");
    Row[] expected = {Row.create(3)};
    checkAnswer(df.select(Functions.approx_count_distinct(df.col("a"))), expected);
  }

  @Test
  public void avg() {
    DataFrame df = getSession().sql("select * from values(3),(2),(1),(3),(2) as T(a)");
    Row[] expected = {Row.create(2.2000)};
    checkAnswer(df.select(Functions.avg(df.col("a"))), expected);
  }

  @Test
  public void corr() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1, 10.0, 0),(2, 10.0, 11.0),"
                    + "(2, 20.0, 22.0),(2, 25.0, 0),(2, 30.0, 35.0) as T(k, v1, v2)");
    Row[] expected = {Row.create(1, null), Row.create(2, 0.40367115665231024)};
    checkAnswer(
        df.groupBy(df.col("k")).agg(Functions.corr(df.col("v1"), df.col("v2"))), expected, false);
  }

  @Test
  public void count() {
    DataFrame df =
        getSession().sql("select * from values(3, 1),(2, 1),(1, 2),(3, 3),(2, 2) as T(a, b)");
    Row[] expected = {Row.create(5)};
    checkAnswer(df.select(Functions.count(df.col("a"))), expected);

    Row[] expected1 = {Row.create(3)};
    checkAnswer(df.select(Functions.count_distinct(df.col("A"))), expected1);

    Row[] expected2 = {Row.create(5)};
    checkAnswer(df.select(Functions.count_distinct(df.col("A"), df.col("b"))), expected2);

    checkAnswer(df.select(Functions.countDistinct(df.col("A"))), expected1);

    checkAnswer(df.select(Functions.countDistinct(df.col("A"), df.col("b"))), expected2);

    checkAnswer(df.select(Functions.countDistinct("A")), expected1);

    checkAnswer(df.select(Functions.countDistinct("A", "b")), expected2);
  }

  @Test
  public void covariance() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1, 10.0, 0),(2, 10.0, 11.0),"
                    + "(2, 20.0, 22.0),(2, 25.0, 0),(2, 30.0, 35.0) as T(k, v1, v2)");
    Row[] expected = {Row.create(1, 0), Row.create(2, 38.75)};

    checkAnswer(
        df.groupBy(df.col("k")).agg(Functions.covar_pop(df.col("v1"), df.col("v2"))),
        expected,
        false);

    Row[] expected1 = {Row.create(1, null), Row.create(2, 51.666666)};
    checkAnswer(
        df.groupBy(df.col("k")).agg(Functions.covar_samp(df.col("v1"), df.col("v2"))),
        expected1,
        false);
  }

  @Test
  public void grouping() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {
      Row.create(1, 2, 0, 0, 0),
      Row.create(2, 1, 0, 0, 0),
      Row.create(2, 2, 0, 0, 0),
      Row.create(1, null, 0, 1, 1),
      Row.create(2, null, 0, 1, 1),
      Row.create(null, null, 1, 1, 3),
      Row.create(null, 2, 1, 0, 2),
      Row.create(null, 1, 1, 0, 2)
    };

    checkAnswer(
        df.cube(df.col("x"), df.col("y"))
            .agg(
                Functions.grouping(df.col("x")),
                Functions.grouping(df.col("y")),
                Functions.grouping_id(df.col("x"), df.col("y"))),
        expected,
        false);

    checkAnswer(
        df.cube("x", "y")
            .agg(
                Functions.grouping(df.col("x")),
                Functions.grouping(df.col("y")),
                Functions.grouping_id(df.col("x"), df.col("y"))),
        expected,
        false);
  }

  @Test
  public void kurtosis() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {Row.create(-3.333333333333, 5.000000000000, 3.613736609956)};

    checkAnswer(
        df.select(
            Functions.kurtosis(df.col("x")),
            Functions.kurtosis(df.col("y")),
            Functions.kurtosis(df.col("z"))),
        expected,
        false);
  }

  @Test
  public void max_min_mean() {
    // Case 01: Non-null values
    DataFrame df1 =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected1 = {Row.create(2, 1, 3.600000)};

    checkAnswer(
        df1.select(
            Functions.max(df1.col("x")), Functions.min(df1.col("y")), Functions.mean(df1.col("z"))),
        expected1,
        false);
    checkAnswer(
        df1.select(Functions.max("x"), Functions.min("y"), Functions.mean("z")), expected1, false);

    // Case 02: Some null values
    DataFrame df2 =
        getSession()
            .sql("select * from values(1,5,8),(null,8,7),(3,null,9),(4,6,null) as T(x,y,z)");
    Row[] expected2 = {Row.create(4, 5, 8.000000)};

    checkAnswer(
        df2.select(
            Functions.max(df2.col("x")), Functions.min(df2.col("y")), Functions.mean(df2.col("z"))),
        expected2,
        false);
    checkAnswer(
        df2.select(Functions.max("x"), Functions.min("y"), Functions.mean("z")), expected2, false);

    // Case 03: All null values
    DataFrame df3 =
        getSession()
            .sql(
                "select * from values(null,null,null),(null,null,null),(null,null,null) as T(x,y,z)");
    Row[] expected3 = {Row.create(null, null, null)};

    checkAnswer(
        df3.select(
            Functions.max(df3.col("x")), Functions.min(df3.col("y")), Functions.mean(df3.col("z"))),
        expected3,
        false);
    checkAnswer(
        df3.select(Functions.max("x"), Functions.min("y"), Functions.mean("z")), expected3, false);
  }

  @Test
  public void median() {
    DataFrame df = getSession().sql("select * from values(3),(2),(1),(3),(2) as T(a)");
    Row[] expected = {Row.create(2.000)};
    checkAnswer(df.select(Functions.median(df.col("a"))), expected);
  }

  @Test
  public void skew() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {Row.create(-0.6085811063146803, -2.236069766354172, 1.8414236309018863)};
    checkAnswer(
        df.select(
            Functions.skew(df.col("x")), Functions.skew(df.col("y")), Functions.skew(df.col("z"))),
        expected);
  }

  @Test
  public void stddev() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {Row.create(0.5477225575051661, 0.4472135954999579, 3.3226495451672298)};
    checkAnswer(
        df.select(
            Functions.stddev(df.col("x")),
            Functions.stddev_samp(df.col("y")),
            Functions.stddev_pop(df.col("z"))),
        expected);
  }

  @Test
  public void sum() {
    DataFrame df = getSession().sql("select * from values(3),(2),(1),(3),(2) as T(a)");
    Row[] expected = {Row.create(3, 6), Row.create(2, 4), Row.create(1, 1)};

    checkAnswer(df.groupBy(df.col("a")).agg(Functions.sum(df.col("a"))), expected, false);

    checkAnswer(df.groupBy(df.col("a")).agg(Functions.sum("a")), expected, false);

    Row[] expected1 = {Row.create(3, 3), Row.create(2, 2), Row.create(1, 1)};
    checkAnswer(df.groupBy(df.col("a")).agg(Functions.sum_distinct(df.col("a"))), expected1, false);
  }

  @Test
  public void variance() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {
      Row.create(1, 0.000000, 1.000000, 2.000000), Row.create(2, 0.333333, 14.888889, 22.333333)
    };
    checkAnswer(
        df.groupBy(df.col("x"))
            .agg(
                Functions.variance(df.col("y")),
                Functions.var_pop(df.col("z")),
                Functions.var_samp(df.col("z"))),
        expected,
        false);
  }

  @Test
  public void approx_percentile() {
    DataFrame df =
        getSession().sql("select * from values(1),(2),(3),(4),(5),(6),(7),(8),(9),(0) as T(a)");
    Row[] expected = {Row.create(4.5)};
    checkAnswer(df.select(Functions.approx_percentile(df.col("a"), 0.5)), expected);
  }

  @Test
  public void approx_percentile_accumulate() {
    DataFrame df =
        getSession().sql("select * from values(1),(2),(3),(4),(5),(6),(7),(8),(9),(0) as T(a)");
    Row[] expected = {
      Row.create(
          "{\n  \"state\": [\n    0.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    1.000000000000000e+00,\n    2.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    3.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "4.000000000000000e+00,\n    1.000000000000000e+00,\n    5.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    6.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "7.000000000000000e+00,\n    1.000000000000000e+00,\n    8.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    9.000000000000000e+00,\n    1.000000000000000e+00\n  ],\n  "
              + "\"type\": \"tdigest\",\n  \"version\": 1\n}")
    };
    checkAnswer(df.select(Functions.approx_percentile_accumulate(df.col("a"))), expected);
  }

  @Test
  public void approx_percentile_estimate() {
    DataFrame df =
        getSession().sql("select * from values(1),(2),(3),(4),(5),(6),(7),(8),(9),(0) as T(a)");
    checkAnswer(
        df.select(
            Functions.approx_percentile_estimate(
                Functions.approx_percentile_accumulate(df.col("a")), 0.5)),
        df.select(Functions.approx_percentile(df.col("a"), 0.5)));
  }

  @Test
  public void approx_percentile_combine() {
    DataFrame df =
        getSession().sql("select * from values(1),(2),(3),(4),(5),(6),(7),(8),(9),(0) as T(a)");
    DataFrame df1 =
        df.select(df.col("a"))
            .where(df.col("a").geq(Functions.lit(3)))
            .select(Functions.approx_percentile_accumulate(df.col("a")).as("b"));
    DataFrame df2 = df.select(Functions.approx_percentile_accumulate(df.col("a")).as("b"));
    DataFrame df3 = df1.union(df2);
    Row[] expected = {
      Row.create(
          "{\n  \"state\": [\n    0.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    1.000000000000000e+00,\n    2.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    3.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "3.000000000000000e+00,\n    1.000000000000000e+00,\n    4.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    4.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "5.000000000000000e+00,\n    1.000000000000000e+00,\n    5.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    6.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "6.000000000000000e+00,\n    1.000000000000000e+00,\n    7.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    7.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "8.000000000000000e+00,\n    1.000000000000000e+00,\n    8.000000000000000e+00,\n    "
              + "1.000000000000000e+00,\n    9.000000000000000e+00,\n    1.000000000000000e+00,\n    "
              + "9.000000000000000e+00,\n    1.000000000000000e+00\n  ],\n  \"type\": \"tdigest\",\n  "
              + "\"version\": 1\n}")
    };

    checkAnswer(df3.select(Functions.approx_percentile_combine(df3.col("b"))), expected, false);
  }

  @Test
  public void coalesce() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(1,2,3),(null,2,3),(null,null,3),(null,null,null),"
                    + "(1,null,3),(1,null,null),(1,2,null) as T(a,b,c)");
    Row[] expected = {
      Row.create(1),
      Row.create(2),
      Row.create(3),
      Row.create((Object) null),
      Row.create(1),
      Row.create(1),
      Row.create(1)
    };
    checkAnswer(
        df.select(Functions.coalesce(df.col("a"), df.col("b"), df.col("c"))), expected, false);
  }

  @Test
  public void equal_nan() {
    DataFrame df =
        getSession().sql("select * from values(1.2),('NaN'::Double),(null),(2.3) as T(a)");
    Row[] expected = {
      Row.create(false, false),
      Row.create(true, false),
      Row.create(null, true),
      Row.create(false, false)
    };
    checkAnswer(
        df.select(Functions.equal_nan(df.col("a")), Functions.is_null(df.col("a"))),
        expected,
        false);
  }

  @Test
  public void cume_dist() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {
      Row.create(0.33333333), Row.create(1.0), Row.create(1.0), Row.create(1.0), Row.create(1.0)
    };
    checkAnswer(
        df.select(Functions.cume_dist().over(Window.partitionBy(df.col("x")).orderBy(df.col("y")))),
        expected,
        false);
  }

  @Test
  public void dense_rank() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {Row.create(1), Row.create(1), Row.create(2), Row.create(2), Row.create(2)};
    checkAnswer(
        df.select(Functions.dense_rank().over(Window.orderBy(df.col("x")))), expected, false);
  }

  @Test
  public void lag() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected1 = {Row.create(0), Row.create(10), Row.create(1), Row.create(0), Row.create(1)};
    checkAnswer(
        df.select(
            Functions.lag(df.col("z"), 1, Functions.lit(0))
                .over(Window.partitionBy(df.col("x")).orderBy(df.col("x")))),
        expected1,
        false);

    Row[] expected2 = {
      Row.create((Object) null),
      Row.create(10),
      Row.create(1),
      Row.create((Object) null),
      Row.create(1)
    };
    checkAnswer(
        df.select(
            Functions.lag(df.col("z"), 1)
                .over(Window.partitionBy(df.col("x")).orderBy(df.col("x")))),
        expected2,
        false);

    checkAnswer(
        df.select(
            Functions.lag(df.col("z")).over(Window.partitionBy(df.col("x")).orderBy(df.col("x")))),
        expected2,
        false);
  }

  @Test
  public void ntile() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)")
            .withColumn("n", Functions.lit(4));
    Row[] expected = {Row.create(1), Row.create(2), Row.create(3), Row.create(1), Row.create(2)};
    checkAnswer(
        df.select(
            Functions.ntile(df.col("n"))
                .over(Window.partitionBy(df.col("x")).orderBy(df.col("y")))),
        expected,
        false);
  }

  @Test
  public void sqrt() {
    DataFrame df =
        getSession().sql("select * from values(1, true, 'a'),(2, false, 'b') as T(num, bool, str)");
    Row[] expected = {Row.create(1.0), Row.create(1.4142135623730951)};
    checkAnswer(df.select(Functions.sqrt(df.col("num"))), expected, false);
  }

  @Test
  public void abs() {
    DataFrame df =
        getSession().sql("select * from values(1, 2, 3),(0, -1, 4),(-5, 0, -9) as T(x,y,z)");
    Row[] expected = {Row.create(1), Row.create(0), Row.create(5)};
    checkAnswer(df.select(Functions.abs(df.col("x"))), expected, false);
  }

  @Test
  public void asin_acos() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Row[] expected = {
      Row.create(1.4706289056333368, 0.1001674211615598),
      Row.create(1.369438406004566, 0.2013579207903308),
      Row.create(1.2661036727794992, 0.3046926540153975)
    };
    checkAnswer(
        df.select(Functions.acos(df.col("a")), Functions.asin(df.col("a"))), expected, false);
  }

  @Test
  public void atan() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Row[] expected = {
      Row.create(0.4636476090008061, 0.09966865249116204),
      Row.create(0.5404195002705842, 0.19739555984988078),
      Row.create(0.6107259643892086, 0.2914567944778671)
    };
    checkAnswer(
        df.select(Functions.atan(df.col("b")), Functions.atan(df.col("a"))), expected, false);

    Row[] expected1 = {
      Row.create(1.373400766945016), Row.create(1.2490457723982544), Row.create(1.1659045405098132)
    };
    checkAnswer(df.select(Functions.atan2(df.col("b"), df.col("a"))), expected1, false);
  }

  @Test
  public void ceil_floor() {
    DataFrame df = getSession().sql("select * from values(1.111),(2.222),(3.333) as T(a)");
    Row[] expected = {Row.create(2, 1), Row.create(3, 2), Row.create(4, 3)};
    checkAnswer(
        df.select(Functions.ceil(df.col("a")), Functions.floor(df.col("a"))), expected, false);
  }

  @Test
  public void cos_cosh() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Row[] expected = {
      Row.create(0.9950041652780258, 1.1276259652063807),
      Row.create(0.9800665778412416, 1.1854652182422676),
      Row.create(0.955336489125606, 1.255169005630943)
    };
    checkAnswer(
        df.select(Functions.cos(df.col("a")), Functions.cosh(df.col("b"))), expected, false);
  }

  @Test
  public void exp() {
    DataFrame df =
        getSession().sql("select * from values(1, 2, 3),(0, -1, 4),(-5, 0, -9) as T(x,y,z)");
    Row[] expected = {
      Row.create(2.718281828459045), Row.create(1.0), Row.create(0.006737946999085467)
    };
    checkAnswer(df.select(Functions.exp(df.col("x"))), expected, false);
  }

  @Test
  public void factorial() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as T(a)");
    Row[] expected = {Row.create(1), Row.create(2), Row.create(6)};
    checkAnswer(df.select(Functions.factorial(df.col("a"))), expected, false);
  }

  @Test
  public void greatest_least() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {Row.create(2), Row.create(3), Row.create(10), Row.create(2), Row.create(3)};
    checkAnswer(
        df.select(Functions.greatest(df.col("x"), df.col("y"), df.col("z"))), expected, false);

    Row[] expected1 = {Row.create(1), Row.create(1), Row.create(1), Row.create(1), Row.create(2)};
    checkAnswer(
        df.select(Functions.least(df.col("x"), df.col("y"), df.col("z"))), expected1, false);
  }

  @Test
  public void log() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as T(a)");
    Row[] expected = {
      Row.create(0.0, 0.0), Row.create(1.0, 0.5), Row.create(1.5849625007211563, 0.7924812503605781)
    };
    checkAnswer(
        df.select(
            Functions.log(Functions.lit(2), df.col("a")),
            Functions.log(Functions.lit(4), df.col("a"))),
        expected,
        false);
  }

  @Test
  public void percent_rank() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {
      Row.create(0.0), Row.create(0.5), Row.create(0.5), Row.create(0.0), Row.create(0.0)
    };
    checkAnswer(
        df.select(
            Functions.percent_rank().over(Window.partitionBy(df.col("x")).orderBy(df.col("y")))),
        expected,
        false);
  }

  @Test
  public void rank() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {Row.create(1), Row.create(2), Row.create(2), Row.create(1), Row.create(1)};
    checkAnswer(
        df.select(Functions.rank().over(Window.partitionBy(df.col("x")).orderBy(df.col("y")))),
        expected,
        false);
  }

  @Test
  public void row_number() {
    DataFrame df =
        getSession()
            .sql("select * from values(1,2,1),(1,2,3),(2,1,10),(2,2,1),(2,2,3) as T(x,y,z)");
    Row[] expected = {Row.create(1), Row.create(2), Row.create(3), Row.create(1), Row.create(2)};
    checkAnswer(
        df.select(
            Functions.row_number().over(Window.partitionBy(df.col("x")).orderBy(df.col("y")))),
        expected,
        false);
  }

  @Test
  public void pow() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Row[] expected = {
      Row.create(0.31622776601683794),
      Row.create(0.3807307877431757),
      Row.create(0.4305116202499342)
    };
    checkAnswer(df.select(Functions.pow(df.col("a"), df.col("b"))), expected, false);
  }

  @Test
  public void round() {
    DataFrame df = getSession().sql("select * from values(1.111),(2.222),(3.333) as T(a)");
    Row[] expected = {Row.create(1.0), Row.create(2.0), Row.create(3.0)};
    checkAnswer(df.select(Functions.round(df.col("a"))), expected, false);
    checkAnswer(df.select(Functions.round(df.col("a"), Functions.lit(0))), expected, false);
  }

  @Test
  public void bitshift() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as T(a)");
    Row[] expected = {Row.create(2, 0), Row.create(4, 1), Row.create(6, 1)};
    checkAnswer(
        df.select(
            Functions.bitshiftleft(df.col("a"), Functions.lit(1)),
            Functions.bitshiftright(df.col("a"), Functions.lit(1))),
        expected,
        false);
  }

  @Test
  public void sin() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Row[] expected = {
      Row.create(0.09983341664682815, 0.10016675001984403),
      Row.create(0.19866933079506122, 0.20133600254109402),
      Row.create(0.29552020666133955, 0.3045202934471426)
    };
    checkAnswer(
        df.select(Functions.sin(df.col("a")), Functions.sinh(df.col("a"))), expected, false);
  }

  @Test
  public void tan() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Row[] expected = {
      Row.create(0.10033467208545055, 0.09966799462495582),
      Row.create(0.2027100355086725, 0.197375320224904),
      Row.create(0.30933624960962325, 0.2913126124515909)
    };
    checkAnswer(
        df.select(Functions.tan(df.col("a")), Functions.tanh(df.col("a"))), expected, false);
  }

  @Test
  public void negate_not() {
    DataFrame df = getSession().sql("select * from values(1, true),(-2,false) as T(a,b)");
    Row[] expected = {Row.create(-1, false), Row.create(2, true)};
    checkAnswer(
        df.select(Functions.negate(df.col("a")), Functions.not(df.col("b"))), expected, false);
  }

  @Test
  public void random() {
    DataFrame df = getSession().sql("select 1");
    df.select(Functions.random(123)).collect();
    df.select(Functions.random()).collect();
  }

  @Test
  public void bitnot() {
    DataFrame df =
        getSession()
            .sql("select * from values(1, true, 'a'), (2, false, 'b') as T(num, bool, str)");
    Row[] expected = {Row.create(-2), Row.create(-3)};
    checkAnswer(df.select(Functions.bitnot(df.col("num"))), expected, false);
  }

  @Test
  public void to_decimal() {
    DataFrame df = getSession().sql("select * from values('1') as t(a)");
    Row[] expected = {Row.create(1)};
    checkAnswer(df.select(Functions.to_decimal(df.col("a"), 10, 0)), expected);
  }

  @Test
  public void div0() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = {Row.create(0.0, 2.0)};
    checkAnswer(
        df.select(
            Functions.div0(Functions.lit(2), Functions.lit(0)),
            Functions.div0(Functions.lit(4), Functions.lit(2))),
        expected);
  }

  @Test
  public void degrees() {
    DataFrame df =
        getSession().sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)");
    Row[] expected = {
      Row.create(5.729577951308233, 28.64788975654116),
      Row.create(11.459155902616466, 34.37746770784939),
      Row.create(17.188733853924695, 40.10704565915762)
    };
    checkAnswer(
        df.select(Functions.degrees(df.col("a")), Functions.degrees(df.col("b"))), expected, false);
  }

  @Test
  public void radians() {
    DataFrame df = getSession().sql("select * from values(1.111),(2.222),(3.333) as T(a)");
    Row[] expected = {
      Row.create(0.019390607989657), Row.create(0.03878121597931), Row.create(0.058171823968971005)
    };
    checkAnswer(df.select(Functions.radians(df.col("a"))), expected, false);
  }

  @Test
  public void md5_sha1_sha2() {
    DataFrame df =
        getSession()
            .sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)");
    Row[] expected = {
      Row.create(
          "5a105e8b9d40e1329780d62ea2265d8a", // pragma: allowlist secret
          "b444ac06613fc8d63795be9ad0beaf55011936ac", // pragma: allowlist secret
          "aff3c83c40e2f1ae099a0166e1f27580525a9de6acd995f21717e984" // pragma: allowlist secret
          ),
      Row.create(
          "ad0234829205b9033196ba818f7a872b", // pragma: allowlist secret
          "109f4b3c50d7b0df729d299bc6f8e9ef9066971f", // pragma: allowlist secret
          "35f757ad7f998eb6dd3dd1cd3b5c6de97348b84a951f13de25355177" // pragma: allowlist secret
          ),
      Row.create(
          "8ad8757baa8564dc136c1e07507f4a98", // pragma: allowlist secret
          "3ebfa301dc59196f18593c45e519287a23297589", // pragma: allowlist secret
          "d2d5c076b2435565f66649edd604dd5987163e8a8240953144ec652f" // pragma: allowlist secret
          )
    };
    checkAnswer(
        df.select(
            Functions.md5(df.col("a")),
            Functions.sha1(df.col("a")),
            Functions.sha2(df.col("a"), 224)),
        expected,
        false);
  }

  @Test
  public void hash() {
    DataFrame df =
        getSession()
            .sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)");
    Row[] expected = {
      Row.create(-1996792119384707157L),
      Row.create(-410379000639015509L),
      Row.create(9028932499781431792L)
    };
    checkAnswer(df.select(Functions.hash(df.col("a"))), expected, false);
  }

  @Test
  public void ascii() {
    DataFrame df =
        getSession()
            .sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)");
    Row[] expected = {Row.create(97), Row.create(98), Row.create(99)};
    checkAnswer(df.select(Functions.ascii(df.col("b"))), expected, false);
  }

  @Test
  public void concat_ws() {
    DataFrame df =
        getSession()
            .sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)");
    Row[] expected = {Row.create("test1,a"), Row.create("test2,b"), Row.create("test3,c")};
    checkAnswer(
        df.select(Functions.concat_ws(Functions.lit(","), df.col("a"), df.col("b"))),
        expected,
        false);
  }

  @Test
  public void initcap_length_lower_upper() {
    DataFrame df = getSession().sql("select * from values('asdFg'),('qqq'),('Qw') as T(a)");
    Row[] expected = {
      Row.create("Asdfg", 5, "asdfg", "ASDFG"),
      Row.create("Qqq", 3, "qqq", "QQQ"),
      Row.create("Qw", 2, "qw", "QW")
    };
    checkAnswer(
        df.select(
            Functions.initcap(df.col("a")),
            Functions.length(df.col("a")),
            Functions.lower(df.col("a")),
            Functions.upper(df.col("a"))),
        expected,
        false);
  }

  @Test
  public void lpad_rpad() {
    DataFrame df = getSession().sql("select * from values('asdFg'),('qqq'),('Qw') as T(a)");
    Row[] expected = {
      Row.create("XXXasdFg", "asdFgSSSS"),
      Row.create("XXXXXqqq", "qqqSSSSSS"),
      Row.create("XXXXXXQw", "QwSSSSSSS")
    };
    checkAnswer(
        df.select(
            Functions.lpad(df.col("a"), Functions.lit(8), Functions.lit("X")),
            Functions.rpad(df.col("a"), Functions.lit(9), Functions.lit("S"))),
        expected,
        false);
  }

  @Test
  public void ltrim_rtrim_trim() {
    DataFrame df = getSession().sql("select * from values('  abcba  '), (' a12321a   ') as T(a)");
    Row[] expected = {
      Row.create("bcba  ", "  abcb", "bcb"), Row.create("12321a   ", " a12321", "12321")
    };
    checkAnswer(
        df.select(
            Functions.ltrim(df.col("a"), Functions.lit(" a")),
            Functions.rtrim(df.col("a"), Functions.lit(" a")),
            Functions.trim(df.col("a"), Functions.lit("a "))),
        expected,
        false);

    Row[] expected2 = {Row.create("abcba  ", "  abcba"), Row.create("a12321a   ", " a12321a")};

    checkAnswer(
        df.select(Functions.ltrim(df.col("a")), Functions.rtrim(df.col("a"))), expected2, false);
  }

  @Test
  public void repeat() {
    DataFrame df =
        getSession()
            .sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)");
    Row[] expected = {Row.create("aaa"), Row.create("bbb"), Row.create("ccc")};
    checkAnswer(df.select(Functions.repeat(df.col("B"), Functions.lit(3))), expected, false);
  }

  @Test
  public void soundex() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create("a140"), Row.create("b550"), Row.create("p200")};
    checkAnswer(df.select(Functions.soundex(df.col("a"))), expected, false);
  }

  @Test
  public void split() {
    DataFrame df = getSession().sql("select * from values('1,2,3,4,5') as T(a)");
    assert df.select(Functions.split(df.col("a"), Functions.lit(",")))
        .collect()[0]
        .getString(0)
        .replaceAll("[ \n]", "")
        .equals("[\"1\",\"2\",\"3\",\"4\",\"5\"]");
  }

  @Test
  public void substring() {
    DataFrame df =
        getSession()
            .sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)");
    Row[] expected = {Row.create("est1"), Row.create("est2"), Row.create("est3")};
    checkAnswer(
        df.select(Functions.substring(df.col("a"), Functions.lit(2), Functions.lit(4))),
        expected,
        false);
  }

  @Test
  public void translate() {
    DataFrame df = getSession().sql("select * from values('  abcba  '), (' a12321a   ') as T(a)");
    Row[] expected = {Row.create("XYcYX"), Row.create("X12321X")};
    checkAnswer(
        df.select(Functions.translate(df.col("a"), Functions.lit("ab "), Functions.lit("XY"))),
        expected,
        false);
  }

  @Test
  public void contains() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create(true), Row.create(false), Row.create(false)};
    checkAnswer(df.select(Functions.contains(df.col("a"), Functions.lit("app"))), expected, false);
  }

  @Test
  public void startswith() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create(false), Row.create(true), Row.create(false)};
    checkAnswer(
        df.select(Functions.startswith(df.col("a"), Functions.lit("ban"))), expected, false);
  }

  @Test
  public void chr() {
    DataFrame df = getSession().sql("select * from values(84,85),(96,97) as T(a, b)");
    Row[] expected = {Row.create("T", "U"), Row.create("`", "a")};
    checkAnswer(df.select(Functions.chr(df.col("a")), Functions.chr(df.col("b"))), expected, false);
  }

  @Test
  public void add_month() {
    DataFrame df =
        getSession()
            .sql("select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)");
    Row[] expected = {
      Row.create(Date.valueOf("2020-09-01")), Row.create(Date.valueOf("2011-01-01"))
    };
    checkAnswer(df.select(Functions.add_months(df.col("a"), Functions.lit(1))), expected, false);
  }

  @Test
  public void current_date() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_date()").collect();
    checkAnswer(df.select(Functions.current_date()), expected);
  }

  @Test
  public void current_timestamp() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert Math.abs(
            df.select(Functions.current_timestamp()).collect()[0].getTimestamp(0).getTime()
                - System.currentTimeMillis())
        < 100000;
  }

  @Test
  public void current_region() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_region()").collect();
    checkAnswer(df.select(Functions.current_region()), expected);
  }

  @Test
  public void current_time() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert df.select(Functions.current_time())
        .collect()[0]
        .getTime(0)
        .toString()
        .matches("\\d{2}:\\d{2}:\\d{2}");
  }

  @Test
  public void current_version() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_version()").collect();
    checkAnswer(df.select(Functions.current_version()), expected);
  }

  @Test
  public void current_account() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_account()").collect();
    checkAnswer(df.select(Functions.current_account()), expected);
  }

  @Test
  public void current_role() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_role()").collect();
    checkAnswer(df.select(Functions.current_role()), expected);
  }

  @Test
  public void current_available_roles() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_available_roles()").collect();
    checkAnswer(df.select(Functions.current_available_roles()), expected, false);
  }

  @Test
  public void current_session() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_session()").collect();
    checkAnswer(df.select(Functions.current_session()), expected, false);
  }

  @Test
  public void current_statement() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert df.select(Functions.current_statement())
        .collect()[0]
        .getString(0)
        .trim()
        .startsWith("SELECT current_statement()");
  }

  @Test
  public void current_user() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_user()").collect();
    checkAnswer(df.select(Functions.current_user()), expected, false);
  }

  @Test
  public void current_database() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_database()").collect();
    checkAnswer(df.select(Functions.current_database()), expected, false);
  }

  @Test
  public void current_schema() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_schema()").collect();
    checkAnswer(df.select(Functions.current_schema()), expected, false);
  }

  @Test
  public void current_schemas() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_schemas()").collect();
    checkAnswer(df.select(Functions.current_schemas()), expected, false);
  }

  @Test
  public void current_warehouse() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = getSession().sql("select current_warehouse()").collect();
    checkAnswer(df.select(Functions.current_warehouse()), expected, false);
  }

  @Test
  public void sysdate() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert df.select(Functions.sysdate()).collect()[0].getTimestamp(0).toString().length() > 0;
  }

  @Test
  public void convert_timezone() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values('2020-05-01 13:11:20.000' :: timestamp_ntz),"
                    + "('2020-08-21 01:30:05.000' :: timestamp_ntz) as T(a)");
    Row[] expected = {
      Row.create(Timestamp.valueOf("2020-05-01 16:11:20.0")),
      Row.create(Timestamp.valueOf("2020-08-21 04:30:05.0"))
    };
    checkAnswer(
        df.select(
            Functions.convert_timezone(
                Functions.lit("America/Los_Angeles"),
                Functions.lit("America/New_York"),
                df.col("a"))),
        expected,
        false);

    DataFrame df2 =
        getSession()
            .sql(
                "select * from values('2020-05-01 16:11:20.0 +02:00',"
                    + "'2020-08-21 04:30:05.0 -06:00') as T(a, b)");
    Row[] expected2 = {
      Row.create(
          Timestamp.valueOf("2020-05-01 07:11:20.0"), Timestamp.valueOf("2020-08-21 06:30:05.0"))
    };
    checkAnswer(
        df2.select(
            Functions.convert_timezone(Functions.lit("America/Los_Angeles"), df2.col("a")),
            Functions.convert_timezone(Functions.lit("America/New_York"), df2.col("b"))),
        expected2,
        false);
  }

  @Test
  public void year_month_day() {
    DataFrame df =
        getSession()
            .sql("select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)");
    Row[] expected = {
      Row.create(2020, 8, 1, 6, 214, 3, 31, new Date(120, 7, 31)),
      Row.create(2010, 12, 1, 3, 335, 4, 48, new Date(110, 11, 31))
    };
    checkAnswer(
        df.select(
            Functions.year(df.col("a")),
            Functions.month(df.col("a")),
            Functions.dayofmonth(df.col("a")),
            Functions.dayofweek(df.col("a")),
            Functions.dayofyear(df.col("a")),
            Functions.quarter(df.col("a")),
            Functions.weekofyear(df.col("a")),
            Functions.last_day(df.col("a"))),
        expected,
        false);
  }

  @Test
  public void hour_minute_second() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values('2020-05-01 13:11:20.000' :: timestamp),"
                    + "('2020-08-21 01:30:05.000' :: timestamp) as T(a)");
    Row[] expected = {Row.create(13, 11, 20), Row.create(1, 30, 5)};
    checkAnswer(
        df.select(
            Functions.hour(df.col("a")),
            Functions.minute(df.col("a")),
            Functions.second(df.col("a"))),
        expected,
        false);
  }

  @Test
  public void nextDay() {
    DataFrame df =
        getSession()
            .sql("select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)");
    Row[] expected = {Row.create(new Date(120, 7, 7)), Row.create(new Date(110, 11, 3))};
    checkAnswer(df.select(Functions.next_day(df.col("a"), Functions.lit("FR"))), expected, false);
  }

  @Test
  public void previousDay() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values('2020-08-01'::Date, 'mo'),('2010-12-01'::Date, 'we') as T(a,b)");
    Row[] expected = {Row.create(new Date(120, 6, 27)), Row.create(new Date(110, 10, 24))};
    checkAnswer(df.select(Functions.previous_day(df.col("a"), df.col("b"))), expected, false);
  }

  @Test
  public void to_timestamp() {
    DataFrame df =
        getSession().sql("select * from values(1561479557),(1565479557),(1161479557) as T(a)");
    Row[] expected = {
      Row.create(Timestamp.valueOf("2019-06-25 16:19:17.0")),
      Row.create(Timestamp.valueOf("2019-08-10 23:25:57.0")),
      Row.create(Timestamp.valueOf("2006-10-22 01:12:37.0"))
    };
    checkAnswer(df.select(Functions.to_timestamp(df.col("a"))), expected, false);

    DataFrame df2 = getSession().sql("select * from values('04/05/2020 01:02:03') as T(a)");
    Row[] expected2 = {Row.create(Timestamp.valueOf("2020-04-05 01:02:03.0"))};
    checkAnswer(
        df2.select(Functions.to_timestamp(df2.col("a"), Functions.lit("mm/dd/yyyy hh24:mi:ss"))),
        expected2,
        false);
  }

  @Test
  public void to_date() {
    DataFrame df = getSession().sql("select * from values('2020-05-11') as T(a)");
    Row[] expected = {Row.create(new Date(120, 4, 11))};
    checkAnswer(df.select(Functions.to_date(df.col("a"))), expected, false);

    DataFrame df1 = getSession().sql("select * from values('2020.07.23') as T(a)");
    Row[] expected1 = {Row.create(new Date(120, 6, 23))};
    checkAnswer(
        df1.select(Functions.to_date(df.col("a"), Functions.lit("YYYY.MM.DD"))), expected1, false);
  }

  @Test
  public void date_from_parts() {
    DataFrame df = getSession().sql("select * from values(2020, 9, 16) as t(year, month, day)");
    Row[] expected = {Row.create(new Date(120, 8, 16))};
    checkAnswer(
        df.select(Functions.date_from_parts(df.col("year"), df.col("month"), df.col("day"))),
        expected);
  }

  @Test
  public void time_from_parts() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert df.select(
            Functions.time_from_parts(Functions.lit(1), Functions.lit(2), Functions.lit(3)))
        .collect()[0]
        .getTime(0)
        .equals(new Time(3723000));

    assert df.select(
            Functions.time_from_parts(
                Functions.lit(1), Functions.lit(2), Functions.lit(3), Functions.lit(444444444)))
        .collect()[0]
        .getTime(0)
        .equals(new Time(3723444));
  }

  @Test
  public void timestamp_from_parts() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(2020, 10, 28, 13, 35, 47, 1234567, 'America/Los_Angeles') as T(year, month, day, hour, minute, second, nanosecond, timezone)");
    assert df.select(
            Functions.timestamp_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.0");

    assert df.select(
            Functions.timestamp_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second"),
                df.col("nanosecond")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.001234567");

    assert df.select(
            Functions.timestamp_from_parts(
                Functions.date_from_parts(df.col("year"), df.col("month"), df.col("day")),
                Functions.time_from_parts(
                    df.col("hour"), df.col("minute"), df.col("second"), df.col("nanosecond"))))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.001234567");
  }

  @Test
  public void timestamp_ltz_from_parts() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(2020, 10, 28, 13, 35, 47, 1234567, 'America/Los_Angeles') as T(year, month, day, hour, minute, second, nanosecond, timezone)");

    assert df.select(
            Functions.timestamp_ltz_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.0");

    assert df.select(
            Functions.timestamp_ltz_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second"),
                df.col("nanosecond")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.001234567");
  }

  @Test
  public void timestamp_ntz_from_parts() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(2020, 10, 28, 13, 35, 47, 1234567, 'America/Los_Angeles') as T(year, month, day, hour, minute, second, nanosecond, timezone)");

    assert df.select(
            Functions.timestamp_ntz_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.0");

    assert df.select(
            Functions.timestamp_ntz_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second"),
                df.col("nanosecond")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.001234567");

    assert df.select(
            Functions.timestamp_ntz_from_parts(
                Functions.date_from_parts(df.col("year"), df.col("month"), df.col("day")),
                Functions.time_from_parts(
                    df.col("hour"), df.col("minute"), df.col("second"), df.col("nanosecond"))))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.001234567");
  }

  @Test
  public void timestamp_tz_from_parts() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(2020, 10, 28, 13, 35, 47, 1234567, 'America/Los_Angeles') as T(year, month, day, hour, minute, second, nanosecond, timezone)");

    assert df.select(
            Functions.timestamp_tz_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.0");

    assert df.select(
            Functions.timestamp_tz_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second"),
                df.col("nanosecond")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.001234567");

    assert df.select(
            Functions.timestamp_tz_from_parts(
                df.col("year"),
                df.col("month"),
                df.col("day"),
                df.col("hour"),
                df.col("minute"),
                df.col("second"),
                df.col("nanosecond"),
                df.col("timezone")))
        .collect()[0]
        .getTimestamp(0)
        .toString()
        .equals("2020-10-28 13:35:47.001234567");
  }

  @Test
  public void dayname() {
    DataFrame df =
        getSession()
            .sql("select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)");
    Row[] expected = {Row.create("Sat"), Row.create("Wed")};
    checkAnswer(df.select(Functions.dayname(df.col("a"))), expected, false);
  }

  @Test
  public void monthname() {
    DataFrame df =
        getSession()
            .sql("select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)");
    Row[] expected = {Row.create("Aug"), Row.create("Dec")};
    checkAnswer(df.select(Functions.monthname(df.col("a"))), expected, false);
  }

  @Test
  public void dateadd() {
    DataFrame df =
        getSession()
            .sql("select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)");
    Row[] expected = {Row.create(new Date(121, 7, 1)), Row.create(new Date(111, 11, 1))};
    checkAnswer(
        df.select(Functions.dateadd("year", Functions.lit(1), df.col("a"))), expected, false);
  }

  @Test
  public void datediff() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values('2020-05-01 13:11:20.000' :: timestamp),"
                    + "('2020-08-21 01:30:05.000' :: timestamp) as T(a)");
    Row[] expected = {Row.create(1), Row.create(1)};
    checkAnswer(
        df.select(df.col("a"), Functions.dateadd("year", Functions.lit(1), df.col("a")).as("b"))
            .select(Functions.datediff("year", Functions.col("a"), Functions.col("b"))),
        expected);
  }

  @Test
  public void trunc() {
    DataFrame df = getSession().sql("select * from values(3.14, 1) as t(expr, scale)");
    Row[] expected = {Row.create(3.1)};
    checkAnswer(df.select(Functions.trunc(df.col("expr"), df.col("scale"))), expected);
  }

  @Test
  public void date_trunc() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values('2020-05-01 13:11:20.000' :: timestamp),"
                    + "('2020-08-21 01:30:05.000' :: timestamp) as T(a)");
    Row[] expected = {
      Row.create(Timestamp.valueOf("2020-04-01 00:00:00.0")),
      Row.create(Timestamp.valueOf("2020-07-01 00:00:00.0"))
    };
    checkAnswer(df.select(Functions.date_trunc("quarter", df.col("a"))), expected, false);
  }

  @Test
  public void concat() {
    DataFrame df =
        getSession()
            .sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)");
    Row[] expected = {Row.create("test1a"), Row.create("test2b"), Row.create("test3c")};
    checkAnswer(df.select(Functions.concat(df.col("a"), df.col("b"))), expected, false);
  }

  @Test
  public void array_overlap() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create(true), Row.create(false)};
    checkAnswer(
        df.select(Functions.arrays_overlap(df.col("arr1"), df.col("arr2"))), expected, false);
  }

  @Test
  public void endswith() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create("apple")};
    checkAnswer(df.filter(Functions.endswith(df.col("a"), Functions.lit("le"))), expected);
  }

  @Test
  public void insert() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create("aabce"), Row.create("babcna"), Row.create("pabch")};
    checkAnswer(
        df.select(
            Functions.insert(
                df.col("a"), Functions.lit(2), Functions.lit(3), Functions.lit("abc"))),
        expected,
        false);
  }

  @Test
  public void left() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create("ap"), Row.create("ba"), Row.create("pe")};
    checkAnswer(df.select(Functions.left(df.col("a"), Functions.lit(2))), expected, false);
  }

  @Test
  public void right() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create("le"), Row.create("na"), Row.create("ch")};
    checkAnswer(df.select(Functions.right(df.col("a"), Functions.lit(2))), expected, false);
  }

  @Test
  public void regexp_count() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create(0), Row.create(3), Row.create(1)};
    checkAnswer(
        df.select(
            Functions.regexp_count(
                df.col("a"), Functions.lit("a"), Functions.lit(2), Functions.lit("c"))),
        expected,
        false);

    Row[] expected1 = {Row.create(1), Row.create(3), Row.create(1)};
    checkAnswer(
        df.select(Functions.regexp_count(df.col("a"), Functions.lit("a"))), expected1, false);
  }

  @Test
  public void regexp_replace() {
    DataFrame df = getSession().sql("select * from values('cat'),('dog'),('mouse') as T(a)");
    Column pattern = Functions.lit("^ca|^[m|d]o");
    Row[] expected = {Row.create("t"), Row.create("g"), Row.create("use")};
    checkAnswer(df.select(Functions.regexp_replace(df.col("a"), pattern)), expected, false);

    Column replacement = Functions.lit("ch");
    Row[] expected1 = {Row.create("cht"), Row.create("chg"), Row.create("chuse")};
    checkAnswer(
        df.select(Functions.regexp_replace(df.col("a"), pattern, replacement)), expected1, false);
  }

  @Test
  public void replace() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create("zpple"), Row.create("bznznz"), Row.create("pezch")};
    checkAnswer(
        df.select(Functions.replace(df.col("a"), Functions.lit("a"), Functions.lit("z"))),
        expected,
        false);

    Row[] expected1 = {Row.create("pple"), Row.create("bnn"), Row.create("pech")};
    checkAnswer(df.select(Functions.replace(df.col("a"), Functions.lit("a"))), expected1, false);
  }

  @Test
  public void charindex() {
    DataFrame df = getSession().sql("select * from values('apple'),('banana'),('peach') as T(a)");
    Row[] expected = {Row.create(0), Row.create(3), Row.create(0)};
    checkAnswer(df.select(Functions.charindex(Functions.lit("na"), df.col("a"))), expected, false);

    Row[] expected1 = {Row.create(0), Row.create(5), Row.create(0)};
    checkAnswer(
        df.select(Functions.charindex(Functions.lit("na"), df.col("a"), Functions.lit(4))),
        expected1,
        false);
  }

  @Test
  public void collate() {
    DataFrame df = getSession().sql("select * from values('  abcba  '), (' a12321a   ') as T(a)");
    Row[] expected = {Row.create("  abcba  ")};
    checkAnswer(
        df.where(Functions.collate(df.col("a"), "en_US-trim").equal_to(Functions.lit("abcba"))),
        expected,
        false);
  }

  @Test
  public void collation() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    Row[] expected = {Row.create("de")};
    checkAnswer(df.select(Functions.collation(Functions.lit("f").collate("de"))), expected);
  }

  @Test
  public void array_intersection() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create("[\n  3\n]"), Row.create("[]")};
    checkAnswer(
        df.select(Functions.array_intersection(df.col("arr1"), df.col("arr2"))), expected, false);
  }

  @Test
  public void is_array() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create(true), Row.create(true)};
    checkAnswer(df.select(Functions.is_array(df.col("arr1"))), expected, false);
  }

  @Test
  public void is_boolean() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(false, true, false)};
    checkAnswer(
        df.select(
            Functions.is_boolean(df.col("arr1")),
            Functions.is_boolean(df.col("bool1")),
            Functions.is_boolean(df.col("str1"))),
        expected,
        false);
  }

  @Test
  public void is_binary() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(true, false, false)};
    checkAnswer(
        df.select(
            Functions.is_binary(df.col("bin1")),
            Functions.is_binary(df.col("bool1")),
            Functions.is_binary(df.col("str1"))),
        expected,
        false);
  }

  @Test
  public void is_char() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(true, false, false)};
    checkAnswer(
        df.select(
            Functions.is_char(df.col("str1")),
            Functions.is_char(df.col("bin1")),
            Functions.is_char(df.col("bool1"))),
        expected,
        false);

    checkAnswer(
        df.select(
            Functions.is_varchar(df.col("str1")),
            Functions.is_varchar(df.col("bin1")),
            Functions.is_varchar(df.col("bool1"))),
        expected,
        false);
  }

  @Test
  public void is_date() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(true, false, false)};
    checkAnswer(
        df.select(
            Functions.is_date(df.col("date1")),
            Functions.is_date(df.col("time1")),
            Functions.is_date(df.col("bool1"))),
        expected,
        false);

    checkAnswer(
        df.select(
            Functions.is_date_value(df.col("date1")),
            Functions.is_date_value(df.col("time1")),
            Functions.is_date_value(df.col("bool1"))),
        expected,
        false);
  }

  @Test
  public void is_decimal() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(true, false, true)};
    checkAnswer(
        df.select(
            Functions.is_decimal(df.col("decimal1")),
            Functions.is_decimal(df.col("double1")),
            Functions.is_decimal(df.col("num1"))),
        expected,
        false);
  }

  @Test
  public void is_double() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(true, true, true, false)};
    checkAnswer(
        df.select(
            Functions.is_double(df.col("decimal1")),
            Functions.is_double(df.col("double1")),
            Functions.is_double(df.col("num1")),
            Functions.is_double(df.col("bool1"))),
        expected,
        false);

    checkAnswer(
        df.select(
            Functions.is_real(df.col("decimal1")),
            Functions.is_real(df.col("double1")),
            Functions.is_real(df.col("num1")),
            Functions.is_real(df.col("bool1"))),
        expected,
        false);
  }

  @Test
  public void is_integer() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(false, false, true, false)};
    checkAnswer(
        df.select(
            Functions.is_integer(df.col("decimal1")),
            Functions.is_integer(df.col("double1")),
            Functions.is_integer(df.col("num1")),
            Functions.is_integer(df.col("bool1"))),
        expected,
        false);
  }

  @Test
  public void is_null_value() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_json(column1) as v  from values ('{\"a\": null}'), ('{\"a\": \"foo\"}'), (null)");
    Row[] expected = {Row.create(true), Row.create(false), Row.create((Object) null)};
    checkAnswer(df.select(Functions.is_null_value(Functions.sqlExpr("v:a"))), expected, false);
  }

  @Test
  public void is_object() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(true, false, false)};
    checkAnswer(
        df.select(
            Functions.is_object(df.col("obj1")),
            Functions.is_object(df.col("arr1")),
            Functions.is_object(df.col("str1"))),
        expected,
        false);
  }

  @Test
  public void is_time() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(true, false, false)};
    checkAnswer(
        df.select(
            Functions.is_time(df.col("time1")),
            Functions.is_time(df.col("date1")),
            Functions.is_time(df.col("timestamp_tz1"))),
        expected,
        false);
  }

  @Test
  public void is_timestamp() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");

    Row[] expected = {Row.create(true, false, false)};
    checkAnswer(
        df.select(
            Functions.is_timestamp_ntz(df.col("timestamp_ntz1")),
            Functions.is_timestamp_ntz(df.col("timestamp_tz1")),
            Functions.is_timestamp_ntz(df.col("timestamp_ltz1"))),
        expected,
        false);

    Row[] expected1 = {Row.create(false, false, true)};
    checkAnswer(
        df.select(
            Functions.is_timestamp_ltz(df.col("timestamp_ntz1")),
            Functions.is_timestamp_ltz(df.col("timestamp_tz1")),
            Functions.is_timestamp_ltz(df.col("timestamp_ltz1"))),
        expected1,
        false);

    Row[] expected2 = {Row.create(false, true, false)};
    checkAnswer(
        df.select(
            Functions.is_timestamp_tz(df.col("timestamp_ntz1")),
            Functions.is_timestamp_tz(df.col("timestamp_tz1")),
            Functions.is_timestamp_tz(df.col("timestamp_ltz1"))),
        expected2,
        false);
  }

  @Test
  public void check_json() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_json(column1) as v  from values ('{\"a\": null}'), ('{\"a\": \"foo\"}'), (null)");
    Row[] expected = {
      Row.create((Object) null), Row.create((Object) null), Row.create((Object) null)
    };
    checkAnswer(df.select(Functions.check_json(df.col("v"))), expected, false);
  }

  @Test
  public void check_xml() {
    DataFrame df =
        getSession()
            .sql(
                "select (column1) as v from values ('<t1>foo<t2>bar</t2><t3></t3></t1>'), "
                    + "('<t1></t1>'), (null), ('')");
    Row[] expected = {
      Row.create((Object) null),
      Row.create((Object) null),
      Row.create((Object) null),
      Row.create((Object) null)
    };
    checkAnswer(df.select(Functions.check_xml(df.col("v"))), expected, false);
  }

  @Test
  public void json_extract_path_text() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_json(column1) as v, column2 as k from values ('{\"a\": null}','a'), "
                    + "('{\"a\": \"foo\"}','a'), ('{\"a\": \"foo\"}','b'), (null,'a')");
    Row[] expected = {
      Row.create((Object) null),
      Row.create("foo"),
      Row.create((Object) null),
      Row.create((Object) null)
    };
    checkAnswer(
        df.select(Functions.json_extract_path_text(df.col("v"), df.col("k"))), expected, false);
  }

  @Test
  public void parse_json() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_json(column1) as v  from values ('{\"a\": null}'), ('{\"a\": \"foo\"}'), (null)");
    Row[] expected = {
      Row.create("{\n  \"a\": null\n}"),
      Row.create("{\n  \"a\": \"foo\"\n}"),
      Row.create((Object) null)
    };
    checkAnswer(df.select(Functions.parse_json(df.col("v"))), expected, false);
  }

  @Test
  public void parse_xml() {
    DataFrame df =
        getSession()
            .sql(
                "select (column1) as v from values ('<t1>foo<t2>bar</t2><t3></t3></t1>'), "
                    + "('<t1></t1>'), (null), ('')");
    Row[] expected = {
      Row.create("<t1>\n  foo\n  <t2>bar</t2>\n  <t3></t3>\n</t1>"),
      Row.create("<t1></t1>"),
      Row.create((Object) null),
      Row.create((Object) null)
    };
    checkAnswer(df.select(Functions.parse_xml(df.col("v"))), expected, false);
  }

  @Test
  public void strip_null_value() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_json(column1) as v  from values ('{\"a\": null}'), ('{\"a\": \"foo\"}'), (null)");
    Row[] expected = {Row.create((Object) null), Row.create("\"foo\""), Row.create((Object) null)};
    checkAnswer(df.select(Functions.strip_null_value(Functions.sqlExpr("v:a"))), expected, false);
  }

  @Test
  public void array_agg() {
    DataFrame df = getSession().sql("select * from values(1), (2), (3) as t(a)");
    df.select(Functions.array_agg(df.col("a"))).show();
  }

  @Test
  public void array_append() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)");
    Row[] expected = {
      Row.create("[\n  1,\n  2,\n  3,\n  \"amount\"\n]"),
      Row.create("[\n  6,\n  7,\n  8,\n  \"amount\"\n]")
    };
    checkAnswer(
        df.select(Functions.array_append(df.col("arr1"), Functions.lit("amount"))),
        expected,
        false);
  }

  @Test
  public void array_cat() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)");
    Row[] expected = {
      Row.create("[\n  1,\n  2,\n  3,\n  3,\n  4,\n  5\n]"),
      Row.create("[\n  6,\n  7,\n  8,\n  9,\n  0,\n  1\n]")
    };
    checkAnswer(df.select(Functions.array_cat(df.col("arr1"), df.col("arr2"))), expected, false);
  }

  @Test
  public void array_compact() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,null,3,3,null,5),(6,null,8,9,null,1) as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create("[\n  1,\n  3\n]"), Row.create("[\n  6,\n  8\n]")};
    checkAnswer(df.select(Functions.array_compact(df.col("arr1"))), expected, false);
  }

  @Test
  public void array_construct() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert df.select(Functions.array_construct(Functions.lit(1), Functions.lit(1.2)))
        .collect()[0]
        .getString(0)
        .equals("[\n  1,\n  1.200000000000000e+00\n]");
  }

  @Test
  public void array_construct_compact() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert df.select(Functions.array_construct_compact(Functions.lit(1), Functions.lit(1.2)))
        .collect()[0]
        .getString(0)
        .equals("[\n  1,\n  1.200000000000000e+00\n]");
  }

  @Test
  public void array_contains() {
    DataFrame df = getSession().sql("select * from values(0) as T(a)");
    assert df.select(
            Functions.array_contains(
                Functions.lit(1), Functions.array_construct(Functions.lit(1), Functions.lit(1.2))))
        .collect()[0]
        .getBoolean(0);
  }

  @Test
  public void array_insert() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, d, e, f "
                    + "from values(1,2,3,2,'e1','[{a:1}]'),(6,7,8,1,'e2','[{a:1},{b:2}]') as T(a,b,c,d,e,f)");
    Row[] expected = {
      Row.create("[\n  1,\n  2,\n  \"e1\",\n  3\n]"), Row.create("[\n  6,\n  \"e2\",\n  7,\n  8\n]")
    };
    checkAnswer(
        df.select(Functions.array_insert(df.col("arr1"), df.col("d"), df.col("e"))),
        expected,
        false);
  }

  @Test
  public void array_position() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, d, e, f "
                    + "from values(1,2,3,2,'e1','[{a:1}]'),(6,7,8,1,'e2','[{a:1},{b:2}]') as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create(1), Row.create((Object) null)};
    checkAnswer(df.select(Functions.array_position(df.col("d"), df.col("arr1"))), expected, false);
  }

  @Test
  public void array_prepend() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)");
    Row[] expected = {
      Row.create("[\n  3.221000000000000e+01,\n  \"amount\",\n  1,\n  2,\n  3\n]"),
      Row.create("[\n  3.221000000000000e+01,\n  \"amount\",\n  6,\n  7,\n  8\n]")
    };
    checkAnswer(
        df.select(
            Functions.array_prepend(
                Functions.array_prepend(df.col("arr1"), Functions.lit("amount")),
                Functions.lit(32.21))),
        expected,
        false);
  }

  @Test
  public void array_size() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, d, e, f "
                    + "from values(1,2,3,2,'e1','[{a:1}]'),(6,7,8,1,'e2','[{a:1},{b:2}]') as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create(3), Row.create(3)};
    checkAnswer(df.select(Functions.array_size(df.col("arr1"))), expected, false);
  }

  @Test
  public void array_slice() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, d, e, f "
                    + "from values(1,2,3,1,2,','),(4,5,6,1,-1,', '),(6,7,8,0,2,';') as T(a,b,c,d,e,f)");
    Row[] expected = {
      Row.create("[\n  2\n]"), Row.create("[\n  5\n]"), Row.create("[\n  6,\n  7\n]")
    };
    checkAnswer(
        df.select(Functions.array_slice(df.col("arr1"), df.col("d"), df.col("e"))),
        expected,
        false);
  }

  @Test
  public void array_to_string() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, d, e, f "
                    + "from values(1,2,3,1,2,','),(4,5,6,1,-1,', '),(6,7,8,0,2,';') as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create("1,2,3"), Row.create("4, 5, 6"), Row.create("6;7;8")};
    checkAnswer(df.select(Functions.array_to_string(df.col("arr1"), df.col("f"))), expected, false);
  }

  @Test
  public void objectagg() {
    DataFrame df =
        getSession()
            .sql(
                "select key, to_variant(value) as value from values('age', 21),('zip', "
                    + "94401) as T(key,value)");
    Row[] expected = {Row.create("{\n  \"age\": 21,\n  \"zip\": 94401\n}")};
    checkAnswer(df.select(Functions.objectagg(df.col("key"), df.col("value"))), expected, false);
  }

  @Test
  public void object_construct() {
    DataFrame df =
        getSession()
            .sql(
                "select key, to_variant(value) as value from values('age', 21),('zip', "
                    + "94401) as T(key,value)");
    Row[] expected = {Row.create("{\n  \"age\": 21\n}"), Row.create("{\n  \"zip\": 94401\n}")};
    checkAnswer(
        df.select(Functions.object_construct(df.col("key"), df.col("value"))), expected, false);
  }

  @Test
  public void object_delete() {
    DataFrame df =
        getSession()
            .sql(
                "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from values('age', 21, 'zip', "
                    + "21021, 'name', 'Joe', 'age', 0, true),('age', 26, 'zip', 94021, 'name', 'Jay', 'key', "
                    + "0, false) as T(a,b,c,d,e,f,k,v,flag)");
    Row[] expected = {
      Row.create("{\n  \"zip\": 21021\n}"), Row.create("{\n  \"age\": 26,\n  \"zip\": 94021\n}")
    };
    checkAnswer(
        df.select(
            Functions.object_delete(
                df.col("obj"), df.col("k"), Functions.lit("name"), Functions.lit("non-exist-key"))),
        expected,
        false);
  }

  @Test
  public void object_insert() {
    DataFrame df =
        getSession()
            .sql(
                "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from values('age', 21, 'zip', "
                    + "21021, 'name', 'Joe', 'age', 0, true),('age', 26, 'zip', 94021, 'name', 'Jay', 'key', "
                    + "0, false) as T(a,b,c,d,e,f,k,v,flag)");
    Row[] expected = {
      Row.create("{\n  \"age\": 21,\n  \"key\": \"v\",\n  \"name\": \"Joe\",\n  \"zip\": 21021\n}"),
      Row.create("{\n  \"age\": 26,\n  \"key\": \"v\",\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")
    };
    checkAnswer(
        df.select(Functions.object_insert(df.col("obj"), Functions.lit("key"), Functions.lit("v"))),
        expected,
        false);

    Row[] expected1 = {
      Row.create("{\n  \"age\": 0,\n  \"name\": \"Joe\",\n  \"zip\": 21021\n}"),
      Row.create("{\n  \"age\": 26,\n  \"key\": 0,\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")
    };
    checkAnswer(
        df.select(Functions.object_insert(df.col("obj"), df.col("k"), df.col("v"), df.col("flag"))),
        expected1,
        false);
  }

  @Test
  public void object_pick() {
    DataFrame df =
        getSession()
            .sql(
                "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from values('age', 21, 'zip', "
                    + "21021, 'name', 'Joe', 'age', 0, true),('age', 26, 'zip', 94021, 'name', 'Jay', 'key', "
                    + "0, false) as T(a,b,c,d,e,f,k,v,flag)");
    Row[] expected = {
      Row.create("{\n  \"age\": 21,\n  \"name\": \"Joe\"\n}"),
      Row.create("{\n  \"name\": \"Jay\"\n}")
    };
    checkAnswer(
        df.select(
            Functions.object_pick(
                df.col("obj"), df.col("k"), Functions.lit("name"), Functions.lit("non-exist-key"))),
        expected,
        false);
  }

  @Test
  public void as_array() {
    DataFrame df =
        getSession()
            .sql(
                "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
                    + "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)");
    Row[] expected = {Row.create("[\n  1,\n  2,\n  3\n]"), Row.create("[\n  6,\n  7,\n  8\n]")};
    checkAnswer(df.select(Functions.as_array(df.col("arr1"))), expected, false);
  }

  @Test
  public void as_binary() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(new byte[] {115, 110, 111, 119}, null, null)};
    checkAnswer(
        df.select(
            Functions.as_binary(df.col("bin1")),
            Functions.as_binary(df.col("bool1")),
            Functions.as_binary(df.col("str1"))),
        expected,
        false);
  }

  @Test
  public void as_char() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create("X", null, null)};
    checkAnswer(
        df.select(
            Functions.as_char(df.col("str1")),
            Functions.as_char(df.col("bin1")),
            Functions.as_char(df.col("bool1"))),
        expected,
        false);
    checkAnswer(
        df.select(
            Functions.as_varchar(df.col("str1")),
            Functions.as_varchar(df.col("bin1")),
            Functions.as_varchar(df.col("bool1"))),
        expected,
        false);
  }

  @Test
  public void as_date() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(new Date(117, 1, 24), null, null)};
    checkAnswer(
        df.select(
            Functions.as_date(df.col("date1")),
            Functions.as_date(df.col("time1")),
            Functions.as_date(df.col("bool1"))),
        expected);
  }

  @Test
  public void as_number() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");

    assert df.select(Functions.as_decimal(df.col("decimal1"))).collect()[0].getLong(0) == 1;
    assert df.select(Functions.as_decimal(df.col("decimal1"), 6)).collect()[0].getLong(0) == 1;
    assert df.select(Functions.as_decimal(df.col("decimal1"), 6, 3))
            .collect()[0]
            .getDecimal(0)
            .doubleValue()
        == 1.23;

    assert df.select(Functions.as_number(df.col("decimal1"))).collect()[0].getLong(0) == 1;
    assert df.select(Functions.as_number(df.col("decimal1"), 6)).collect()[0].getLong(0) == 1;
    assert df.select(Functions.as_number(df.col("decimal1"), 6, 3))
            .collect()[0]
            .getDecimal(0)
            .doubleValue()
        == 1.23;
  }

  @Test
  public void as_real() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(1.23, 3.21, 15.0, null)};
    checkAnswer(
        df.select(
            Functions.as_double(df.col("decimal1")),
            Functions.as_double(df.col("double1")),
            Functions.as_double(df.col("num1")),
            Functions.as_double(df.col("bool1"))),
        expected);

    checkAnswer(
        df.select(
            Functions.as_real(df.col("decimal1")),
            Functions.as_real(df.col("double1")),
            Functions.as_real(df.col("num1")),
            Functions.as_real(df.col("bool1"))),
        expected);
  }

  @Test
  public void as_integer() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(1, null, 15, null)};
    checkAnswer(
        df.select(
            Functions.as_integer(df.col("decimal1")),
            Functions.as_integer(df.col("double1")),
            Functions.as_integer(df.col("num1")),
            Functions.as_integer(df.col("bool1"))),
        expected);
  }

  @Test
  public void as_object() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create("{\n  \"Tree\": \"Pine\"\n}", null, null)};
    checkAnswer(
        df.select(
            Functions.as_object(df.col("obj1")),
            Functions.as_object(df.col("arr1")),
            Functions.as_object(df.col("str1"))),
        expected);
  }

  @Test
  public void as_time() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(Time.valueOf("20:57:01"), null, null)};
    checkAnswer(
        df.select(
            Functions.as_time(df.col("time1")),
            Functions.as_time(df.col("date1")),
            Functions.as_time(df.col("timestamp_tz1"))),
        expected);
  }

  @Test
  public void as_timestamp() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    Row[] expected = {Row.create(Timestamp.valueOf("2017-02-24 12:00:00.456"), null, null)};
    checkAnswer(
        df.select(
            Functions.as_timestamp_ntz(df.col("timestamp_ntz1")),
            Functions.as_timestamp_ntz(df.col("timestamp_tz1")),
            Functions.as_timestamp_ntz(df.col("timestamp_ltz1"))),
        expected);

    Row[] expected1 = {Row.create(null, null, Timestamp.valueOf("2017-02-24 04:00:00.123"))};
    checkAnswer(
        df.select(
            Functions.as_timestamp_ltz(df.col("timestamp_ntz1")),
            Functions.as_timestamp_ltz(df.col("timestamp_tz1")),
            Functions.as_timestamp_ltz(df.col("timestamp_ltz1"))),
        expected1);

    Row[] expected2 = {Row.create(null, Timestamp.valueOf("2017-02-24 13:00:00.123"), null)};
    checkAnswer(
        df.select(
            Functions.as_timestamp_tz(df.col("timestamp_ntz1")),
            Functions.as_timestamp_tz(df.col("timestamp_tz1")),
            Functions.as_timestamp_tz(df.col("timestamp_ltz1"))),
        expected2);
  }

  @Test
  public void strtok_to_array() {
    DataFrame df =
        getSession().sql("select * from values('1,2,3,4,5', ','),('1 2 3 4 5', ' ') as T(a, b)");
    Row[] expected = {
      Row.create("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]"),
      Row.create("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]")
    };
    checkAnswer(df.select(Functions.strtok_to_array(df.col("a"), df.col("b"))), expected, false);

    Row[] expected1 = {
      Row.create("[\n  \"1,2,3,4,5\"\n]"),
      Row.create("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]")
    };
    checkAnswer(df.select(Functions.strtok_to_array(df.col("a"))), expected1, false);
  }

  @Test
  public void to_array() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as T(a)");
    Row[] expected = {Row.create("[\n  1\n]"), Row.create("[\n  2\n]"), Row.create("[\n  3\n]")};
    checkAnswer(df.select(Functions.to_array(df.col("a"))), expected, false);
  }

  @Test
  public void to_json() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as T(a)");
    Row[] expected = {Row.create("1"), Row.create("2"), Row.create("3")};
    checkAnswer(df.select(Functions.to_json(df.col("a"))), expected, false);
  }

  @Test
  public void to_object() {
    DataFrame df =
        getSession()
            .sql(
                "select to_variant(to_array('Example')) as arr1,"
                    + " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, "
                    + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
                    + " to_variant(true) as bool1,"
                    + " to_variant('X') as str1, "
                    + " to_variant(to_date('2017-02-24')) as date1, "
                    + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
                    + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
                    + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
                    + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
                    + " to_variant(1.23::decimal(6, 3)) as decimal1, "
                    + " to_variant(3.21::double) as double1, "
                    + " to_variant(15) as num1 ");
    checkAnswer(
        df.select(Functions.to_object(df.col("obj1"))),
        new Row[] {Row.create("{\n  \"Tree\": \"Pine\"\n}")});
  }

  @Test
  public void to_variant() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as T(a)");
    Row[] expected = {Row.create("1"), Row.create("2"), Row.create("3")};
    checkAnswer(df.select(Functions.to_variant(df.col("a"))), expected, false);
  }

  @Test
  public void to_xml() {
    DataFrame df = getSession().sql("select * from values(1),(2),(3) as T(a)");
    Row[] expected = {
      Row.create("<SnowflakeData type=\"INTEGER\">1</SnowflakeData>"),
      Row.create("<SnowflakeData type=\"INTEGER\">2</SnowflakeData>"),
      Row.create("<SnowflakeData type=\"INTEGER\">3</SnowflakeData>")
    };
    checkAnswer(df.select(Functions.to_xml(df.col("a"))), expected, false);
  }

  @Test
  public void get() {
    DataFrame df =
        getSession()
            .sql(
                "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from values('age', 21, 'zip', "
                    + "21021, 'name', 'Joe', 'age', 0, true),('age', 26, 'zip', 94021, 'name', 'Jay', 'key', "
                    + "0, false) as T(a,b,c,d,e,f,k,v,flag)");
    checkAnswer(
        df.select(Functions.get(df.col("obj"), df.col("k"))),
        new Row[] {Row.create("21"), Row.create((Object) null)},
        false);
  }

  @Test
  public void get_ignore_case() {
    DataFrame df =
        getSession()
            .sql(
                "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from values('age', 21, 'zip', "
                    + "21021, 'name', 'Joe', 'age', 0, true),('age', 26, 'zip', 94021, 'name', 'Jay', 'key', "
                    + "0, false) as T(a,b,c,d,e,f,k,v,flag)");
    checkAnswer(
        df.select(Functions.get_ignore_case(df.col("obj"), Functions.lit("AGE"))),
        new Row[] {Row.create("21"), Row.create("26")},
        false);
  }

  @Test
  public void object_keys() {
    DataFrame df =
        getSession()
            .sql(
                "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from values('age', 21, 'zip', "
                    + "21021, 'name', 'Joe', 'age', 0, true),('age', 26, 'zip', 94021, 'name', 'Jay', 'key', "
                    + "0, false) as T(a,b,c,d,e,f,k,v,flag)");
    checkAnswer(
        df.select(Functions.object_keys(df.col("obj"))),
        new Row[] {
          Row.create("[\n  \"age\",\n  \"name\",\n  \"zip\"\n]"),
          Row.create("[\n  \"age\",\n  \"name\",\n  \"zip\"\n]")
        },
        false);
  }

  @Test
  public void xmlget() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_xml(a) as v, b as t2, c as t3, d as instance from values"
                    + "('<t1>foo<t2>bar</t2><t3></t3></t1>','t2','t3',0),('<t1></t1>','t2','t3',0),"
                    + "('<t1><t2>foo</t2><t2>bar</t2></t1>','t2','t3',1) as T(a,b,c,d)");
    checkAnswer(
        df.select(Functions.get(Functions.xmlget(df.col("v"), df.col("t2")), Functions.lit("$"))),
        new Row[] {Row.create("\"bar\""), Row.create((Object) null), Row.create("\"foo\"")},
        false);

    checkAnswer(
        df.select(
            Functions.get(
                Functions.xmlget(df.col("v"), df.col("t3"), Functions.lit("0")),
                Functions.lit("@"))),
        new Row[] {Row.create("\"t3\""), Row.create((Object) null), Row.create((Object) null)},
        false);
  }

  @Test
  public void get_path() {
    DataFrame df =
        getSession()
            .sql(
                "select parse_json(column1) as v, column2 as k from values ('{\"a\": null}','a'), "
                    + "('{\"a\": \"foo\"}','a'), ('{\"a\": \"foo\"}','b'), (null,'a')");
    checkAnswer(
        df.select(Functions.get_path(df.col("v"), df.col("k"))),
        new Row[] {
          Row.create("null"),
          Row.create("\"foo\""),
          Row.create((Object) null),
          Row.create((Object) null)
        },
        false);
  }

  @Test
  public void iff() {
    DataFrame df =
        getSession()
            .sql(
                "select * from values(true, 2, 2, 4), (false, 12, 12, 14), (true, 22, 23, 24) as t(a, b, c, d)");
    checkAnswer(
        df.select(
            df.col("a"),
            df.col("b"),
            df.col("d"),
            Functions.iff(df.col("a"), df.col("b"), df.col("d"))),
        new Row[] {
          Row.create(true, 2, 4, 2), Row.create(false, 12, 14, 14), Row.create(true, 22, 24, 22)
        },
        false);
  }

  @Test
  public void seq() {
    Row[] expected = {Row.create(0), Row.create(1), Row.create(2), Row.create(3), Row.create(4)};
    checkAnswer(getSession().generator(5, new Column[] {Functions.seq1()}), expected);
    checkAnswer(getSession().generator(5, new Column[] {Functions.seq2()}), expected);
    checkAnswer(getSession().generator(5, new Column[] {Functions.seq4()}), expected);
    checkAnswer(getSession().generator(5, new Column[] {Functions.seq8()}), expected);

    checkAnswer(getSession().generator(5, Functions.seq1(false)), expected);
    checkAnswer(getSession().generator(5, Functions.seq2(false)), expected);
    checkAnswer(getSession().generator(5, Functions.seq4(false)), expected);
    checkAnswer(getSession().generator(5, Functions.seq8(false)), expected);
  }

  @Test
  public void uniform() {
    Row[] result =
        getSession()
            .generator(5, Functions.uniform(Functions.lit(1), Functions.lit(5), Functions.random()))
            .collect();
    assert result.length == 5;
    for (int i = 0; i < 5; i++) {
      assert result[i].size() == 1;
      assert result[i].getInt(0) >= 1 && result[i].getInt(0) <= 5;
    }
  }

  @Test
  public void listagg() {
    DataFrame df = getSession().sql("select * from values(1,1),(2,1),(1,1),(3,2) as t(a,b)");
    checkAnswer(
        df.groupBy(df.col("b")).agg(Functions.listagg(df.col("a")).withinGroup(df.col("a").asc())),
        new Row[] {Row.create(1, "112"), Row.create(2, "3")});

    checkAnswer(
        df.groupBy(df.col("b"))
            .agg(Functions.listagg(df.col("a"), ",").withinGroup(df.col("a").asc())),
        new Row[] {Row.create(1, "1,1,2"), Row.create(2, "3")});

    checkAnswer(
        df.groupBy(df.col("b"))
            .agg(Functions.listagg(df.col("a"), ",", true).withinGroup(df.col("a").asc())),
        new Row[] {Row.create(1, "1,2"), Row.create(2, "3")});
  }

  @Test
  public void any_value() {
    DataFrame df = getSession().sql("select * from values (1),(2),(3) as t(a)");
    Row[] result = df.select(Functions.any_value(df.col("a"))).collect();
    assert result.length == 1;
    assert result[0].getInt(0) == 1 || result[0].getInt(0) == 2 || result[0].getInt(0) == 3;
  }

  @Test
  public void test_asc() {
    DataFrame df = getSession().sql("select * from values(3),(1),(2) as t(a)");
    Row[] expected = {Row.create(1), Row.create(2), Row.create(3)};

    checkAnswer(df.sort(Functions.asc("a")), expected, false);
  }

  @Test
  public void test_desc() {
    DataFrame df = getSession().sql("select * from values(2),(1),(3) as t(a)");
    Row[] expected = {Row.create(3), Row.create(2), Row.create(1)};

    checkAnswer(df.sort(Functions.desc("a")), expected, false);
  }

  @Test
  public void test_size() {
    DataFrame df = getSession()
            .sql(
                    "select array_construct(a,b,c) as arr from values(1,2,3) as T(a,b,c)");
    Row[] expected = {Row.create(3)};

    checkAnswer(df.select(Functions.size(Functions.col("arr"))), expected, false);
  }

  @Test
  public void test_expr() {
    DataFrame df = getSession().sql("select a from values(1), (2), (3) as T(a)");
    Row[] expected = {Row.create(3)};
    checkAnswer(df.filter(Functions.expr("a > 2")), expected, false);
  }

}
