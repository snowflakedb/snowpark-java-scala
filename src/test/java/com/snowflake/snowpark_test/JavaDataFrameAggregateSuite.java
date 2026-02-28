package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.GroupingSets;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class JavaDataFrameAggregateSuite extends TestBase {

  @Test
  public void mean() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", 1, 11, "b"),
                  Row.create("b", 2, 22, "c"),
                  Row.create("a", 3, 33, "d"),
                  Row.create("b", 4, 44, "e")
                },
                StructType.create(
                    new StructField("key", DataTypes.StringType),
                    new StructField("value1", DataTypes.IntegerType),
                    new StructField("value2", DataTypes.IntegerType),
                    new StructField("rest", DataTypes.StringType)));

    Row[] expected = {Row.create("a", 2.0, 22.0), Row.create("b", 3.0, 33.0)};
    checkAnswer(df.groupBy("key").avg(df.col("value1"), df.col("value2")), expected);
    checkAnswer(df.groupBy("key").mean(df.col("value1"), df.col("value2")), expected);
  }

  @Test
  public void sum() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("dotNET", 2012, 10000.0),
                  Row.create("Java", 2012, 20000.0),
                  Row.create("dotNET", 2012, 5000.0),
                  Row.create("dotNET", 2013, 48000.0),
                  Row.create("Java", 2013, 30000.0)
                },
                StructType.create(
                    new StructField("course", DataTypes.StringType),
                    new StructField("year", DataTypes.IntegerType),
                    new StructField("earnings", DataTypes.DoubleType)));
    Row[] expected = {
      Row.create("Java", 2012, 20000.0),
      Row.create("Java", 2013, 30000.0),
      Row.create("Java", null, 50000.0),
      Row.create("dotNET", 2012, 15000.0),
      Row.create("dotNET", 2013, 48000.0),
      Row.create("dotNET", null, 63000.0),
      Row.create(null, null, 113000.0)
    };
    checkAnswer(df.rollup("course", "year").sum(df.col("earnings")), expected);
    checkAnswer(df.rollup(df.col("course"), df.col("year")).sum(df.col("earnings")), expected);
  }

  @Test
  public void median() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", 1, 11, "b"),
                  Row.create("b", 2, 22, "c"),
                  Row.create("a", 3, 33, "d"),
                  Row.create("b", 4, 44, "e"),
                  Row.create("b", 44, 444, "f")
                },
                StructType.create(
                    new StructField("key", DataTypes.StringType),
                    new StructField("value1", DataTypes.IntegerType),
                    new StructField("value2", DataTypes.IntegerType),
                    new StructField("rest", DataTypes.StringType)));
    checkAnswer(
        df.groupBy("key").median(df.col("value1"), df.col("value2")),
        new Row[] {Row.create("a", 2.0, 22.0), Row.create("b", 4.0, 44.0)});
  }

  @Test
  public void min() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", 1, 0, "b"), Row.create("b", 2, 4, "c"), Row.create("a", 2, 3, "d")
                },
                StructType.create(
                    new StructField("key", DataTypes.StringType),
                    new StructField("value1", DataTypes.IntegerType),
                    new StructField("value2", DataTypes.IntegerType),
                    new StructField("rest", DataTypes.StringType)));
    checkAnswer(
        df.groupBy("key").min(df.col("value2")),
        new Row[] {Row.create("a", 0), Row.create("b", 4)});
  }

  @Test
  public void max() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", 1, 11, "b"),
                  Row.create("b", 2, 22, "c"),
                  Row.create("a", 3, 33, "d"),
                  Row.create("b", 4, 44, "e")
                },
                StructType.create(
                    new StructField("key", DataTypes.StringType),
                    new StructField("value1", DataTypes.IntegerType),
                    new StructField("value2", DataTypes.IntegerType),
                    new StructField("rest", DataTypes.StringType)));
    checkAnswer(
        df.groupBy("key").max(df.col("value1"), df.col("value2")),
        new Row[] {Row.create("a", 3, 33), Row.create("b", 4, 44)});
  }

  @Test
  public void any_value() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", 1, 11, "b"),
                  Row.create("b", 2, 22, "c"),
                  Row.create("a", 3, 33, "d"),
                  Row.create("b", 4, 44, "e")
                },
                StructType.create(
                    new StructField("key", DataTypes.StringType),
                    new StructField("value1", DataTypes.IntegerType),
                    new StructField("value2", DataTypes.IntegerType),
                    new StructField("rest", DataTypes.StringType)));
    Row[] results = df.groupBy("key").any_value(df.col("value1"), df.col("value2")).collect();
    assert results.length == 2;
    for (Row result : results) {
      if (result.getString(0).equals("a")) {
        assert (result.getInt(1) == 1 || result.getInt(1) == 3)
            && (result.getInt(2) == 11 || result.getInt(2) == 33);
      } else {
        assert (result.getInt(1) == 2 || result.getInt(1) == 4)
            && (result.getInt(2) == 22 || result.getInt(2) == 44);
      }
    }
  }

  @Test
  public void count() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", 1), Row.create("b", 2), Row.create("c", 1), Row.create("d", 5)
                },
                StructType.create(
                    new StructField("id", DataTypes.StringType),
                    new StructField("value", DataTypes.IntegerType)));
    DataFrame limit2DF = df.limit(2);
    checkAnswer(
        limit2DF.groupBy("id").count().select(limit2DF.col("id")),
        limit2DF.select(limit2DF.col("id")));
  }

  @Test
  public void offset() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create("a", 1), Row.create("b", 2), Row.create("c", 1), Row.create("d", 5)
                },
                StructType.create(
                    new StructField("id", DataTypes.StringType),
                    new StructField("value", DataTypes.IntegerType)));
    DataFrame offset2DF = df.offset(2);
    checkAnswer(
        offset2DF.groupBy("id").count().select(offset2DF.col("id")),
        offset2DF.select(offset2DF.col("id")));
  }

  @Test
  public void builtin() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {Row.create(1, 11), Row.create(2, 12), Row.create(1, 13)},
                StructType.create(
                    new StructField("a", DataTypes.IntegerType),
                    new StructField("b", DataTypes.IntegerType)));
    checkAnswer(
        df.groupBy("a").builtin("max", df.col("a"), df.col("b")),
        new Row[] {Row.create(1, 1, 13), Row.create(2, 2, 12)});
  }

  @Test
  public void groupByGroupingSet() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {
                  Row.create(201, "Thomas Leonard Vicente", "LVN", "Technician"),
                  Row.create(202, "Tamara Lolita VanZant", "LVN", "Technician"),
                  Row.create(341, "Georgeann Linda Vente", "LVN", "General"),
                  Row.create(471, "Andrea Renee Nouveau", "RN", "Amateur Extra"),
                  Row.create(101, "Lily Vine", "LVN", null),
                  Row.create(102, "Larry Vancouver", "LVN", null),
                  Row.create(172, "Rhonda Nova", "RN", null)
                },
                StructType.create(
                    new StructField("id", DataTypes.IntegerType),
                    new StructField("full_name", DataTypes.StringType),
                    new StructField("medical_license", DataTypes.StringType),
                    new StructField("radio_license", DataTypes.StringType)));

    Row[] result =
        df.groupBy("medical_license")
            .agg(Functions.count(Functions.col("*")).as("count"))
            .withColumn("radio_license", Functions.lit(null))
            .select("medical_license", "radio_license", "count")
            .unionAll(
                df.groupBy("radio_license")
                    .agg(Functions.count(Functions.col("*")).as("count"))
                    .withColumn("medical_license", Functions.lit(null))
                    .select("medical_license", "radio_license", "count"))
            .sort(Functions.col("count"))
            .collect();

    Set<com.snowflake.snowpark_java.Column> set1 = new HashSet<>();
    set1.add(df.col("medical_license"));
    Set<com.snowflake.snowpark_java.Column> set2 = new HashSet<>();
    set2.add(df.col("radio_license"));
    DataFrame groupingSets =
        df.groupByGroupingSets(GroupingSets.create(set1, set2))
            .agg(Functions.count(Functions.col("*")).as("count"))
            .sort(Functions.col("count"));

    checkAnswer(groupingSets, result);
  }

  @Test
  public void pivot() {
    StructType schema =
        StructType.create(
            new StructField("empid", DataTypes.IntegerType),
            new StructField("amount", DataTypes.IntegerType),
            new StructField("month", DataTypes.StringType));
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
    DataFrame df = getSession().createDataFrame(data, schema);
    Row[] expected = {
      Row.create(1, 10400, 8000, 11000, 18000), Row.create(2, 39500, 90700, 12000, 5300)
    };

    checkAnswer(
        df.pivot("month", new String[] {"JAN", "FEB", "MAR", "APR"})
            .agg(Functions.sum(df.col("amount")))
            .sort(df.col("empid")),
        expected);

    checkAnswer(
        df.pivot(df.col("month"), new String[] {"JAN", "FEB", "MAR", "APR"})
            .agg(Functions.sum(df.col("amount")))
            .sort(df.col("empid")),
        expected);
  }
}
