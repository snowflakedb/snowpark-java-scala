package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.udf.JavaUDF1;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import org.junit.Test;

public class JavaUDFSuite extends UDFTestBase {
  public JavaUDFSuite() {}

  private boolean dependencyAdded = false;

  @Override
  public Session getSession() {
    Session session = super.getSession();
    if (!dependencyAdded) {
      dependencyAdded = true;
      addDepsToClassPath(session);
    }
    return session;
  }

  @Test
  public void udfOfVariant() {
    DataFrame df = getSession().sql("select to_variant(a) as var from values(1) as t(a)");
    UserDefinedFunction udf =
        Functions.udf((Variant var) -> var, DataTypes.VariantType, DataTypes.VariantType);
    checkAnswer(df.select(udf.apply(df.col("var"))), new Row[] {Row.create("1")});

    // variant array
    DataFrame df1 =
        getSession()
            .sql(
                "select array_construct(to_variant(a), to_variant(b)) as arr from values(1, 2) as"
                    + " t(a, b)");
    UserDefinedFunction udf1 =
        Functions.udf(
            (Variant[] arr) -> new Variant[] {arr[1], arr[0]},
            DataTypes.createArrayType(DataTypes.VariantType),
            DataTypes.createArrayType(DataTypes.VariantType));
    checkAnswer(
        df1.select(udf1.apply(df1.col("arr"))), new Row[] {Row.create("[\n  \"2\",\n  \"1\"\n]")});

    // variant map
    DataFrame df2 =
        getSession()
            .sql(
                "select object_construct('a', to_variant(a), 'b', to_variant(b)) as map from"
                    + " values(1, 2) as t(a, b)");
    UserDefinedFunction udf2 =
        Functions.udf(
            (Map<String, Variant> map) -> map,
            DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType),
            DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType));
    checkAnswer(
        df2.select(udf2.apply(df2.col("map"))),
        new Row[] {Row.create("{\n  \"a\": \"1\",\n  \"b\": \"2\"\n}")});
  }

  @Test
  public void udfOfGeography() {
    Geography testData =
        Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}");
    StructType schema = StructType.create(new StructField("geo", DataTypes.GeographyType));
    DataFrame df = getSession().createDataFrame(new Row[] {Row.create(testData)}, schema);
    UserDefinedFunction udf =
        Functions.udf((Geography geo) -> geo, DataTypes.GeographyType, DataTypes.GeographyType);

    Geography expected =
        Geography.fromGeoJSON(
            "{\n  \"coordinates\": [\n    125.6,\n    10.1\n  ],\n  \"type\": \"Point\"\n}");
    checkAnswer(df.select(udf.apply(df.col("geo"))), new Row[] {Row.create(expected)});

    // snowflake doesn't support array/map of geography
  }

  @Test
  public void udfOfNumber() {
    Row[] data = {Row.create((byte) 1, (short) 2, 3, 4L, 5.5f, 6.6, 7.7)};
    StructType schema =
        StructType.create(
            new StructField("byte", DataTypes.ByteType),
            new StructField("short", DataTypes.ShortType),
            new StructField("int", DataTypes.IntegerType),
            new StructField("long", DataTypes.LongType),
            new StructField("float", DataTypes.FloatType),
            new StructField("double", DataTypes.DoubleType));
    DataFrame df = getSession().createDataFrame(data, schema);
    // doesn't support byte

    UserDefinedFunction udf1 =
        Functions.udf((Short num) -> num, DataTypes.ShortType, DataTypes.ShortType);
    checkAnswer(df.select(udf1.apply(df.col("short"))), new Row[] {Row.create(2)});

    UserDefinedFunction udf2 =
        Functions.udf((Integer num) -> num, DataTypes.IntegerType, DataTypes.IntegerType);
    checkAnswer(df.select(udf2.apply(df.col("int"))), new Row[] {Row.create(3)});

    UserDefinedFunction udf3 =
        Functions.udf((Long num) -> num, DataTypes.LongType, DataTypes.LongType);
    checkAnswer(df.select(udf3.apply(df.col("long"))), new Row[] {Row.create(4)});

    UserDefinedFunction udf4 =
        Functions.udf((Float num) -> num, DataTypes.FloatType, DataTypes.FloatType);
    checkAnswer(df.select(udf4.apply(df.col("float"))), new Row[] {Row.create(5.5)});

    UserDefinedFunction udf5 =
        Functions.udf((Double num) -> num, DataTypes.DoubleType, DataTypes.DoubleType);
    checkAnswer(df.select(udf5.apply(df.col("double"))), new Row[] {Row.create(6.6)});
  }

  @Test
  public void udfOfDate() {
    withTimeZoneTest(
        () -> {
          Row[] data = {
            Row.create(
                Date.valueOf("2021-12-07"),
                Timestamp.valueOf("2019-01-01 00:00:00"),
                Time.valueOf("01:02:03"))
          };
          StructType schema =
              StructType.create(
                  new StructField("date", DataTypes.DateType),
                  new StructField("timestamp", DataTypes.TimestampType),
                  new StructField("time", DataTypes.TimeType));
          DataFrame df = getSession().createDataFrame(data, schema);

          UserDefinedFunction udf1 =
              Functions.udf((Date date) -> date, DataTypes.DateType, DataTypes.DateType);
          checkAnswer(
              df.select(udf1.apply(df.col("date"))),
              new Row[] {Row.create(Date.valueOf("2021-12-07"))});

          UserDefinedFunction udf2 =
              Functions.udf(
                  (Timestamp timestamp) -> timestamp,
                  DataTypes.TimestampType,
                  DataTypes.TimestampType);
          checkAnswer(
              df.select(udf2.apply(df.col("timestamp"))),
              new Row[] {Row.create(Timestamp.valueOf("2019-01-01 00:00:00"))});

          UserDefinedFunction udf3 =
              Functions.udf((Time time) -> time, DataTypes.TimeType, DataTypes.TimeType);
          checkAnswer(
              df.select(udf3.apply(df.col("time"))),
              new Row[] {Row.create(Time.valueOf("01:02:03"))});
        },
        getSession());
  }

  @Test
  public void udfOfBinary() {
    DataFrame df =
        getSession()
            .createDataFrame(
                new Row[] {Row.create("data")},
                StructType.create(new StructField("a", DataTypes.StringType)));
    UserDefinedFunction toBinary =
        Functions.udf(
            (JavaUDF1<String, Object>) String::getBytes,
            DataTypes.StringType,
            DataTypes.BinaryType);
    UserDefinedFunction fromBinary =
        Functions.udf(
            (JavaUDF1<byte[], Object>) String::new, DataTypes.BinaryType, DataTypes.StringType);
    checkAnswer(
        df.select(toBinary.apply(df.col("a")).as("b")).select(fromBinary.apply(Functions.col("b"))),
        new Row[] {Row.create("data")});
  }

  @Test
  public void udfOfOther() {
    Row[] data = {Row.create(true, "abc")};
    StructType schema =
        StructType.create(
            new StructField("boolean", DataTypes.BooleanType),
            new StructField("string", DataTypes.StringType));
    DataFrame df = getSession().createDataFrame(data, schema);
    UserDefinedFunction udf1 =
        Functions.udf((Boolean bool) -> bool, DataTypes.BooleanType, DataTypes.BooleanType);
    checkAnswer(df.select(udf1.apply(df.col("boolean"))), new Row[] {Row.create(true)});

    UserDefinedFunction udf2 =
        Functions.udf((String str) -> str, DataTypes.StringType, DataTypes.StringType);
    checkAnswer(df.select(udf2.apply(df.col("string"))), new Row[] {Row.create("abc")});
  }

  @Test
  public void udfNullValue() {
    DataFrame df =
        getSession().sql("select * from values(1, null), (null, 2), (4, 5) as t(col1, col2)");
    DataType[] input = {DataTypes.IntegerType, DataTypes.IntegerType};
    UserDefinedFunction udf =
        Functions.udf(
            (Integer col1, Integer col2) -> {
              if (col1 == null) {
                return null;
              } else if (col2 == null) {
                return 100;
              } else {
                return col1 + col2;
              }
            },
            input,
            DataTypes.IntegerType);
    Row[] result = {Row.create(100), Row.create((Object) null), Row.create(9)};
    checkAnswer(df.select(udf.apply(df.col("col1"), df.col("col2"))), result);
  }
}
