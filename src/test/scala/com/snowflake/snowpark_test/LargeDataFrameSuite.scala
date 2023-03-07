package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.{JavaStoredProcExclude, Row, SampleDataTest, TestData, UnstableTest}

import java.sql.{Date, Time, Timestamp}
import com.snowflake.snowpark.internal.Utils.randomGenerator

import scala.collection.mutable.ArrayBuffer

// contains costly tests
class LargeDataFrameSuite extends TestData {
  import session.implicits._

  test("toLocalIterator should not load all data at once", JavaStoredProcExclude, UnstableTest) {
    val df = session
      .range(1000000)
      .select(
        col("id").as("a"),
        (col("id") + 1).as("b"),
        (col("id") + 2).as("c"),
        (col("id") + 3).as("d"),
        (col("id") + 4).as("e"),
        (col("id") + 5).as("f"),
        (col("id") + 6).as("g"),
        (col("id") + 7).as("h"),
        (col("id") + 8).as("i"),
        (col("id") + 9).as("j"))
      .cacheResult()
    val t1 = System.currentTimeMillis()
    df.collect()
    val t2 = System.currentTimeMillis()
    val it = df.toLocalIterator
    val t3 = System.currentTimeMillis()
    it.next()
    val t4 = System.currentTimeMillis()

    val loadArrayTime = t2 - t1
    val loadIteratorTime = t3 - t2
    // in memory
    val loadFistValueTime = t4 - t3

    // may be not stable
    assert(loadArrayTime > (1.25 * loadIteratorTime).toLong)
    assert(loadIteratorTime > 10 * loadFistValueTime)
  }

  test("limit on order by", SampleDataTest) {
    val a = session
      .table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")
      .select("L_RETURNFLAG", "L_SHIPMODE")
      .filter(col("L_RETURNFLAG") === "A")
      .groupBy("L_RETURNFLAG", "L_SHIPMODE")
      .count()
    val n = session
      .table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")
      .select("L_RETURNFLAG", "L_SHIPMODE")
      .filter(col("L_RETURNFLAG") === "N")
      .groupBy("L_RETURNFLAG", "L_SHIPMODE")
      .count()
    val union =
      a.unionAll(n)
    val result = union
      .select(col("COUNT"))
      .sort(col("COUNT"))
      .limit(10)
      .collect()

    (0 until (result.length - 1)).foreach(index =>
      assert(result(index).getInt(0) < result(index + 1).getInt(0)))
  }

  test("createDataFrame for large values: basic types") {
    val schema = StructType(
      Seq(
        StructField("ID", LongType),
        StructField("string", StringType),
        StructField("byte", ByteType),
        StructField("short", ShortType),
        StructField("int", IntegerType),
        StructField("long", LongType),
        StructField("float", FloatType),
        StructField("double", DoubleType),
        StructField("decimal", DecimalType(10, 3)),
        StructField("boolean", BooleanType),
        StructField("binary", BinaryType),
        StructField("timestamp", TimestampType),
        StructField("date", DateType)))

    val schemaString = """root
                         | |--ID: Long (nullable = true)
                         | |--STRING: String (nullable = true)
                         | |--BYTE: Long (nullable = true)
                         | |--SHORT: Long (nullable = true)
                         | |--INT: Long (nullable = true)
                         | |--LONG: Long (nullable = true)
                         | |--FLOAT: Double (nullable = true)
                         | |--DOUBLE: Double (nullable = true)
                         | |--DECIMAL: Decimal(10, 3) (nullable = true)
                         | |--BOOLEAN: Boolean (nullable = true)
                         | |--BINARY: Binary (nullable = true)
                         | |--TIMESTAMP: Timestamp (nullable = true)
                         | |--DATE: Date (nullable = true)
                         |""".stripMargin

    val timestamp: Long = 1606179541282L

    val largeData = new ArrayBuffer[Row]()
    for (i <- 0 to 1024) {
      largeData.append(
        Row(
          i.toLong,
          "a",
          1.toByte,
          2.toShort,
          3,
          4L,
          1.1F,
          1.2D,
          new java.math.BigDecimal(1.2),
          true,
          Array(1.toByte, 2.toByte),
          new Timestamp(timestamp - 100),
          new Date(timestamp - 100)))
    }
    // Add one null values
    largeData.append(
      Row(1025, null, null, null, null, null, null, null, null, null, null, null, null))

    val result = session.createDataFrame(largeData, schema)
    // byte, short, int, long are converted to long
    // float and double are converted to double
    result.schema.printTreeString()
    assert(getSchemaString(result.schema) == schemaString)
    checkAnswer(result.sort(col("id")), largeData, false)
  }

  test("createDataFrame for large values: time") {
    val schema = StructType(Seq(StructField("id", LongType), StructField("time", TimeType)))

    val rowCount = 550
    val largeData = new ArrayBuffer[Row]()
    for (i <- 0 until rowCount) {
      largeData.append(Row(i.toLong, Time.valueOf("11:12:13")))
    }
    largeData.append(Row(rowCount, null))

    val df = session.createDataFrame(largeData, schema)
    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ID: Long (nullable = true)
          | |--TIME: Time (nullable = true)
          |""".stripMargin)

    val expected = new ArrayBuffer[Row]()
    val snowflakeTime = session.sql("select '11:12:13' :: Time").collect()(0).getTime(0)
    for (i <- 0 until rowCount) {
      expected.append(Row(i.toLong, snowflakeTime))
    }
    expected.append(Row(rowCount, null))
    checkAnswer(df.sort(col("id")), expected, sort = false)
  }

  // In the result, Array, Map and Geography are String data
  test("createDataFrame for large values: Array, Map, Variant") {
    val schema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("array", ArrayType(null)),
        StructField("map", MapType(null, null)),
        StructField("variant", VariantType),
        StructField("geography", GeographyType)))

    val rowCount = 350
    val largeData = new ArrayBuffer[Row]()
    for (i <- 0 until rowCount) {
      largeData.append(
        Row(
          i.toLong,
          Array("'", 2),
          Map("'" -> 1),
          new Variant(1),
          Geography.fromGeoJSON("POINT(30 10)")))
    }
    largeData.append(Row(rowCount, null, null, null, null, null))

    val df = session.createDataFrame(largeData, schema)
    assert(
      getSchemaString(df.schema) ==
        """root
          | |--ID: Long (nullable = true)
          | |--ARRAY: Array (nullable = true)
          | |--MAP: Map (nullable = true)
          | |--VARIANT: Variant (nullable = true)
          | |--GEOGRAPHY: Geography (nullable = true)
          |""".stripMargin)

    val expected = new ArrayBuffer[Row]()
    for (i <- 0 until rowCount) {
      expected.append(
        Row(
          i.toLong,
          "[\n  \"'\",\n  2\n]",
          "{\n  \"'\": 1\n}",
          "1",
          Geography.fromGeoJSON("""{
                                  |  "coordinates": [
                                  |    30,
                                  |    10
                                  |  ],
                                  |  "type": "Point"
                                  |}""".stripMargin)))
    }
    expected.append(Row(rowCount, null, null, null, null))
    checkAnswer(df.sort(col("id")), expected, sort = false)
  }

  test("createDataFrame for large values: variant in array and map") {
    val schema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("array", ArrayType(null)),
        StructField("map", MapType(null, null))))
    val largeData = new ArrayBuffer[Row]()
    val rowCount = 350
    for (i <- 0 until rowCount) {
      largeData.append(
        Row(i.toLong, Array(new Variant(1), new Variant("\"'")), Map("a" -> new Variant("\"'"))))
    }
    largeData.append(Row(rowCount, null, null))
    val df = session.createDataFrame(largeData, schema)
    val expected = new ArrayBuffer[Row]()
    for (i <- 0 until rowCount) {
      expected.append(Row(i.toLong, "[\n  1,\n  \"\\\"'\"\n]", "{\n  \"a\": \"\\\"'\"\n}"))
    }
    expected.append(Row(rowCount, null, null))
    checkAnswer(df.sort(col("id")), expected, sort = false)
  }

  test("createDataFrame for large values: geography in array and map") {
    val schema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("array", ArrayType(null)),
        StructField("map", MapType(null, null))))
    val largeData = new ArrayBuffer[Row]()
    val rowCount = 350
    for (i <- 0 until rowCount) {
      largeData.append(
        Row(
          i.toLong,
          Array(
            Geography.fromGeoJSON("point(30 10)"),
            Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}")),
          Map(
            "a" -> Geography.fromGeoJSON("point(30 10)"),
            "b" -> Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[300,100]}"))))
    }
    largeData.append(Row(rowCount, null, null))
    val df = session.createDataFrame(largeData, schema)
    val expected = new ArrayBuffer[Row]()
    for (i <- 0 until rowCount) {
      expected.append(
        Row(
          i.toLong,
          "[\n  \"point(30 10)\",\n  {\n    \"coordinates\": [\n" +
            "      30,\n      10\n    ],\n    \"type\": \"Point\"\n  }\n]",
          "{\n  \"a\": \"point(30 10)\",\n  \"b\": {\n    \"coordinates\": [\n" +
            "      300,\n      100\n    ],\n    \"type\": \"Point\"\n  }\n}"))
    }
    expected.append(Row(rowCount, null, null))
    checkAnswer(df.sort(col("id")), expected, sort = false)
  }

  test("test large ResultSet with multiple chunks") {
    val c = lit(randomGenerator.alphanumeric.take(1024).mkString)
    // The row size is about 10K,
    // The result set is about 100M. It's sure there are multiple result chunks.
    val df = session
      .range(10000)
      .select(
        col("id"),
        c.as("c0"),
        c.as("c1"),
        c.as("c2"),
        c.as("c3"),
        c.as("c4"),
        c.as("c5"),
        c.as("c6"),
        c.as("c7"),
        c.as("c8"),
        c.as("c9"))
    val rows = df.collect()
    assert(rows.length == 10000)
    assert(rows.last.getLong(0) == 9999)
  }

}
