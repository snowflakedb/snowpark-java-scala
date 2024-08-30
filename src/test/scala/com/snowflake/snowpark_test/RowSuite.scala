package com.snowflake.snowpark_test

import com.snowflake.snowpark.types._
import com.snowflake.snowpark.{Row, SNTestBase, SnowparkClientException}

import java.sql.{Date, Time, Timestamp}
import java.time.{Instant, LocalDate}
import java.util
import java.util.TimeZone

class RowSuite extends SNTestBase {

  test("toSeq, size, and copy") {
    val row = Row(1, null, "str")
    assert(row.toSeq == Array(1, null, "str").toSeq)
    assert(row.size == 3)
    assert(row.length == 3)
    assert(row.copy().toSeq == Array(1, null, "str").toSeq)
  }

  test("equals") {
    val row1 = Row(1, null, "str")
    val row2 = Row(2, null, "str")
    val row3 = Row.fromSeq(Seq(1, null, "str"))
    assert(row1 == row3)
    assert(row1 != row2)
    assert(row2.equals(row2.clone()))
  }

  test("get") {
    val time = System.currentTimeMillis()
    val list = new util.ArrayList[Int](3)
    list.add(1)
    list.add(2)
    list.add(3)

    val map = new util.HashMap[String, String]()
    map.put("a", "b")

    val row = Row(
      null,
      true,
      1.toByte,
      2.toShort,
      3,
      4.toLong,
      5.5.toFloat,
      5.5,
      "str",
      new java.math.BigDecimal(6),
      new Date(time),
      LocalDate.of(2020, 8, 8),
      new Timestamp(time),
      Instant.ofEpochMilli(time),
      Seq(1, 2, 3),
      Map("a" -> "b"),
      Row(1, 2, 3),
      Array[Byte](1, 9),
      Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"),
      Geometry.fromGeoJSON(
        "{\"coordinates\": [3.000000000000000e+01,1.000000000000000e+01],\"type\": \"Point\"}"))

    assert(row.length == 20)
    assert(row.isNullAt(0))
    assert(row.getBoolean(1))
    assert(row.getByte(2) == 1.toByte)
    assert(row.getShort(3) == 2.toShort)
    assert(row.getInt(4) == 3)
    assert(row.getLong(5) == 4L)
    assert(row.getFloat(6) == 5.5f)
    assert(row.getDouble(7) == 5.5)
    assert(row.getString(8) == "str")
    assert(row.getVariant(8) == new Variant("str"))
    assert(row.getDecimal(9) == new java.math.BigDecimal(6))
    assert(row.getDate(10) == new Date(time))
    assert(row.getTimestamp(12) == new Timestamp(time))
    assert(row.getBinary(17) sameElements Array[Byte](1, 9))
    assertThrows[ClassCastException](row.getString(6))
    assertThrows[ClassCastException](row.getBinary(6))
    assert(
      row.getGeography(18) ==
        Geography.fromGeoJSON("{\"type\":\"Point\",\"coordinates\":[30,10]}"))
    assertThrows[ClassCastException](row.getBinary(18))
    assert(row.getString(18) == "{\"type\":\"Point\",\"coordinates\":[30,10]}")
    assert(
      row.getGeometry(19) ==
        Geometry.fromGeoJSON(
          "{\"coordinates\": [3.000000000000000e+01,1.000000000000000e+01],\"type\": \"Point\"}"))
    assert(
      row.getString(19) ==
        "{\"coordinates\": [3.000000000000000e+01,1.000000000000000e+01],\"type\": \"Point\"}")
  }

  test("number getters") {
    val testRow = Row.fromSeq(
      Seq(
        1.toByte,
        Short.MaxValue,
        Short.MinValue,
        Int.MaxValue,
        Int.MinValue,
        Long.MaxValue,
        Long.MinValue,
        Float.MaxValue,
        Float.MinValue,
        Double.MaxValue,
        Double.MinValue,
        "Str"))

    // getByte
    assert(testRow.getByte(0) == 1.toByte)
    var err = intercept[SnowparkClientException](testRow.getByte(1))
    var msg = ".*Cannot cast .* to Byte."
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(2))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(3))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(4))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(5))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(6))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(7))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(8))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(9))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(10))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getByte(11))
    assert(err.message.matches(msg))

    // getShort
    assert(testRow.getShort(0) == 1.toShort)
    assert(testRow.getShort(1) == Short.MaxValue)
    assert(testRow.getShort(2) == Short.MinValue)
    msg = ".*Cannot cast .* to Short."
    err = intercept[SnowparkClientException](testRow.getShort(3))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(4))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(5))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(6))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(7))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(8))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(9))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(10))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getShort(11))
    assert(err.message.matches(msg))

    // getInt
    assert(testRow.getInt(0) == 1)
    assert(testRow.getInt(1) == Short.MaxValue.toInt)
    assert(testRow.getInt(2) == Short.MinValue.toInt)
    assert(testRow.getInt(3) == Int.MaxValue)
    assert(testRow.getInt(4) == Int.MinValue)
    msg = ".*Cannot cast .* to Int."
    err = intercept[SnowparkClientException](testRow.getInt(5))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getInt(6))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getInt(7))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getInt(8))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getInt(9))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getInt(10))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getInt(11))
    assert(err.message.matches(msg))

    // getLong
    assert(testRow.getLong(0) == 1L)
    assert(testRow.getLong(1) == Short.MaxValue.toLong)
    assert(testRow.getLong(2) == Short.MinValue.toLong)
    assert(testRow.getLong(3) == Int.MaxValue.toLong)
    assert(testRow.getLong(4) == Int.MinValue.toLong)
    assert(testRow.getLong(5) == Long.MaxValue)
    assert(testRow.getLong(6) == Long.MinValue)
    msg = ".*Cannot cast .* to Long."
    err = intercept[SnowparkClientException](testRow.getLong(7))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getLong(8))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getLong(9))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getLong(10))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getLong(11))
    assert(err.message.matches(msg))

    // getFloat
    assert(testRow.getFloat(0) == 1f)
    assert(testRow.getFloat(1) == Short.MaxValue.toFloat)
    assert(testRow.getFloat(2) == Short.MinValue.toFloat)
    assert(testRow.getFloat(3) == Int.MaxValue.toFloat)
    assert(testRow.getFloat(4) == Int.MinValue.toFloat)
    assert(testRow.getFloat(5) == Long.MaxValue.toFloat)
    assert(testRow.getFloat(6) == Long.MinValue.toFloat)
    assert(testRow.getFloat(7) == Float.MaxValue)
    assert(testRow.getFloat(8) == Float.MinValue)
    msg = ".*Cannot cast .* to Float."
    err = intercept[SnowparkClientException](testRow.getFloat(9))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getFloat(10))
    assert(err.message.matches(msg))
    err = intercept[SnowparkClientException](testRow.getFloat(11))
    assert(err.message.matches(msg))

    // getDouble
    assert(testRow.getDouble(0) == 1.0)
    assert(testRow.getDouble(1) == Short.MaxValue.toDouble)
    assert(testRow.getDouble(2) == Short.MinValue.toDouble)
    assert(testRow.getDouble(3) == Int.MaxValue.toDouble)
    assert(testRow.getDouble(4) == Int.MinValue.toDouble)
    assert(testRow.getDouble(5) == Long.MaxValue.toDouble)
    assert(testRow.getDouble(6) == Long.MinValue.toDouble)
    assert(testRow.getDouble(7) == Float.MaxValue.toDouble)
    assert(testRow.getDouble(8) == Float.MinValue.toDouble)
    assert(testRow.getDouble(9) == Double.MaxValue)
    assert(testRow.getDouble(10) == Double.MinValue)
    msg = ".*Cannot cast .* to Double."
    err = intercept[SnowparkClientException](testRow.getDouble(11))
    assert(err.message.matches(msg))
  }

  test("getAs") {
    val milliseconds = System.currentTimeMillis()

    val schema = StructType(
      Seq(
        StructField("c01", BinaryType),
        StructField("c02", BooleanType),
        StructField("c03", ByteType),
        StructField("c04", DateType),
        StructField("c05", DoubleType),
        StructField("c06", FloatType),
        StructField("c07", GeographyType),
        StructField("c08", GeometryType),
        StructField("c09", IntegerType),
        StructField("c10", LongType),
        StructField("c11", ShortType),
        StructField("c12", StringType),
        StructField("c13", TimeType),
        StructField("c14", TimestampType),
        StructField("c15", VariantType)))

    var data = Seq(
      Row(
        Array[Byte](1, 9),
        true,
        Byte.MinValue,
        Date.valueOf("2024-01-01"),
        Double.MinValue,
        Float.MinPositiveValue,
        Geography.fromGeoJSON("POINT(30 10)"),
        Geometry.fromGeoJSON("POINT(20 40)"),
        Int.MinValue,
        Long.MinValue,
        Short.MinValue,
        "string",
        Time.valueOf("16:23:04"),
        new Timestamp(milliseconds),
        new Variant(1)))

    var df = session.createDataFrame(data, schema)
    var row = df.collect()(0)

    assert(row.getAs[Array[Byte]](0) sameElements Array[Byte](1, 9))
    assert(row.getAs[Boolean](1))
    assert(row.getAs[Byte](2) == Byte.MinValue)
    assert(row.getAs[Date](3) == Date.valueOf("2024-01-01"))
    assert(row.getAs[Double](4) == Double.MinValue)
    assert(row.getAs[Float](5) == Float.MinPositiveValue)
    assert(row.getAs[Geography](6) == Geography.fromGeoJSON("""{
    |  "coordinates": [
    |    30,
    |    10
    |  ],
    |  "type": "Point"
    |}""".stripMargin))
    assert(row.getAs[Geometry](7) == Geometry.fromGeoJSON("""{
    |  "coordinates": [
    |    2.000000000000000e+01,
    |    4.000000000000000e+01
    |  ],
    |  "type": "Point"
    |}""".stripMargin))
    assert(row.getAs[Int](8) == Int.MinValue)
    assert(row.getAs[Long](9) == Long.MinValue)
    assert(row.getAs[Short](10) == Short.MinValue)
    assert(row.getAs[String](11) == "string")
    assert(row.getAs[Time](12) == Time.valueOf("16:23:04"))
    assert(row.getAs[Timestamp](13) == new Timestamp(milliseconds))
    assert(row.getAs[Variant](14) == new Variant(1))
    assertThrows[ClassCastException](row.getAs[Boolean](0))
    assertThrows[ArrayIndexOutOfBoundsException](row.getAs[Boolean](-1))

    data = Seq(
      Row(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null))

    df = session.createDataFrame(data, schema)
    row = df.collect()(0)

    assert(row.getAs[Array[Byte]](0) == null)
    assert(!row.getAs[Boolean](1))
    assert(row.getAs[Byte](2) == 0)
    assert(row.getAs[Date](3) == null)
    assert(row.getAs[Double](4) == 0)
    assert(row.getAs[Float](5) == 0)
    assert(row.getAs[Geography](6) == null)
    assert(row.getAs[Geometry](7) == null)
    assert(row.getAs[Int](8) == 0)
    assert(row.getAs[Long](9) == 0)
    assert(row.getAs[Short](10) == 0)
    assert(row.getAs[String](11) == null)
    assert(row.getAs[Time](12) == null)
    assert(row.getAs[Timestamp](13) == null)
    assert(row.getAs[Variant](14) == null)
  }

  test("getAs with structured map") {
    structuredTypeTest {
      val query =
        """SELECT
          |  {'a':1,'b':2}::MAP(VARCHAR, NUMBER) as map1,
          |  {'1':'a','2':'b'}::MAP(NUMBER, VARCHAR) as map2,
          |  {'1':{'a':1,'b':2},'2':{'c':3}}::MAP(NUMBER, MAP(VARCHAR, NUMBER)) as map3
          |""".stripMargin

      val df = session.sql(query)
      val row = df.collect()(0)

      val map1 = row.getAs[Map[String, Long]](0)
      assert(map1("a") == 1L)
      assert(map1("b") == 2L)

      val map2 = row.getAs[Map[Long, String]](1)
      assert(map2(1) == "a")
      assert(map2(2) == "b")

      val map3 = row.getAs[Map[Long, Map[String, Long]]](2)
      assert(map3(1) == Map("a" -> 1, "b" -> 2))
      assert(map3(2) == Map("c" -> 3))
    }
  }

  test("getAs with structured array") {
    structuredTypeTest {
      TimeZone.setDefault(TimeZone.getTimeZone("US/Pacific"))

      val query =
        """SELECT
          |    [1,2,3]::ARRAY(NUMBER) AS arr1,
          |    ['a','b']::ARRAY(VARCHAR) AS arr2,
          |    [parse_json(31000000)::timestamp_ntz]::ARRAY(TIMESTAMP_NTZ) AS arr3,
          |    [[1,2]]::ARRAY(ARRAY) AS arr4
          |""".stripMargin

      val df = session.sql(query)
      val row = df.collect()(0)

      val array1 = row.getAs[Array[Object]](0)
      assert(array1 sameElements Array(1, 2, 3))

      val array2 = row.getAs[Array[Object]](1)
      assert(array2 sameElements Array("a", "b"))

      val array3 = row.getAs[Array[Object]](2)
      assert(array3 sameElements Array(new Timestamp(31000000000L)))

      val array4 = row.getAs[Array[Object]](3)
      assert(array4 sameElements Array("[\n  1,\n  2\n]"))
    }
  }

  test("hashCode") {
    val row1 = Row(1, 2, 3)
    val row2 = Row("str", null, 3)
    val row3 = Row("str", null, 3)

    assert(row1 != row2)
    assert(row1.hashCode() != row2.hashCode())
    assert(row2 == row3)
    assert(row2.hashCode() == row3.hashCode())
  }

  test("Row with Array[String]/Seq[String]/Map[String, String]") {
    val row1 = Row(Array("a", "b"), Seq("c", "d"))
    assert(row1.getVariant(0) == new Variant("[\"a\",\"b\"]"))
    assert(row1.getVariant(1) == new Variant("[\"c\",\"d\"]"))

    val row2 = Row(Map("1" -> "one"), Map("2" -> "two"))
    assert(row2.getVariant(0) == new Variant("{\"1\": \"one\"}"))
    assert(row2.getVariant(1) == new Variant("{\"2\": \"two\"}"))
  }

  test("Row with Array[Variant]/Seq[Variant]/Map[String, Variant]") {
    val row1 =
      Row(Array(new Variant("a"), new Variant("b")), Seq(new Variant("c"), new Variant("d")))
    assert(row1.getVariant(0) == new Variant("[\"a\",\"b\"]"))
    assert(row1.getVariant(1) == new Variant("[\"c\",\"d\"]"))

    val row2 = Row(Map("1" -> new Variant("one")), Map("2" -> new Variant("two")))
    assert(row2.getVariant(0) == new Variant("{\"1\": \"one\"}"))
    assert(row2.getVariant(1) == new Variant("{\"2\": \"two\"}"))
  }
}
