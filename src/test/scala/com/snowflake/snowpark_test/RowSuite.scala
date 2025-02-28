package com.snowflake.snowpark_test

import com.snowflake.snowpark.types._
import com.snowflake.snowpark.{Row, SNTestBase, SnowparkClientException}

import java.sql.{Date, Time, Timestamp}
import java.time.{Instant, LocalDate}
import java.util
import java.util.TimeZone

class RowSuite extends SNTestBase {

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
      val oldTimeZone = TimeZone.getDefault
      try {
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
      } finally {
        TimeZone.setDefault(oldTimeZone)
      }
    }
  }

  test("getAs with field name") {
    val schema =
      StructType(Seq(StructField("EmpName", StringType), StructField("NumVal", IntegerType)))
    val df = session.createDataFrame(Seq(Row("abcd", 10), Row("efgh", 20)), schema)
    val row = df.collect()(0)

    assert(row.getAs[String]("EmpName") == row.getAs[String](0))
    assert(row.getAs[String]("EmpName").charAt(3) == 'd')
    assert(row.getAs[Int]("NumVal") == row.getAs[Int](1))

    assert(row.getAs[String]("EMPNAME") == row.getAs[String](0))

    assertThrows[IllegalArgumentException](row.getAs[String]("NonExistingColumn"))

    val rowWithoutSchema = Row(40, "Alice")
    assertThrows[UnsupportedOperationException](
      rowWithoutSchema.getAs[Integer]("NonExistingColumn"));
  }

  test("fieldIndex") {
    val schema =
      StructType(Seq(StructField("EmpName", StringType), StructField("NumVal", IntegerType)))
    assert(schema.fieldIndex("EmpName") == 0)
    assert(schema.fieldIndex("NumVal") == 1)
    assertThrows[IllegalArgumentException](schema.fieldIndex("NonExistingColumn"))
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
