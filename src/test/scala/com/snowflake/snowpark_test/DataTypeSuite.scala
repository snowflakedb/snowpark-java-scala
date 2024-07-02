package com.snowflake.snowpark_test

import com.snowflake.snowpark.{Row, SNTestBase, TestUtils}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.Utils

import java.sql.{Date, Time, Timestamp}
import java.util.TimeZone

// Test DataTypes out of com.snowflake.snowpark package.
class DataTypeSuite extends SNTestBase {
  override def beforeAll: Unit = {
    super.beforeAll
    if (isPreprodAccount) {
      session.sql("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE=true").show()
      session
        .sql("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true")
        .show()
      session
        .sql("alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT=true")
        .show()
    }
  }

  override def afterAll: Unit = {
    if (isPreprodAccount) {
      session.sql("alter session unset ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE").show()
      session.sql("alter session unset IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE").show()
      session.sql("alter session unset FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT").show()
    }
    super.afterAll
  }
  test("IntegralType") {
    def verifyIntegralType(tpe: DataType): Unit = {
      assert(TestUtils.isIntegralType(tpe))
      assert(TestUtils.isNumericType(tpe))
      assert(TestUtils.isAtomicType(tpe))
      assert(tpe.isInstanceOf[DataType])
      assert(tpe.typeName == tpe.toString)
      assert(tpe.typeName == tpe.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type"))
    }

    Seq(ByteType, ShortType, IntegerType, LongType).foreach(verifyIntegralType)
    succeed
  }

  test("FractionalType") {
    def verifyIntegralType(tpe: DataType): Unit = {
      assert(TestUtils.isFractionalType(tpe))
      assert(TestUtils.isNumericType(tpe))
      assert(TestUtils.isAtomicType(tpe))
      assert(tpe.isInstanceOf[DataType])
      assert(tpe.typeName == tpe.toString)
      assert(tpe.typeName == tpe.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type"))
    }

    Seq(FloatType, DoubleType).foreach(verifyIntegralType)
    succeed
  }

  test("DecimalType") {
    val tpe = DecimalType(38, 19)
    assert(TestUtils.isFractionalType(tpe))
    assert(TestUtils.isNumericType(tpe))
    assert(TestUtils.isAtomicType(tpe))
    assert(tpe.isInstanceOf[DataType])
    assert(tpe.typeName == tpe.toString)
    assert(tpe.toString == "Decimal(38, 19)")
  }

  test("StringType") {
    assert(StringType.isInstanceOf[DataType])
    assert(TestUtils.isAtomicType(StringType))
    assert(StringType.typeName == StringType.toString)
    assert(StringType.typeName == "String")
  }

  test("BooleanType") {
    assert(BooleanType.isInstanceOf[DataType])
    assert(TestUtils.isAtomicType(BooleanType))
    assert(BooleanType.typeName == BooleanType.toString)
    assert(BooleanType.typeName == "Boolean")
  }

  test("DateType") {
    assert(DateType.isInstanceOf[DataType])
    assert(TestUtils.isAtomicType(DateType))
    assert(DateType.typeName == DateType.toString)
    assert(DateType.typeName == "Date")
  }

  test("BinaryType") {
    assert(BinaryType.isInstanceOf[DataType])
    assert(TestUtils.isAtomicType(BinaryType))
    assert(BinaryType.typeName == BinaryType.toString)
    assert(BinaryType.typeName == "Binary")
  }

  test("TimestampType") {
    assert(TimestampType.isInstanceOf[DataType])
    assert(TestUtils.isAtomicType(TimestampType))
    assert(TimestampType.typeName == TimestampType.toString)
    assert(TimestampType.typeName == "Timestamp")
  }

  test("StructType") {
    var tpe = StructType()
    assert(tpe.isInstanceOf[DataType])
    assert(tpe.length == 0)

    tpe = tpe.add("col1", IntegerType).add(StructField("col2", StringType, nullable = false))
    assert(tpe.length == 2)
    assert(tpe.typeName == "Struct")
    assert(
      tpe.toString == "StructType[StructField(COL1, Integer, Nullable = true), " +
        "StructField(COL2, String, Nullable = false)]")

    assert(tpe(1) == StructField("col2", StringType, nullable = false))
    assert(tpe("col1") == StructField("col1", IntegerType))

    assert(tpe.names == Seq("COL1", "COL2"))
    assert(tpe.nameToField("col3").isEmpty)

    assertThrows[ArrayIndexOutOfBoundsException] {
      tpe(3)
    }
    assertThrows[IllegalArgumentException] {
      tpe("col3")
    }
  }

  test("printTreeString") {
    val schema: StructType = StructType(
      Seq(
        StructField("col1", BinaryType),
        StructField("col2", BooleanType),
        StructField(
          "col14",
          StructType(Seq(StructField("col15", TimestampType, nullable = false))),
          nullable = false),
        StructField("col3", DateType, nullable = false),
        StructField(
          "col4",
          StructType(Seq(
            StructField("col5", ByteType),
            StructField("col6", ShortType),
            StructField("col7", IntegerType, nullable = false),
            StructField("col8", LongType),
            StructField(
              "col12",
              StructType(Seq(StructField("col13", StringType))),
              nullable = false),
            StructField("col9", FloatType),
            StructField("col10", DoubleType),
            StructField("col11", DecimalType(10, 1)))))))

    assert(
      TestUtils.treeString(schema, 0) ==
        s"""root
           | |--COL1: Binary (nullable = true)
           | |--COL2: Boolean (nullable = true)
           | |--COL14: Struct (nullable = false)
           |   |--COL15: Timestamp (nullable = false)
           | |--COL3: Date (nullable = false)
           | |--COL4: Struct (nullable = true)
           |   |--COL5: Byte (nullable = true)
           |   |--COL6: Short (nullable = true)
           |   |--COL7: Integer (nullable = false)
           |   |--COL8: Long (nullable = true)
           |   |--COL12: Struct (nullable = false)
           |     |--COL13: String (nullable = true)
           |   |--COL9: Float (nullable = true)
           |   |--COL10: Double (nullable = true)
           |   |--COL11: Decimal(10, 1) (nullable = true)
           |""".stripMargin)
  }

  test("ColumnIdentifier") {
    val column1 = ColumnIdentifier("col")
    val column2 = ColumnIdentifier("COL")
    val column3 = ColumnIdentifier("\"COL\"")
    val column4 = ColumnIdentifier("\"col\"")
    assert(column1 == column2)
    assert(column2.equals(column3))
    assert(column3 != column4)

    assert(column1.hashCode() == column3.hashCode())
    assert(column1.clone() == column2)
  }

  test("use BigDecimal more times") {
    val d1 = DecimalType(2, 1)
    val d2 = DecimalType(2, 1)
    assert(d1.equals(d2))

    val df = session
      .range(1)
      .select(
        lit(0.05).cast(DecimalType(5, 2)).as("a"),
        lit(0.01).cast(DecimalType(7, 2)).as("b"))

    assert(
      TestUtils.treeString(df.schema, 0) ==
        s"""root
           | |--A: Decimal(5, 2) (nullable = false)
           | |--B: Decimal(7, 2) (nullable = false)
           |""".stripMargin)
  }

  test("read Structured Array") {
    val oldTimeZone = TimeZone.getDefault
    try {
      // Need to set default time zone because the expected result has timestamp data
      TimeZone.setDefault(TimeZone.getTimeZone("US/Pacific"))
      val query =
        """SELECT
          |    [1, 2, 3]::ARRAY(NUMBER) AS arr1,
          |    [1.1, 2.2, 3.3]::ARRAY(FLOAT) AS arr2,
          |    [true, false]::ARRAY(BOOLEAN) AS arr3,
          |    ['a', 'b']::ARRAY(VARCHAR) AS arr4,
          |    [parse_json(31000000)::timestamp_ntz]::ARRAY(TIMESTAMP_NTZ) AS arr5,
          |    [TO_BINARY('SNOW', 'utf-8')]::ARRAY(BINARY) AS arr6,
          |    [TO_DATE('2013-05-17')]::ARRAY(DATE) AS arr7,
          |    [[1,2]]::ARRAY(ARRAY) AS arr9,
          |    [OBJECT_CONSTRUCT('name', 1)]::ARRAY(OBJECT) AS arr10,
          |    [[1, 2], [3, 4]]::ARRAY(ARRAY(NUMBER)) AS arr11,
          |    [1.234::DECIMAL(13, 5)]::ARRAY(DECIMAL(13,5)) as arr12,
          |    [time '10:03:56']::ARRAY(TIME) as arr21
          |""".stripMargin
      val df = session.sql(query)
      assert(df.collect().head.getSeq[Double](1).isInstanceOf[Seq[Double]])
      checkAnswer(
        df,
        Row(
          Array(1L, 2L, 3L),
          Array(1.1, 2.2, 3.3),
          Array(true, false),
          Array("a", "b"),
          Array(new Timestamp(31000000000L)),
          Array(Array(83.toByte, 78.toByte, 79.toByte, 87.toByte)),
          Array(Date.valueOf("2013-05-17")),
          Array("[\n  1,\n  2\n]"),
          Array("{\n  \"name\": 1\n}"),
          Array(Array(1L, 2L), Array(3L, 4L)),
          Array(java.math.BigDecimal.valueOf(1.234)),
          Array(Time.valueOf("10:03:56"))))
    } finally {
      TimeZone.setDefault(oldTimeZone)
    }
  }

  test("read Structured Map") {
    val query =
      """SELECT
        |  {'a':1,'b':2} :: MAP(VARCHAR, NUMBER) as map1,
        |  {'1':'a','2':'b'} :: MAP(NUMBER, VARCHAR) as map2,
        |  {'1':[1,2,3],'2':[4,5,6]} :: MAP(NUMBER, ARRAY(NUMBER)) as map3,
        |  {'1':{'a':1,'b':2},'2':{'c':3}} :: MAP(NUMBER, MAP(VARCHAR, NUMBER)) as map4,
        |  [{'a':1,'b':2},{'c':3}] :: ARRAY(MAP(VARCHAR, NUMBER)) as map5,
        |  {'a':1,'b':2} :: OBJECT as map0
        |""".stripMargin
    val df = session.sql(query)
    checkAnswer(
      df,
      Row(
        Map("b" -> 2, "a" -> 1),
        Map(2 -> "b", 1 -> "a"),
        Map(2 -> Array(4L, 5L, 6L), 1 -> Array(1L, 2L, 3L)),
        Map(2 -> Map("c" -> 3), 1 -> Map("a" -> 1, "b" -> 2)),
        Array(Map("a" -> 1, "b" -> 2), Map("c" -> 3)),
        "{\n  \"a\": 1,\n  \"b\": 2\n}"))
  }

  test("read object") {
    val query =
      // scalastyle:off
      """SELECT
        |  {'b': 1, 'a': '22'} :: OBJECT(a VARCHAR, b NUMBER) as object1,
        |  {'a': 1, 'b': [1,2,3,4], 'c': true} :: OBJECT(a NUMBER, b ARRAY(NUMBER), c BOOLEAN) as object2,
        |  {'a': 1, 'b': [1,2,3,4], 'c': {'1':'a'}} :: OBJECT(a NUMBER, b ARRAY(NUMBER), c MAP(NUMBER, VARCHAR)) as object3,
        |  {'a': {'b': {'a':10,'c': 1}}} :: OBJECT(a OBJECT(b OBJECT(c NUMBER, a NUMBER))) as object4,
        |  [{'a':1,'b':2},{'b':3,'a':4}] :: ARRAY(OBJECT(a NUMBER, b NUMBER)) as arr1,
        |  {'a1':{'b':2}, 'a2':{'b':3}} :: MAP(VARCHAR, OBJECT(b NUMBER)) as map1
        |""".stripMargin
    // scalastyle:on

    val df = session.sql(query)
    val result = df.collect()
    assert(result.length == 1)
    val row = result.head
    assert(row.getObject(0).length == 2)
    assert(row.getObject(0).getString(0) == "22")
    assert(row.getObject(0).getLong(1) == 1L)

    assert(row.getObject(1).length == 3)
    assert(row.getObject(1).getLong(0) == 1L)
    assert(row.getObject(1).getSeq(1).length == 4)
    val arr1 = row.getObject(1).getSeq[Long](1)
    assert(arr1.isInstanceOf[Seq[Long]])
    assert(arr1.sameElements(Array(1L, 2L, 3L, 4L)))
    assert(row.getObject(1).getBoolean(2))

    assert(row.getObject(2).length == 3)
    assert(row.getObject(2).getLong(0) == 1L)
    assert(row.getObject(2).getSeq(1).length == 4)
    val arr2 = row.getObject(2).getSeq[Long](1)
    assert(arr2.isInstanceOf[Seq[Long]])
    assert(arr2.sameElements(Array(1L, 2L, 3L, 4L)))
    val map1 = row.getObject(2).getMap[Long, String](2)
    assert(map1 == Map(1L -> "a"))

    assert(row.getObject(3).length == 1)
    val row1 = row.getObject(3).getObject(0)
    assert(row1.length == 1)
    val row2 = row1.getObject(0)
    assert(row2.length == 2)
    assert(row2.getInt(0) == 1)
    assert(row2.getInt(1) == 10)

    assert(row.getSeq[Row](4).length == 2)
    val arr3 = row.getSeq[Row](4)
    val row3 = arr3.head
    val row4 = arr3(1)
    assert(row3.length == 2)
    assert(row3.getInt(0) == 1)
    assert(row3.getInt(1) == 2)
    assert(row4.length == 2)
    assert(row4.getInt(0) == 4)
    assert(row4.getInt(1) == 3)

    assert(row.getMap[String, Row](5).size == 2)
    val map2 = row.getMap[String, Row](5)
    val row5 = map2("a1")
    val row6 = map2("a2")
    assert(row5.length == 1)
    assert(row5.getInt(0) == 2)
    assert(row6.length == 1)
    assert(row6.getInt(0) == 3)

  }

  test("ArrayType v2") {
    val query = """SELECT
                  |    [1, 2, 3]::ARRAY(NUMBER) AS arr1,
                  |    [1.1, 2.2, 3.3]::ARRAY(FLOAT) AS arr2,
                  |    [true, false]::ARRAY(BOOLEAN) AS arr3,
                  |    ['a', 'b']::ARRAY(VARCHAR) AS arr4,
                  |    [parse_json(31000000)::timestamp_ntz]::ARRAY(TIMESTAMP_NTZ) AS arr5,
                  |    [TO_BINARY('SNOW', 'utf-8')]::ARRAY(BINARY) AS arr6,
                  |    [TO_DATE('2013-05-17')]::ARRAY(DATE) AS arr7,
                  |    ['1', 2]::ARRAY(VARIANT) AS arr8,
                  |    [[1,2]]::ARRAY(ARRAY) AS arr9,
                  |    [OBJECT_CONSTRUCT('name', 1)]::ARRAY(OBJECT) AS arr10,
                  |    [[1, 2], [3, 4]]::ARRAY(ARRAY(NUMBER)) AS arr11,
                  |    [1, 2, 3] AS arr0""".stripMargin
    val df = session.sql(query)
    assert(
      TestUtils.treeString(df.schema, 0) ==
        s"""root
           | |--ARR1: Array[Long nullable = true] (nullable = true)
           | |--ARR2: Array[Double nullable = true] (nullable = true)
           | |--ARR3: Array[Boolean nullable = true] (nullable = true)
           | |--ARR4: Array[String nullable = true] (nullable = true)
           | |--ARR5: Array[Timestamp nullable = true] (nullable = true)
           | |--ARR6: Array[Binary nullable = true] (nullable = true)
           | |--ARR7: Array[Date nullable = true] (nullable = true)
           | |--ARR8: Array[Variant nullable = true] (nullable = true)
           | |--ARR9: Array[Array nullable = true] (nullable = true)
           | |--ARR10: Array[Map nullable = true] (nullable = true)
           | |--ARR11: Array[Array[Long nullable = true] nullable = true] (nullable = true)
           | |--ARR0: Array (nullable = true)
           |""".stripMargin)
    // schema string: nullable
    assert(
      // since we retrieved the schema of df before, df.select("*") will use the
      // schema query instead of the real query to analyze the result schema.
      TestUtils.treeString(df.select("*").schema, 0) ==
        s"""root
           | |--ARR1: Array[Long nullable = true] (nullable = true)
           | |--ARR2: Array[Double nullable = true] (nullable = true)
           | |--ARR3: Array[Boolean nullable = true] (nullable = true)
           | |--ARR4: Array[String nullable = true] (nullable = true)
           | |--ARR5: Array[Timestamp nullable = true] (nullable = true)
           | |--ARR6: Array[Binary nullable = true] (nullable = true)
           | |--ARR7: Array[Date nullable = true] (nullable = true)
           | |--ARR8: Array[Variant nullable = true] (nullable = true)
           | |--ARR9: Array[Array nullable = true] (nullable = true)
           | |--ARR10: Array[Map nullable = true] (nullable = true)
           | |--ARR11: Array[Array[Long nullable = true] nullable = true] (nullable = true)
           | |--ARR0: Array (nullable = true)
           |""".stripMargin)

    // schema string: not nullable
    val query2 =
      """SELECT
        |    [1, 2, 3]::ARRAY(NUMBER not null) AS arr1,
        |    [[1, 2], [3, 4]]::ARRAY(ARRAY(NUMBER not null) not null) AS arr11""".stripMargin

    val df2 = session.sql(query2)
    assert(
      TestUtils.treeString(df2.schema, 0) ==
        // scalastyle:off
        s"""root
           | |--ARR1: Array[Long nullable = false] (nullable = true)
           | |--ARR11: Array[Array[Long nullable = false] nullable = false] (nullable = true)
           |""".stripMargin)
    // scalastyle:on

    assert(
      TestUtils.treeString(df2.select("*").schema, 0) ==
        // scalastyle:off
        s"""root
           | |--ARR1: Array[Long nullable = false] (nullable = true)
           | |--ARR11: Array[Array[Long nullable = false] nullable = false] (nullable = true)
           |""".stripMargin)
    // scalastyle:on
  }

  test("MapType v2") {
    val query =
      """SELECT
        |  {'a': 1, 'b': 2} :: MAP(VARCHAR, NUMBER) as map1,
        |  {'1': 'a'} :: MAP(NUMBER, VARCHAR) as map2,
        |  {'1': [1,2,3]} :: MAP(NUMBER, ARRAY(NUMBER)) as map3,
        |  {'1': {'a':1}} :: MAP(NUMBER, MAP(VARCHAR, NUMBER)) as map4,
        |  {'a': 1, 'b': 2} :: OBJECT as map0
        |""".stripMargin
    val df = session.sql(query)
    assert(
      TestUtils.treeString(df.schema, 0) ==
        // scalastyle:off
        s"""root
           | |--MAP1: Map[String, Long nullable = true] (nullable = true)
           | |--MAP2: Map[Long, String nullable = true] (nullable = true)
           | |--MAP3: Map[Long, Array[Long nullable = true] nullable = true] (nullable = true)
           | |--MAP4: Map[Long, Map[String, Long nullable = true] nullable = true] (nullable = true)
           | |--MAP0: Map (nullable = true)
           |""".stripMargin)
    // scalastyle:on

    assert(
      // since we retrieved the schema of df before, df.select("*") will use the
      // schema query instead of the real query to analyze the result schema.
      TestUtils.treeString(df.select("*").schema, 0) ==
        // scalastyle:off
        s"""root
           | |--MAP1: Map[String, Long nullable = true] (nullable = true)
           | |--MAP2: Map[Long, String nullable = true] (nullable = true)
           | |--MAP3: Map[Long, Array[Long nullable = true] nullable = true] (nullable = true)
           | |--MAP4: Map[Long, Map[String, Long nullable = true] nullable = true] (nullable = true)
           | |--MAP0: Map (nullable = true)
           |""".stripMargin)
    // scalastyle:on

    // nullable
    val query2 =
      """SELECT
        |  {'a': 1, 'b': 2} :: MAP(VARCHAR, NUMBER not null) as map1,
        |  {'1': [1,2,3]} :: MAP(NUMBER, ARRAY(NUMBER not null)) as map3,
        |  {'1': {'a':1}} :: MAP(NUMBER, MAP(VARCHAR, NUMBER not null)) as map4
        |""".stripMargin
    val df2 = session.sql(query2)
    assert(
      TestUtils.treeString(df2.schema, 0) ==
        // scalastyle:off
        s"""root
           | |--MAP1: Map[String, Long nullable = false] (nullable = true)
           | |--MAP3: Map[Long, Array[Long nullable = false] nullable = true] (nullable = true)
           | |--MAP4: Map[Long, Map[String, Long nullable = false] nullable = true] (nullable = true)
           |""".stripMargin)
    // scalastyle:on

    assert(
      TestUtils.treeString(df2.select("*").schema, 0) ==
        // scalastyle:off
        s"""root
           | |--MAP1: Map[String, Long nullable = false] (nullable = true)
           | |--MAP3: Map[Long, Array[Long nullable = false] nullable = true] (nullable = true)
           | |--MAP4: Map[Long, Map[String, Long nullable = false] nullable = true] (nullable = true)
           |""".stripMargin)
    // scalastyle:on

  }

  test("ObjectType v2") {
    val query =
      // scalastyle:off
      """SELECT
        |  {'a': 1, 'b': 'a'} :: OBJECT(a VARCHAR, b NUMBER) as object1,
        |  {'a': 1, 'b': [1,2,3,4]} :: OBJECT(a VARCHAR, b ARRAY(NUMBER)) as object2,
        |  {'a': 1, 'b': [1,2,3,4], 'c': {'1':'a'}} :: OBJECT(a VARCHAR, b ARRAY(NUMBER), c MAP(NUMBER, VARCHAR)) as object3,
        |  {'a': {'b': {'c': 1}}} :: OBJECT(a OBJECT(b OBJECT(c NUMBER))) as object4
        |""".stripMargin
    // scalastyle:on
    val df = session.sql(query)
    assert(
      TestUtils.treeString(df.schema, 0) ==
        // scalastyle:off
        s"""root
           | |--OBJECT1: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Long (nullable = true)
           | |--OBJECT2: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = true] (nullable = true)
           | |--OBJECT3: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = true] (nullable = true)
           |   |--C: Map[Long, String nullable = true] (nullable = true)
           | |--OBJECT4: Struct (nullable = true)
           |   |--A: Struct (nullable = true)
           |     |--B: Struct (nullable = true)
           |       |--C: Long (nullable = true)
           |""".stripMargin)
    // scalastyle:on

    // schema string: nullable
    assert(
      TestUtils.treeString(df.select("*").schema, 0) ==
        // scalastyle:off
        s"""root
           | |--OBJECT1: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Long (nullable = true)
           | |--OBJECT2: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = true] (nullable = true)
           | |--OBJECT3: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = true] (nullable = true)
           |   |--C: Map[Long, String nullable = true] (nullable = true)
           | |--OBJECT4: Struct (nullable = true)
           |   |--A: Struct (nullable = true)
           |     |--B: Struct (nullable = true)
           |       |--C: Long (nullable = true)
           |""".stripMargin)
    // scalastyle:on

    // schema query: not null
    val query2 =
      // scalastyle:off
      """SELECT
        |  {'a': 1, 'b': 'a'} :: OBJECT(a VARCHAR not null, b NUMBER) as object1,
        |  {'a': 1, 'b': [1,2,3,4]} :: OBJECT(a VARCHAR, b ARRAY(NUMBER not null) not null) as object2,
        |  {'a': 1, 'b': [1,2,3,4], 'c': {'1':'a'}} :: OBJECT(a VARCHAR, b ARRAY(NUMBER), c MAP(NUMBER, VARCHAR not null) not null) as object3,
        |  {'a': {'b': {'c': 1}}} :: OBJECT(a OBJECT(b OBJECT(c NUMBER not null) not null) not null) as object4
        |""".stripMargin
    // scalastyle:on

    val df2 = session.sql(query2)
    assert(
      TestUtils.treeString(df2.schema, 0) ==
        // scalastyle:off
        s"""root
           | |--OBJECT1: Struct (nullable = true)
           |   |--A: String (nullable = false)
           |   |--B: Long (nullable = true)
           | |--OBJECT2: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = false] (nullable = false)
           | |--OBJECT3: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = true] (nullable = true)
           |   |--C: Map[Long, String nullable = false] (nullable = false)
           | |--OBJECT4: Struct (nullable = true)
           |   |--A: Struct (nullable = false)
           |     |--B: Struct (nullable = false)
           |       |--C: Long (nullable = false)
           |""".stripMargin)
    // scalastyle:on

    assert(
      TestUtils.treeString(df2.select("*").schema, 0) ==
        // scalastyle:off
        s"""root
           | |--OBJECT1: Struct (nullable = true)
           |   |--A: String (nullable = false)
           |   |--B: Long (nullable = true)
           | |--OBJECT2: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = false] (nullable = false)
           | |--OBJECT3: Struct (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Array[Long nullable = true] (nullable = true)
           |   |--C: Map[Long, String nullable = false] (nullable = false)
           | |--OBJECT4: Struct (nullable = true)
           |   |--A: Struct (nullable = false)
           |     |--B: Struct (nullable = false)
           |       |--C: Long (nullable = false)
           |""".stripMargin)
    // scalastyle:on
  }

  test("Variant containing word null in the text") {
    import session.implicits._
    var variant = new Variant("null string starts with null")
    var df = Seq(variant).toDF("a")
    checkAnswer(df, Row("\"null string starts with null\""))

    variant = new Variant("string with null in the middle")
    df = Seq(variant).toDF("a")
    checkAnswer(df, Row("\"string with null in the middle\""))

    variant = new Variant("string with null in the end null")
    df = Seq(variant).toDF("a")
    checkAnswer(df, Row("\"string with null in the end null\""))
  }
}
