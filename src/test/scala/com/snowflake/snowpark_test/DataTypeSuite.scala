package com.snowflake.snowpark_test

import com.snowflake.snowpark.{Row, SNTestBase, TestUtils}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.functions._

// Test DataTypes out of com.snowflake.snowpark package.
class DataTypeSuite extends SNTestBase {
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

    schema.printTreeString()

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
                  |    [1, 2, 3] AS arr0;""".stripMargin
    val df = session.sql(query)
    assert(
      TestUtils.treeString(df.schema, 0) ==
        s"""root
           | |--ARR1: ArrayType[Long nullable = true] (nullable = true)
           | |--ARR2: ArrayType[Double nullable = true] (nullable = true)
           | |--ARR3: ArrayType[Boolean nullable = true] (nullable = true)
           | |--ARR4: ArrayType[String nullable = true] (nullable = true)
           | |--ARR5: ArrayType[Timestamp nullable = true] (nullable = true)
           | |--ARR6: ArrayType[Binary nullable = true] (nullable = true)
           | |--ARR7: ArrayType[Date nullable = true] (nullable = true)
           | |--ARR8: ArrayType[Variant nullable = true] (nullable = true)
           | |--ARR9: ArrayType[ArrayType[String] nullable = true] (nullable = true)
           | |--ARR10: ArrayType[MapType[String, String] nullable = true] (nullable = true)
           | |--ARR11: ArrayType[ArrayType[Long nullable = true] nullable = true] (nullable = true)
           | |--ARR0: ArrayType[String] (nullable = true)
           |""".stripMargin)
    // schema string: nullable
    assert(
      // since we retrieved the schema of df before, df.select("*") will use the
      // schema query instead of the real query to analyze the result schema.
      TestUtils.treeString(df.select("*").schema, 0) ==
        s"""root
           | |--ARR1: ArrayType[Long nullable = true] (nullable = true)
           | |--ARR2: ArrayType[Double nullable = true] (nullable = true)
           | |--ARR3: ArrayType[Boolean nullable = true] (nullable = true)
           | |--ARR4: ArrayType[String nullable = true] (nullable = true)
           | |--ARR5: ArrayType[Timestamp nullable = true] (nullable = true)
           | |--ARR6: ArrayType[Binary nullable = true] (nullable = true)
           | |--ARR7: ArrayType[Date nullable = true] (nullable = true)
           | |--ARR8: ArrayType[Variant nullable = true] (nullable = true)
           | |--ARR9: ArrayType[ArrayType[String] nullable = true] (nullable = true)
           | |--ARR10: ArrayType[MapType[String, String] nullable = true] (nullable = true)
           | |--ARR11: ArrayType[ArrayType[Long nullable = true] nullable = true] (nullable = true)
           | |--ARR0: ArrayType[String] (nullable = true)
           |""".stripMargin)

    // schema string: not nullable
    val query2 =
      """SELECT
        |    [1, 2, 3]::ARRAY(NUMBER not null) AS arr1,
        |    [[1, 2], [3, 4]]::ARRAY(ARRAY(NUMBER not null) not null) AS arr11""".stripMargin

    val df2 = session.sql(query2)
    df.schema.printTreeString()
    assert(
      TestUtils.treeString(df2.schema, 0) ==
        s"""root
           | |--ARR1: ArrayType[Long nullable = false] (nullable = true)
           | |--ARR11: ArrayType[ArrayType[Long nullable = false] nullable = false] (nullable = true)
           |""".stripMargin)

    assert(
      TestUtils.treeString(df2.select("*").schema, 0) ==
        s"""root
           | |--ARR1: ArrayType[Long nullable = false] (nullable = true)
           | |--ARR11: ArrayType[ArrayType[Long nullable = false] nullable = false] (nullable = true)
           |""".stripMargin)

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
        s"""root
           | |--MAP1: MapType[String, Long] (nullable = true)
           | |--MAP2: MapType[Long, String] (nullable = true)
           | |--MAP3: MapType[Long, ArrayType[Long]] (nullable = true)
           | |--MAP4: MapType[Long, MapType[String, Long]] (nullable = true)
           | |--MAP0: MapType[String, String] (nullable = true)
           |""".stripMargin)
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
           | |--OBJECT1: StructType[StructField(A, String, Nullable = true), StructField(B, Long, Nullable = true)] (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: Long (nullable = true)
           | |--OBJECT2: StructType[StructField(A, String, Nullable = true), StructField(B, ArrayType[Long], Nullable = true)] (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: ArrayType[Long] (nullable = true)
           | |--OBJECT3: StructType[StructField(A, String, Nullable = true), StructField(B, ArrayType[Long], Nullable = true), StructField(C, MapType[Long, String], Nullable = true)] (nullable = true)
           |   |--A: String (nullable = true)
           |   |--B: ArrayType[Long] (nullable = true)
           |   |--C: MapType[Long, String] (nullable = true)
           | |--OBJECT4: StructType[StructField(A, StructType[StructField(B, StructType[StructField(C, Long, Nullable = true)], Nullable = true)], Nullable = true)] (nullable = true)
           |   |--A: StructType[StructField(B, StructType[StructField(C, Long, Nullable = true)], Nullable = true)] (nullable = true)
           |     |--B: StructType[StructField(C, Long, Nullable = true)] (nullable = true)
           |       |--C: Long (nullable = true)
           |""".stripMargin)
    // scalastyle:on
  }
}
