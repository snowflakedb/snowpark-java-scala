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

}
