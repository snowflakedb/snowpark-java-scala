package com.snowflake.snowpark

import com.snowflake.snowpark.types._

class InternalDataTypeSuite extends UnitTestBase {
  test("schema string") {
    assert(ArrayType(IntegerType).schemaString == "Array")
    assert(
      new StructuredArrayType(IntegerType, true).schemaString == "Array[Integer nullable = true]")
    assert(BinaryType.schemaString == "Binary")
    assert(BooleanType.schemaString == "Boolean")
    assert(DateType.schemaString == "Date")
    assert(MapType(IntegerType, StringType).schemaString == "Map")
    assert(
      new StructuredMapType(
        StringType,
        IntegerType,
        true).schemaString == "Map[String, Integer nullable = true]")
    assert(ByteType.schemaString == "Byte")
    assert(ShortType.schemaString == "Short")
    assert(IntegerType.schemaString == "Integer")
    assert(LongType.schemaString == "Long")
    assert(FloatType.schemaString == "Float")
    assert(DoubleType.schemaString == "Double")
    assert(DecimalType(2, 1).schemaString == "Decimal(2, 1)")
    assert(StringType.schemaString == "String")
    assert(TimestampType.schemaString == "Timestamp")
    assert(TimeType.schemaString == "Time")
    assert(GeometryType.schemaString == "Geometry")
    assert(GeographyType.schemaString == "Geography")
    assert(StructType().schemaString == "Struct")
    assert(VariantType.schemaString == "Variant")
  }

  test("StructuredArray") {
    val arr1 = StructuredArrayType(IntegerType, nullable = false)
    assert(arr1.isInstanceOf[ArrayType])
    assert(arr1.toString == "ArrayType[Integer nullable = false]")
  }

  test("StructuredMap") {
    val map1 = StructuredMapType(StringType, IntegerType, isValueType = true)
    assert(map1.isInstanceOf[MapType])
    assert(map1.toString == "MapType[String, Integer nullable = true]")
  }

  test("Decimal") {
    assert(DecimalType.MAX_SCALE == 38)
    assert(DecimalType.MAX_PRECISION == 38)
  }

  test("Variant") {
    assert(Variant.VariantTypes.getType("RealNumber") == Variant.VariantTypes.RealNumber)
    assert(Variant.VariantTypes.getType("FixedNumber") == Variant.VariantTypes.FixedNumber)
    assert(Variant.VariantTypes.getType("Boolean") == Variant.VariantTypes.Boolean)
    assert(Variant.VariantTypes.getType("String") == Variant.VariantTypes.String)
    assert(Variant.VariantTypes.getType("Binary") == Variant.VariantTypes.Binary)
    assert(Variant.VariantTypes.getType("Time") == Variant.VariantTypes.Time)
    assert(Variant.VariantTypes.getType("Date") == Variant.VariantTypes.Date)
    assert(Variant.VariantTypes.getType("Timestamp") == Variant.VariantTypes.Timestamp)
    assert(Variant.VariantTypes.getType("Array") == Variant.VariantTypes.Array)
    assert(Variant.VariantTypes.getType("Object") == Variant.VariantTypes.Object)
    intercept[Exception] { Variant.VariantTypes.getType("not_exist_type") }
  }
}
