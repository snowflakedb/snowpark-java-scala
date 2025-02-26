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
  }
}
