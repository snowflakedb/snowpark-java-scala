package com.snowflake.snowpark

import com.snowflake.snowpark.types._
import com.snowflake.snowpark.proto.ast

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

  test("toAst") {
    assert(ArrayType(IntegerType).toAst.variant.isInstanceOf[ast.DataType.Variant.ArrayType])
    assert(
      StructuredArrayType(IntegerType, nullable = true).toAst.variant
        .isInstanceOf[ast.DataType.Variant.ArrayType])
    assert(BinaryType.toAst.variant.isInstanceOf[ast.DataType.Variant.BinaryType])
    assert(BooleanType.toAst.variant.isInstanceOf[ast.DataType.Variant.BooleanType])
    assert(ByteType.toAst.variant.isInstanceOf[ast.DataType.Variant.ByteType])
    assert(DateType.toAst.variant.isInstanceOf[ast.DataType.Variant.DateType])
    assert(DecimalType(2, 1).toAst.variant.isInstanceOf[ast.DataType.Variant.DecimalType])
    assert(DoubleType.toAst.variant.isInstanceOf[ast.DataType.Variant.DoubleType])
    assert(FloatType.toAst.variant.isInstanceOf[ast.DataType.Variant.FloatType])
    assert(GeographyType.toAst.variant.isInstanceOf[ast.DataType.Variant.GeographyType])
    assert(GeometryType.toAst.variant.isInstanceOf[ast.DataType.Variant.GeometryType])
    assert(IntegerType.toAst.variant.isInstanceOf[ast.DataType.Variant.IntegerType])
    assert(LongType.toAst.variant.isInstanceOf[ast.DataType.Variant.LongType])
    assert(
      MapType(IntegerType, StringType).toAst.variant.isInstanceOf[ast.DataType.Variant.MapType])
    assert(
      StructuredMapType(IntegerType, StringType, isValueType = true).toAst.variant
        .isInstanceOf[ast.DataType.Variant.MapType])
    assert(DoubleType.toAst.variant.isInstanceOf[ast.DataType.Variant.DoubleType])
    assert(ShortType.toAst.variant.isInstanceOf[ast.DataType.Variant.ShortType])
    assert(StringType.toAst.variant.isInstanceOf[ast.DataType.Variant.StringType])
    assert(StructType().toAst.variant.isInstanceOf[ast.DataType.Variant.StructType])
    assert(TimeType.toAst.variant.isInstanceOf[ast.DataType.Variant.TimeType])
    assert(TimestampType.toAst.variant.isInstanceOf[ast.DataType.Variant.TimestampType])
    assert(VariantType.toAst.variant.isInstanceOf[ast.DataType.Variant.VariantType])
  }
}
