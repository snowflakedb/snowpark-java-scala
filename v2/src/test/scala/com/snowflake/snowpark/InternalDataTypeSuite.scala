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
    checkAst(
      ArrayType(IntegerType).toAst,
      ast.DataType(variant = ast.DataType.Variant.ArrayType(
        ast.ArrayType(ty = Some(ast.DataType(variant = ast.DataType.Variant.IntegerType(true)))))))
    checkAst(
      StructuredArrayType(IntegerType, nullable = true).toAst,
      ast.DataType(variant = ast.DataType.Variant.ArrayType(
        ast.ArrayType(
          structured = true,
          ty = Some(ast.DataType(variant = ast.DataType.Variant.IntegerType(true)))))))
    checkAst(BinaryType.toAst, ast.DataType(variant = ast.DataType.Variant.BinaryType(true)))
    checkAst(BooleanType.toAst, ast.DataType(variant = ast.DataType.Variant.BooleanType(true)))
    checkAst(ByteType.toAst, ast.DataType(variant = ast.DataType.Variant.ByteType(true)))
    checkAst(DateType.toAst, ast.DataType(variant = ast.DataType.Variant.DateType(true)))
    checkAst(
      DecimalType(2, 1).toAst,
      ast.DataType(variant = ast.DataType.Variant.DecimalType(ast.DecimalType(2, 1))))
    checkAst(DoubleType.toAst, ast.DataType(variant = ast.DataType.Variant.DoubleType(true)))
    checkAst(FloatType.toAst, ast.DataType(variant = ast.DataType.Variant.FloatType(true)))
    checkAst(GeographyType.toAst, ast.DataType(variant = ast.DataType.Variant.GeographyType(true)))
    checkAst(GeometryType.toAst, ast.DataType(variant = ast.DataType.Variant.GeometryType(true)))
    checkAst(IntegerType.toAst, ast.DataType(variant = ast.DataType.Variant.IntegerType(true)))
    checkAst(LongType.toAst, ast.DataType(variant = ast.DataType.Variant.LongType(true)))
    checkAst(
      MapType(IntegerType, StringType).toAst,
      ast.DataType(variant = ast.DataType.Variant.MapType(ast.MapType(
        keyTy = Some(ast.DataType(variant = ast.DataType.Variant.IntegerType(true))),
        valueTy = Some(ast.DataType(variant =
          ast.DataType.Variant.StringType(ast.StringType(length = Some(1)))))))))
    checkAst(
      StructuredMapType(IntegerType, StringType, isValueType = true).toAst,
      ast.DataType(variant = ast.DataType.Variant.MapType(ast.MapType(
        keyTy = Some(ast.DataType(variant = ast.DataType.Variant.IntegerType(true))),
        structured = true,
        valueTy = Some(ast.DataType(variant =
          ast.DataType.Variant.StringType(ast.StringType(length = Some(1)))))))))
    checkAst(ShortType.toAst, ast.DataType(variant = ast.DataType.Variant.ShortType(true)))
    checkAst(
      StringType.toAst,
      ast.DataType(variant = ast.DataType.Variant.StringType(ast.StringType(length = Some(1)))))
//    assert(StructType().toAst.variant.isInstanceOf[ast.DataType.Variant.StructType])
    checkAst(TimeType.toAst, ast.DataType(variant = ast.DataType.Variant.TimeType(true)))
//    assert(TimestampType.toAst.variant.isInstanceOf[ast.DataType.Variant.TimestampType])
//    assert(VariantType.toAst.variant.isInstanceOf[ast.DataType.Variant.VariantType])
  }
}
