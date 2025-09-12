package com.snowflake.snowpark

import com.snowflake.snowpark.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util.Locale

class DataTypeParserSuite extends AnyFunSuite {

  private def expectRight(input: String, expected: DataType): Unit = {
    DataTypeParser.parseDataType(input) match {
      case Right(dt) => assert(dt == expected, s"$input parsed to $dt, expected $expected")
      case Left(err) => fail(s"Expected Right for '$input' but got error: ${err.format}")
    }
  }

  private def expectLeftContains(input: String, needle: String): Unit = {
    DataTypeParser.parseDataType(input) match {
      case Left(err) =>
        assert(err.format.toLowerCase(Locale.ROOT).contains(needle.toLowerCase(Locale.ROOT)))
      case Right(dt) => fail(s"Expected Left for '$input' but got: $dt")
    }
  }

  test("parseDataType - simple types") {
    expectRight("byte", ByteType)
    expectRight("ByteInt", ByteType)
    expectRight("TINYINT", ByteType)
    expectRight("short", ShortType)
    expectRight("SMALLINT", ShortType)
    expectRight("INT", IntegerType)
    expectRight("INTEGER", IntegerType)
    expectRight("long", LongType)
    expectRight("BIGINT", LongType)
    expectRight("float", FloatType)
    expectRight("double", DoubleType)
    expectRight("decimal", DecimalType(38, 0))
    expectRight("number", DecimalType(38, 0))
    expectRight("numeric", DecimalType(38, 0))
    expectRight(" string ", StringType)
    expectRight("binary", BinaryType)
    expectRight("date", DateType)
    expectRight("time", TimeType)
    expectRight("timestamp", TimestampType)
    expectRight("variant", VariantType)
    expectRight("object", MapType(StringType, VariantType))
    expectRight("boolean", BooleanType)
    expectRight("geography", GeographyType)
    expectRight("geometry", GeometryType)
  }

  test("parseDataType - decimal without scale defaults to 0") {
    expectRight("decimal(10)", DecimalType(10, 0))
    expectRight("number(38)", DecimalType(38, 0))
    expectRight("numeric(5)", DecimalType(5, 0))
  }

  test("parseDataType - decimal with precision and scale") {
    expectRight("decimal(10,2)", DecimalType(10, 2))
    expectRight(" number ( 18 , 0 ) ", DecimalType(18, 0))
    expectRight("numeric(5, 2)", DecimalType(5, 2))
  }

  test("parseDataType - decimal invalid numbers and bounds") {
    expectLeftContains("decimal(x)", "not a valid integer")
    expectLeftContains("decimal(10,x)", "not a valid integer")
    expectLeftContains("decimal(0,0)", "precision must be between 1")
    expectLeftContains("decimal(39,0)", "precision must be between 1")
    expectLeftContains("decimal(10,-1)", "scale must be between 0")
    expectLeftContains("decimal(10,39)", "scale must be between 0")
    expectLeftContains("decimal(10,11)", "scale cannot exceed precision")
  }

  test("parseDataType - array of simple and complex types") {
    expectRight("array<string>", ArrayType(StringType))
    expectRight("array< decimal(10,2) >", ArrayType(DecimalType(10, 2)))
  }

  test("parseDataType - map of nested types and delimiter at top level only") {
    expectRight("map<string, int>", MapType(StringType, IntegerType))
    expectRight(
      "map<decimal(10,2), array<string>>",
      MapType(DecimalType(10, 2), ArrayType(StringType)))
  }

  test("parseDataType - map malformed inner types") {
    expectLeftContains("map<,>", "Malformed type")
    expectLeftContains("map<string>", "Malformed type")
    expectLeftContains("map<string,>", "Malformed type")
    expectLeftContains("map<,string>", "Malformed type")
  }

  test("parseDataType - nested delimiter splitting only at top level") {
    // ensure the comma inside decimal() isn't treated as top-level for map splitting
    expectRight(
      "map<array<decimal(10,2)>, array<decimal(3,0)>>",
      MapType(ArrayType(DecimalType(10, 2)), ArrayType(DecimalType(3, 0))))
  }

  test("parseDataType - unbalanced brackets in complex types") {
    expectLeftContains("array<string", "Unbalanced")
    expectLeftContains("map<string, int", "Unbalanced")
    expectLeftContains("map<string, (int>", "Unbalanced")
    expectLeftContains("array<decimal(10,2>>", "Unbalanced")
  }

  test("parseDataType - unsupported and empty inputs") {
    expectLeftContains(null.asInstanceOf[String], "cannot be null or empty")
    expectLeftContains("   ", "cannot be null or empty")
    expectLeftContains("bad_type", "Unsupported data type")
  }
}
