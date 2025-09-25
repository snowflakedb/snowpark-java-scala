package com.snowflake.snowpark

import com.snowflake.snowpark.internal.DataTypeParser
import com.snowflake.snowpark.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  GeographyType,
  GeometryType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType,
  TimeType,
  TimestampType,
  VariantType
}
import com.snowflake.snowpark.internal.DataTypeParser.{
  EmptyInput,
  MalformedType,
  ParseError,
  UnsupportedType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.reflect.ClassTag

/**
 * Test suite for the [[DataTypeParser]] object.
 */
class DataTypeParserSuite extends AnyFunSuite with TableDrivenPropertyChecks {

  private def expectRight(input: String, expected: DataType): Unit = {
    DataTypeParser.parseDataType(input) match {
      case Right(dt) => assert(dt == expected, s"$input parsed to $dt, expected $expected")
      case Left(err) => fail(s"Expected Right for '$input' but got error: ${err.format}")
    }
  }

  private def expectLeft[E <: ParseError](input: String)(implicit ct: ClassTag[E]): Unit = {
    DataTypeParser.parseDataType(input) match {
      case Left(_: E) => // Success
      case Left(other) =>
        val expectedErrorType = ct.runtimeClass.getSimpleName
        val actualErrorType = other.getClass.getSimpleName
        fail(s"Expected error of type $expectedErrorType for '$input' but got $actualErrorType")
      case Right(dt) => fail(s"Expected Left for '$input' but got $dt")
    }
  }

  test("parseDataType - should correctly parse all simple data types") {
    val simpleTypes = Table(
      ("input", "expected"),
      ("byte", ByteType),
      ("byteint", ByteType),
      ("tinyint", ByteType),
      ("short", ShortType),
      ("smallint", ShortType),
      ("int", IntegerType),
      ("integer", IntegerType),
      ("long", LongType),
      ("bigint", LongType),
      ("float", FloatType),
      ("double", DoubleType),
      ("decimal", DecimalType(38, 0)),
      ("number", DecimalType(38, 0)),
      ("numeric", DecimalType(38, 0)),
      ("string", StringType),
      ("binary", BinaryType),
      ("date", DateType),
      ("time", TimeType),
      ("timestamp", TimestampType),
      ("variant", VariantType),
      ("object", MapType(StringType, VariantType)),
      ("boolean", BooleanType),
      ("geography", GeographyType),
      ("geometry", GeometryType))

    forAll(simpleTypes) { (input: String, expected: DataType) =>
      expectRight(input, expected)
    }
  }

  test("parseDataType - should handle case insensitivity and whitespace trimming") {
    val caseInsensitivityTypes = Table(
      ("input", "expected"),
      ("   BoOlEaN  ", BooleanType),
      ("StRiNg", StringType),
      ("  DECIMAL  ", DecimalType(38, 0)),
      ("array<STRING>", ArrayType(StringType)),
      (" \t array \n < \r string \t > \n ", ArrayType(StringType)))

    forAll(caseInsensitivityTypes) { (input: String, expected: DataType) =>
      expectRight(input, expected)
    }
  }

  test("parseDataType - should correctly parse decimal types with and without scale") {
    val validDecimalTypes = Table(
      ("input", "expected"),
      ("decimal(10)", DecimalType(10, 0)),
      ("number(38)", DecimalType(38, 0)),
      ("numeric(5)", DecimalType(5, 0)),
      ("decimal(10,2)", DecimalType(10, 2)),
      ("number(18,0) ", DecimalType(18, 0)),
      ("numeric(5,2)", DecimalType(5, 2)))

    forAll(validDecimalTypes) { (input: String, expected: DataType) =>
      expectRight(input, expected)
    }
  }

  test("parseDataType - should reject invalid decimal parameter formats") {
    val invalidDecimalTypes = Table(
      "input",
      "decimal()",
      "decimal(10,-1)",
      "decimal(-10,1)",
      "decimal(abc)",
      "decimal(10,xyz)",
      "decimal(10,2,3)",
      "numeric(1.5)")

    forAll(invalidDecimalTypes) { input =>
      expectLeft[UnsupportedType](input)
    }
  }

  test("parseDataType - should correctly parse array types") {
    val arrayTypes = Table(
      ("input", "expected"),
      ("array<string>", ArrayType(StringType)),
      ("array< decimal(10,2) >", ArrayType(DecimalType(10, 2))),
      ("array<array<int>>", ArrayType(ArrayType(IntegerType))),
      ("array<map<string, array<int>>>", ArrayType(MapType(StringType, ArrayType(IntegerType)))))

    forAll(arrayTypes) { (input: String, expected: DataType) =>
      expectRight(input, expected)
    }
  }

  test("parseDataType - should correctly parse map types") {
    val mapTypes = Table(
      ("input", "expected"),
      ("map<string, int>", MapType(StringType, IntegerType)),
      ("map<decimal(10,2), array<string>>", MapType(DecimalType(10, 2), ArrayType(StringType))),
      (
        "map<array<decimal(10,2)>, array<decimal(3,0)>>",
        MapType(ArrayType(DecimalType(10, 2)), ArrayType(DecimalType(3, 0)))),
      (
        "map<string, map<int, array<decimal(5,2)>>>",
        MapType(StringType, MapType(IntegerType, ArrayType(DecimalType(5, 2))))))

    forAll(mapTypes) { (input: String, expected: DataType) =>
      expectRight(input, expected)
    }
  }

  test("parseDataType - should handle empty and null inputs") {
    val emptyTypes = Table(null.asInstanceOf[String], "", "   ", "\t\n\r ")

    forAll(emptyTypes) { input =>
      expectLeft[EmptyInput](input)
    }
  }

  test("parseDataType - should reject malformed map types") {
    val malformedMapTypes = Table("map<string>", "map<string,>", "map<,string>", "map<,>")

    forAll(malformedMapTypes) { input =>
      expectLeft[MalformedType](input)
    }
  }

  test("parseDataType - should reject unbalanced brackets") {
    val unbalancedBracketsTypes = Table(
      "array<string",
      "array<string>>",
      "map<string, int",
      "map<string, int>>",
      "array<decimal(10,2>>",
      "decimal(10,2))",
      "array<<string>",
      "map<string, (int>")

    forAll(unbalancedBracketsTypes) { input =>
      expectLeft[UnsupportedType](input)
    }
  }

  test("parseDataType - should reject unsupported type names") {
    expectLeft[UnsupportedType]("bad_type")
  }
}
