package com.snowflake.snowpark.types

import java.util.Locale
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.util.matching.Regex

private[snowpark] object DataTypeParser {

  /**
   * Base exception for all data type parsing errors. Provides detailed context about what went
   * wrong during parsing.
   */
  private sealed abstract class DataTypeParseException(
      message: String,
      val input: String,
      val position: Option[Int] = None,
      cause: Throwable = null)
      extends Exception(
        s"$message${position.map(p => s" at position $p").getOrElse("")}: '$input'",
        cause)

  /** Thrown when an unknown or unsupported data type is encountered */
  private final case class UnsupportedDataTypeException(
    dataType: String,
    suggestions: Seq[String] = Seq.empty) extends DataTypeParseException(s"Unsupported data type${if (suggestions.nonEmpty) s" (did you mean: ${suggestions.mkString(", ")}?)" else ""}", dataType)

  /** Thrown when the structure of a complex type is malformed */
  private final case class MalformedTypeException(
      dataType: String,
      expectedFormat: String,
      pos: Option[Int] = None)
      extends DataTypeParseException(
        s"Malformed type definition. Expected format: $expectedFormat",
        dataType,
        pos)

  /** Thrown when brackets or parentheses are mismatched */
  private final case class UnbalancedBracketsException(
      dataType: String,
      bracketType: String,
      pos: Int)
      extends DataTypeParseException(s"Unbalanced $bracketType", dataType, Some(pos))

  /** Thrown when decimal precision/scale values are invalid */
  private final case class InvalidDecimalParametersException(
      dataType: String,
      precision: String,
      scale: Option[String],
      error: String)
      extends DataTypeParseException(
        s"Invalid decimal parameters (precision=$precision${scale.map(s => s", scale=$s").getOrElse("")}): $error",
        dataType)

  private object Patterns {
    val Array: Regex = """(?i)^array\s*<\s*(.+?)\s*>$""".r
    val Decimal: Regex =
      """(?i)^(decimal|number|numeric)\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\)\s*$""".r
    val Map: Regex = """(?i)^map\s*<\s*(.+)\s*>$""".r
  }

  private val DefaultDecimalType = DecimalType(DecimalType.MAX_PRECISION, 0)

  /**
   * Mapping of simple type names to their corresponding [[DataType]] instances.
   */
  private val SimpleTypeMap: Map[String, DataType] = Map(
    // Integer types
    "byte" -> ByteType,
    "byteint" -> ByteType,
    "tinyint" -> ByteType,
    "short" -> ShortType,
    "smallint" -> ShortType,
    "int" -> IntegerType,
    "integer" -> IntegerType,
    "long" -> LongType,
    "bigint" -> LongType,

    // Floating point types
    "float" -> FloatType,
    "double" -> DoubleType,

    // Decimal types
    "decimal" -> DefaultDecimalType,
    "number" -> DefaultDecimalType,
    "numeric" -> DefaultDecimalType,

    // String and binary types
    "string" -> StringType,
    "binary" -> BinaryType,

    // Date/Time types
    "date" -> DateType,
    "time" -> TimeType,
    "timestamp" -> TimestampType,

    // Boolean type
    "boolean" -> BooleanType,

    // Semi-structured types
    "variant" -> VariantType,
    "object" -> MapType(StringType, VariantType),

    // Spatial types
    "geography" -> GeographyType,
    "geometry" -> GeometryType)

  private def normalizeDataType(dataType: String): String = dataType.toLowerCase(Locale.ROOT).trim

  /**
   * Parses a string representation of a data type into a [[DataType]] instance.
   *
   * This method handles both simple types (e.g., "string", "int", "boolean") and complex types
   * (e.g., "decimal(10,2)", "array<string>", "map<string,int>", "struct<name:string,age:int>").
   *
   * @param dataType
   *   The string representation of the data type to parse.
   * @return
   *   The corresponding [[DataType]] instance.
   * @throws MalformedTypeException
   *   If the input is null, empty, or malformed.
   * @throws UnsupportedDataTypeException
   *   If the data type is not supported.
   */
  def parseDataType(dataType: String): DataType = {
    if (dataType == null) {
      throw MalformedTypeException(dataType, "non-empty type definition")
    }

    val normalizedDataType = this.normalizeDataType(dataType)

    if (normalizedDataType.isEmpty) {
      throw MalformedTypeException(dataType, "non-empty type definition")
    }

    SimpleTypeMap.get(normalizedDataType) match {
      case Some(parsedDataType) => parsedDataType
      case None => parseComplexType(normalizedDataType, dataType)
    }
  }

  /**
   * Parses complex data types that require parameter parsing or nested type definitions.
   *
   * This method handles parameterized types such as decimal(precision,scale), array<elementType>,
   * map<keyType,valueType>, and struct<field1:type1,field2:type2>.
   *
   * @param normalizedDataType
   *   The normalized string representation of the data type.
   * @param originalDataType
   *   The original string representation for error reporting.
   * @return
   *   The corresponding [[DataType]] instance for the complex type.
   * @throws UnsupportedDataTypeException
   *   If the complex type pattern is not recognized.
   * @throws MalformedTypeException
   *   If the type definition is malformed (via delegate methods).
   * @throws InvalidDecimalParametersException
   *   If decimal parameters are invalid (via parseDecimalType).
   */
  private def parseComplexType(normalizedDataType: String, originalDataType: String): DataType = {
    normalizedDataType match {
      case Patterns.Decimal(_, precision, scale) =>
        parseDecimalType(precision, Option(scale), originalDataType)

      case Patterns.Array(arrayType) =>
        parseArrayType(arrayType, originalDataType)

      case Patterns.Map(mapType) =>
        parseMapType(mapType, originalDataType)

      case _ =>
        throw UnsupportedDataTypeException(originalDataType)
    }
  }

  private def parseDecimalType(
      precisionStr: String,
      scaleStr: Option[String],
      original: String): DecimalType = {
    Try {
      val precision = precisionStr.toInt
      val scale = scaleStr.map(_.toInt).getOrElse(0)

      if (precision <= 0 || precision > DecimalType.MAX_PRECISION) {
        throw InvalidDecimalParametersException(
          original,
          precisionStr,
          scaleStr,
          s"precision must be between 1 and ${DecimalType.MAX_PRECISION}")
      }

      if (scale < 0 || scale > DecimalType.MAX_SCALE || scale > precision) {
        throw InvalidDecimalParametersException(
          original,
          precisionStr,
          scaleStr,
          s"scale must be between 0 and precision ($precision)")
      }

      DecimalType(precision, scale)
    } match {
      case Success(decimalType) => decimalType
      case Failure(_: NumberFormatException) =>
        throw InvalidDecimalParametersException(
          original,
          precisionStr,
          scaleStr,
          "parameters must be valid integers")
      case Failure(e: DataTypeParseException) => throw e
      case Failure(NonFatal(e)) =>
        throw InvalidDecimalParametersException(
          original,
          precisionStr,
          scaleStr,
          s"unexpected error: ${e.getMessage}")
    }
  }

  /**
   * Parses an array type from its string representation.
   *
   * This method validates the bracket structure of the original type string and then
   * recursively parses the element type to create an ArrayType instance.
   *
   * @param arrayType
   *   The string representation of the array's element type (e.g., "int" for "array<int>").
   * @param originalDataType
   *   The original complete type string for error reporting (e.g., "array<int>").
   * @return
   *   An ArrayType instance with the parsed element type.
   * @throws DataTypeParseException
   *   If the bracket structure is invalid or the element type cannot be parsed.
   */
  private def parseArrayType(arrayType: String, originalDataType: String): ArrayType = {
    this.validateBrackets(originalDataType)
    ArrayType(this.parseDataType(arrayType))
  }

  /**
   * Parses a map type from its string representation.
   *
   * This method validates the bracket structure of the original type string and then
   * splits the map type string at the top-level comma delimiter to extract the key and value
   * type strings. Both key and value types are recursively parsed to create a MapType instance.
   *
   * @param mapType
   *   The string representation of the map's key and value types (e.g., "string, int" for "map<string, int>").
   * @param original
   *   The original complete type string for error reporting (e.g., "map<string, int>").
   * @return
   *   A MapType instance with the parsed key and value types.
   * @throws DataTypeParseException
   *   If the bracket structure is invalid, the key/value types cannot be parsed, or the format
   *   doesn't match the expected "map<key_type, value_type>" pattern.
   */
  private def parseMapType(mapType: String, original: String): MapType = {
    this.validateBrackets(original)
    this.splitAtTopLevelDelimiter(mapType, delimiter = ',') match {
      case Some((keyStr, valueStr)) if keyStr.nonEmpty && valueStr.nonEmpty =>
        val keyType = this.parseDataType(keyStr)
        val valueType = this.parseDataType(valueStr)
        MapType(keyType, valueType)
      case _ =>
        throw MalformedTypeException(original, "map<key_type, value_type>")
    }
  }

  /**
   * Tracks the nesting depth of brackets and the current position while parsing data type strings.
   *
   * This case class is used to maintain state during parsing operations that need to respect
   * bracket boundaries. It tracks the depth of angle brackets `< >` and parentheses `( )`
   * separately, along with the current character position in the string being parsed.
   *
   * @param angleDepth
   *   The current nesting depth of angle brackets (< >). Starts at 0.
   * @param parenDepth
   *   The current nesting depth of parentheses (( )). Starts at 0.
   * @param position
   *   The current character position in the string being parsed. Starts at 0.
   */
  private case class BracketState(angleDepth: Int = 0, parenDepth: Int = 0, position: Int = 0) {
    def isAtTopLevel: Boolean = angleDepth == 0 && parenDepth == 0

    def update(char: Char): BracketState = char match {
      case '<' => copy(angleDepth = angleDepth + 1, position = position + 1)
      case '>' => copy(angleDepth = angleDepth - 1, position = position + 1)
      case '(' => copy(parenDepth = parenDepth + 1, position = position + 1)
      case ')' => copy(parenDepth = parenDepth - 1, position = position + 1)
      case _ => copy(position = position + 1)
    }
  }

  /**
   * Split a string at the first occurrence of a delimiter at the top level.
   *
   * This method finds the first occurrence of the specified delimiter that is not nested within
   * angle brackets `< >` or parentheses `( )`. If such a delimiter is found, the string is split
   * into two parts at that position, with both parts trimmed of leading and trailing whitespace.
   *
   * @param s
   *   The string to split.
   * @param delimiter
   *   The character to split on.
   * @return
   *   Some((left, right)) if a top-level delimiter is found, None otherwise. Both left and right
   *   parts are trimmed of whitespace.
   */
  private def splitAtTopLevelDelimiter(s: String, delimiter: Char): Option[(String, String)] = {
    @tailrec
    def findSplit(state: BracketState): Option[Int] = {
      if (state.position >= s.length) {
        None
      } else {
        val char = s.charAt(state.position)
        if (char == delimiter && state.isAtTopLevel) {
          Some(state.position)
        } else {
          findSplit(state.update(char))
        }
      }
    }

    findSplit(BracketState()).map { splitAt =>
      (s.substring(0, splitAt).trim, s.substring(splitAt + 1).trim)
    }
  }

  /**
   * Validate that brackets are properly balanced in a data type string.
   *
   * Checks that angle brackets < > and parentheses ( ) are properly matched and balanced throughout
   * the data type definition. Throws an exception if any brackets are unbalanced or mismatched.
   *
   * @param dataType
   *   The data type string to validate.
   * @throws UnbalancedBracketsException
   *   If brackets are not properly balanced.
   */
  private def validateBrackets(dataType: String): Unit = {
    @tailrec
    def check(state: BracketState): Unit = {
      if (state.position >= dataType.length) {
        if (state.angleDepth != 0) {
          throw UnbalancedBracketsException(dataType, "angle brackets <>", state.position)
        }
        if (state.parenDepth != 0) {
          throw UnbalancedBracketsException(dataType, "parentheses ()", state.position)
        }
      } else {
        val newState = state.update(dataType.charAt(state.position))
        if (newState.angleDepth < 0) {
          throw UnbalancedBracketsException(dataType, "angle brackets <>", state.position)
        }
        if (newState.parenDepth < 0) {
          throw UnbalancedBracketsException(dataType, "parentheses ()", state.position)
        }
        check(newState)
      }
    }

    check(BracketState())
  }
}
