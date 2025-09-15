package com.snowflake.snowpark.internal

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

import java.util.Locale
import scala.annotation.tailrec
import scala.util.Try
import scala.util.matching.Regex

private[snowpark] object DataTypeParser {

  /**
   * Parses a data type from its string representation.
   *
   * It handles both simple types (e.g., `string`, `int`, `boolean`) that can be resolved via direct
   * lookup, and complex types (e.g., `decimal(10,2)`, `array<string>`, `map<string, int>`) that
   * require pattern matching.
   *
   * @param input
   *   The string representation of the data type to parse.
   * @return
   *   Either a [[ParseError]] if parsing fails, or the corresponding [[DataType]] instance.
   */
  def parseDataType(input: String): Either[ParseError, DataType] = {
    this.normalize(input) match {
      case None | Some("") => Left(EmptyInput())
      case Some(normalizedInput) =>
        this.StringToDataTypeMap.get(normalizedInput) match {
          case Some(dataType) => Right(dataType)
          case None => this.parseComplexType(normalizedInput, input)
        }
    }
  }

  /**
   * Base trait for all parsing errors that can occur during data type parsing.
   */
  sealed trait ParseError {

    /**
     * Returns a formatted error message describing the parsing failure.
     *
     * @return
     *   A string containing the error message.
     */
    def format: String
  }

  /**
   * Error indicating that the input string for parsing is null or empty.
   */
  private[snowpark] case class EmptyInput() extends ParseError {
    def format: String = "Type definition cannot be null or empty"
  }

  /**
   * Error indicating that the input string represents an unsupported or unrecognized data type.
   *
   * @param input
   *   The input string that could not be recognized.
   */
  private[snowpark] case class UnsupportedType(input: String) extends ParseError {
    def format: String = s"Unsupported data type: '$input'"
  }

  /**
   * Error indicating that the input string has a malformed structure for the expected data type
   * format.
   *
   * This error is returned when the input string partially matches a known type pattern but has
   * structural issues (e.g., missing parameters, incorrect syntax).
   *
   * @param input
   *   The malformed input string.
   * @param expected
   *   A description of the expected format.
   */
  private[snowpark] case class MalformedType(input: String, expected: String) extends ParseError {
    def format: String = s"Malformed type '$input'. Expected: $expected"
  }

  /**
   * Error indicating that an integer value could not be parsed from the input string.
   *
   * @param input
   *   The input string that caused the parsing error.
   * @param reason
   *   A specific description of what made the integer invalid.
   */
  private[snowpark] case class InvalidInteger(input: String, reason: String) extends ParseError {
    def format: String = s"Invalid integer '$input': $reason"
  }

  /**
   * Default [[DecimalType]] instance used when no precision or scale parameters are specified.
   */
  private val DefaultDecimalType = DecimalType(DecimalType.MAX_PRECISION, 0)

  /**
   * Mapping of normalized string representations to corresponding [[DataType]] instances.
   *
   * This map is used for direct lookups of simple data types that do not require parameterization
   * or complex parsing logic.
   */
  private val StringToDataTypeMap: Map[String, DataType] = Map(
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

    // Text and binary types
    "string" -> StringType,
    "binary" -> BinaryType,

    // Boolean types
    "boolean" -> BooleanType,

    // Date and time types
    "date" -> DateType,
    "time" -> TimeType,
    "timestamp" -> TimestampType,

    // Semi-structured types
    "variant" -> VariantType,
    "object" -> MapType(StringType, VariantType),

    // Spatial types
    "geography" -> GeographyType,
    "geometry" -> GeometryType)

  private object TypePatterns {

    /**
     * Regex pattern for matching array type declarations.
     *
     * Matches strings like `array<string>`, `ARRAY<DECIMAL(10,2)>`, etc. Captures the inner element
     * type specification.
     */
    val ArrayPattern: Regex = """(?i)^array\s*<\s*(.+?)\s*>$""".r

    /**
     * Regex pattern for matching decimal/number/numeric type declarations with parameters.
     *
     * Matches strings like `decimal(10)`, `NUMBER(10,2)`, `numeric(5, 3)`, etc. Captures the
     * precision (required) and scale (optional) parameters. Only accepts valid integers.
     */
    val DecimalPattern: Regex =
      """(?i)^(decimal|number|numeric)\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\)\s*$""".r

    /**
     * Regex pattern for matching map type declarations.
     *
     * Matches strings like `map<string, int>`, `MAP<DECIMAL(10,2), ARRAY<STRING>>`, etc. Captures
     * the inner key-value type specification.
     */
    val MapPattern: Regex = """(?i)^map\s*<\s*(.+)\s*>$""".r
  }

  private object DecimalExtractor {

    /**
     * Extracts the precision and optional scale from a decimal type string.
     *
     * @param s
     *   The input string representing a decimal type.
     * @return
     *   An [[Option]] containing a tuple of precision and optional scale if the input matches the
     *   pattern.
     */
    def unapply(s: String): Option[(String, Option[String])] = {
      TypePatterns.DecimalPattern.findFirstMatchIn(s).map { m =>
        (m.group(2), Option(m.group(3)))
      }
    }
  }

  private object ArrayExtractor {

    /**
     * Extracts the element type from an array type string.
     *
     * @param s
     *   The input string representing an array type.
     * @return
     *   An [[Option]] containing the element type if the input matches the pattern.
     */
    def unapply(s: String): Option[String] = {
      TypePatterns.ArrayPattern.findFirstMatchIn(s).map(_.group(1))
    }
  }

  private object MapExtractor {

    /**
     * Extracts the key-value type specification from a map type string.
     *
     * @param s
     *   The input string representing a map type.
     * @return
     *   An [[Option]] containing the key-value type specification if the input matches the pattern.
     */
    def unapply(s: String): Option[String] = {
      TypePatterns.MapPattern.findFirstMatchIn(s).map(_.group(1))
    }
  }

  /**
   * Immutable state tracker for bracket nesting during parsing operations.
   *
   * This case class maintains the current nesting depth for different bracket types and the current
   * parsing position.
   *
   * The state tracking is needed for proper parsing of nested complex types such as `map<string,
   * array<decimal(10,2)>>` where brackets at different levels have different semantic meanings.
   *
   * @param angleDepth
   *   Current nesting depth of angle brackets (<>), used for arrays and maps.
   * @param parenDepth
   *   Current nesting depth of parentheses (()), used for decimal parameters.
   * @param position
   *   Current character position in the string being parsed.
   */
  private case class BracketState(angleDepth: Int = 0, parenDepth: Int = 0, position: Int = 0) {

    /**
     * Checks if the parser is currently at the top level (not nested within any brackets).
     *
     * @return
     *   `true` if the parser is not nested within any angle or parenthesis brackets; `false`
     *   otherwise.
     */
    def isAtTopLevel: Boolean = angleDepth == 0 && parenDepth == 0

    /**
     * Processes a single character and returns the updated bracket state.
     *
     * This method handles bracket characters by updating the appropriate depth counter and always
     * increments the position.
     *
     * @param char
     *   The character to process.
     * @return
     *   A new [[BracketState]] reflecting the effect of processing the character.
     */
    def processChar(char: Char): BracketState = {
      val nextPosition = position + 1
      char match {
        case '<' => copy(angleDepth = angleDepth + 1, position = nextPosition)
        case '>' => copy(angleDepth = angleDepth - 1, position = nextPosition)
        case '(' => copy(parenDepth = parenDepth + 1, position = nextPosition)
        case ')' => copy(parenDepth = parenDepth - 1, position = nextPosition)
        case _ => copy(position = nextPosition)
      }
    }
  }

  /**
   * Parses complex data types from normalized input strings.
   *
   * @param normalized
   *   The normalized type string.
   * @param original
   *   The original input string preserved for error reporting.
   * @return
   *   Either a [[ParseError]] if parsing fails, or the corresponding [[DataType]] instance.
   */
  private def parseComplexType(
      normalized: String,
      original: String): Either[ParseError, DataType] = {
    normalized match {
      case DecimalExtractor(precision, scale) =>
        this.parseDecimalType(precision, scale, original)
      case ArrayExtractor(elementType) =>
        this.parseArrayType(elementType)
      case MapExtractor(innerTypes) =>
        this.parseMapType(innerTypes, original)
      case _ =>
        Left(UnsupportedType(original))
    }
  }

  /**
   * Parses a decimal type with precision and optional scale.
   *
   * @param precisionStr
   *   String representation of precision.
   * @param scaleStr
   *   Optional string representation of scale.
   * @param original
   *   Original type string for error reporting.
   * @return
   *   Either a [[DecimalType]] or [[ParseError]].
   */
  private def parseDecimalType(
      precisionStr: String,
      scaleStr: Option[String],
      original: String): Either[ParseError, DataType] = {
    val scaleIfEmpty = Right(0)
    for {
      precision <- this.parseIntSafe(precisionStr, original)
      scale <- scaleStr.fold[Either[ParseError, Int]](scaleIfEmpty)(this.parseIntSafe(_, original))
    } yield DecimalType(precision, scale)
  }

  /**
   * Parses an array type by recursively parsing its element type.
   *
   * @param elementType
   *   String representation of the array's element type.
   * @return
   *   Either a [[ArrayType]] containing the parsed element type or a [[ParseError]].
   */
  private def parseArrayType(elementType: String): Either[ParseError, DataType] = {
    this.parseDataType(elementType).map(ArrayType)
  }

  /**
   * Parses a map type by splitting the inner types at the top-level delimiter and recursively
   * parsing the key and value types.
   *
   * @param innerTypes
   *   String representation of the map's inner key-value type specification.
   * @param original
   *   Original type string for error reporting.
   * @return
   *   Either a [[MapType]] containing the parsed key and value types or a [[ParseError]].
   */
  private def parseMapType(innerTypes: String, original: String): Either[ParseError, DataType] = {
    this.splitAtTopLevelDelimiter(innerTypes, delimiter = ',') match {
      case Some((keyStr, valueStr)) if keyStr.nonEmpty && valueStr.nonEmpty =>
        for {
          keyType <- this.parseDataType(keyStr)
          valueType <- this.parseDataType(valueStr)
        } yield MapType(keyType, valueType)
      case _ =>
        Left(MalformedType(original, expected = "map<key_type, value_type>"))
    }
  }

  /**
   * Normalizes input strings by converting to lowercase and trimming whitespace.
   *
   * @param input
   *   The input string to normalize.
   * @return
   *   An [[Option]] containing the normalized string, or [[None]] if the input was null.
   */
  private def normalize(input: String): Option[String] =
    Option(input).map(_.toLowerCase(Locale.ROOT).trim)

  /**
   * Safely parses a string to an integer with error handling.
   *
   * This method attempts to convert a string representation to an integer value, wrapping any
   * parsing failures in a [[ParseError]] for consistent error handling throughout the parser.
   *
   * @param str
   *   The string to parse as an integer.
   * @param original
   *   The original input string for error reporting context.
   * @return
   *   Either a [[ParseError]] if the string cannot be parsed as an integer, or the successfully
   *   parsed integer value.
   */
  private def parseIntSafe(str: String, original: String): Either[ParseError, Int] =
    Try(str.toInt).toEither.left.map(_ =>
      InvalidInteger(original, s"'$str' is not a valid integer"))

  /**
   * Splits a string at the first top-level delimiter occurrence.
   *
   * "Top-level" means the delimiter is not nested within angle brackets or parentheses. This is
   * essential for parsing map types like `map<decimal(10,2), array<string>>` where the comma
   * separating key and value types should not be confused with commas within the decimal parameters
   * or other nested structures.
   *
   * The method uses tail recursion to efficiently traverse the string while maintaining bracket
   * depth state, ensuring O(n) performance with constant stack space.
   *
   * @param s
   *   The string to split.
   * @param delimiter
   *   The delimiter character to find.
   * @return
   *   An [[Option]] containing a tuple of the left and right substrings if the delimiter is found
   *   at the top level; [[None]] if no such delimiter exists.
   */
  private def splitAtTopLevelDelimiter(s: String, delimiter: Char): Option[(String, String)] = {
    @tailrec
    def findDelimiterPosition(chars: List[Char], state: BracketState): Option[Int] = {
      chars match {
        case Nil => None
        case head :: tail =>
          if (head == delimiter && state.isAtTopLevel) {
            Some(state.position)
          } else {
            findDelimiterPosition(tail, state.processChar(head))
          }
      }
    }

    findDelimiterPosition(s.toList, BracketState()).map { pos =>
      val (left, right) = s.splitAt(pos)
      (left.trim, right.drop(1).trim) // drop(1) to skip the delimiter character
    }
  }
}
