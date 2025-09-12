package com.snowflake.snowpark.types

import java.util.Locale
import scala.annotation.tailrec
import scala.util.{Either, Left, Right, Try}
import scala.util.matching.Regex

private[snowpark] object DataTypeParser {

  sealed trait ParseError {
    def format: String
  }

  private case class EmptyInput() extends ParseError {
    def format: String = "Type definition cannot be null or empty"
  }

  private case class UnsupportedType(input: String) extends ParseError {
    def format: String = s"Unsupported data type: '$input'"
  }

  private case class MalformedType(input: String, expected: String) extends ParseError {
    def format: String = s"Malformed type '$input'. Expected: $expected"
  }

  private case class UnbalancedBrackets(input: String, bracketType: String, pos: Int)
      extends ParseError {
    def format: String = s"Unbalanced $bracketType at position $pos: '$input'"
  }

  private case class InvalidDecimal(input: String, reason: String) extends ParseError {
    def format: String = s"Invalid decimal '$input': $reason"
  }

  /**
   * Compiled regex patterns for complex data types.
   */
  private object TypePatterns {
    val ArrayPattern: Regex = """(?i)^array\s*<\s*(.+?)\s*>$""".r
    val DecimalPattern: Regex =
      """(?i)^(decimal|number|numeric)\s*\(\s*([^,\s)]+)(?:\s*,\s*([^,\s)]+))?\s*\)\s*$""".r
    val MapPattern: Regex = """(?i)^map\s*<\s*(.+)\s*>$""".r
  }

  /**
   * Bracket types for validation.
   */
  private object BracketTypes {
    val AngleBrackets = "angle brackets <>"
    val Parentheses = "parentheses ()"
  }

  /**
   * Default decimal type when no parameters are specified.
   */
  private val DefaultDecimalType = DecimalType(DecimalType.MAX_PRECISION, 0)

  /**
   * Registry mapping normalized type names to their corresponding [[DataType]] instances.
   *
   * This map is used for fast lookup of simple, non-parameterized types.
   */
  private val SimpleTypeRegistry: Map[String, DataType] = Map(
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

    // Date and time types
    "date" -> DateType,
    "time" -> TimeType,
    "timestamp" -> TimestampType,

    // Semi-structured types
    "variant" -> VariantType,
    "object" -> MapType(StringType, VariantType),
    "boolean" -> BooleanType,

    // Spatial types
    "geography" -> GeographyType,
    "geometry" -> GeometryType
  )

  /**
   * Normalizes input string by trimming whitespace and converting to lowercase.
   *
   * @param input
   *   The string to normalize.
   * @return
   *   The normalized string.
   */
  private def normalize(input: String): String =
    Option(input).map(_.toLowerCase(Locale.ROOT).trim).getOrElse("")

  /**
   * Parses a data type from its string representation.
   *
   * This method handles both simple types (e.g., `string`, `int`, `boolean`) and complex types
   * (e.g., `decimal(10,2)`, `array<string>`, `map<string, int>`).
   *
   * @param input
   *   The string representation of the data type to parse.
   * @return
   *   Either a [[ParseError]] if parsing fails, or the corresponding [[DataType]] instance.
   */
  def parseDataType(input: String): Either[ParseError, DataType] = {
    Option(input).map(_.trim).filter(_.nonEmpty) match {
      case None | Some("") => Left(EmptyInput())
      case Some(validInput) =>
        val normalized = normalize(validInput)
        SimpleTypeRegistry.get(normalized) match {
          case Some(dataType) => Right(dataType)
          case None => parseComplexType(normalized, validInput)
        }
    }
  }

  /**
   * Parses complex data types from normalized input strings.
   *
   * Handles parameterized types such as decimal, array, and map by matching against custom
   * extractors.
   *
   * @param normalized
   *   The normalized type string.
   * @param original
   *   The original input string for error reporting.
   * @return
   *   Either a [[ParseError]] or the parsed [[DataType]].
   */
  private def parseComplexType(
      normalized: String,
      original: String): Either[ParseError, DataType] = {
    // If input appears to use brackets/parentheses, first validate their balance
    // so we can surface precise UnbalancedBrackets errors instead of generic unsupported types.
    val containsBrackets = original.contains('<') || original.contains('>') || original.contains(
      '(') || original.contains(')')
    if (containsBrackets) {
      validateBrackets(original) match {
        case Left(err) => return Left(err)
        case Right(_) => // continue
      }
    }

    normalized match {
      case DecimalExtractor(precision, scale) =>
        parseDecimalType(precision, scale, original)
      case ArrayExtractor(elementType) =>
        parseArrayType(elementType, original)
      case MapExtractor(innerTypes) =>
        parseMapType(innerTypes, original)
      case _ =>
        Left(UnsupportedType(original))
    }
  }

  /**
   * Extractor object for decimal type strings.
   */
  private object DecimalExtractor {

    /**
     * Extracts precision and optional scale from decimal type strings.
     *
     * Matches strings like "decimal(10)", "decimal(10, 2)", etc. and extracts the precision
     * (required) and scale (optional) parameters.
     *
     * @param s
     *   The normalized decimal type string to match against.
     * @return
     *   Some((precision, scale)) if the string matches the decimal pattern, where precision is
     *   always present and scale is optional; None otherwise.
     */
    def unapply(s: String): Option[(String, Option[String])] = {
      TypePatterns.DecimalPattern.findFirstMatchIn(s).map { m =>
        (m.group(2), Option(m.group(3)))
      }
    }
  }

  /**
   * Extractor object for array type strings.
   */
  private object ArrayExtractor {

    /**
     * Extracts the element type from array type strings.
     *
     * Matches strings like "array<string>", "array<decimal(10, 2)>", etc. and extracts the inner
     * element type specification.
     *
     * @param s
     *   The normalized array type string to match against.
     * @return
     *   Some(elementType) if the string matches the array pattern, where elementType is the inner
     *   type specification; None otherwise.
     */
    def unapply(s: String): Option[String] = {
      TypePatterns.ArrayPattern.findFirstMatchIn(s).map(_.group(1))
    }
  }

  /**
   * Extractor object for map type strings.
   */
  private object MapExtractor {

    /**
     * Extracts the inner type specification from map type strings.
     *
     * Matches strings like "map<string, int>", "map<decimal(10, 2), array<string>>", etc. and
     * extracts the inner type specification containing both key and value types.
     *
     * @param s
     *   The normalized map type string to match against.
     * @return
     *   Some(innerTypes) if the string matches the map pattern, where innerTypes is the
     *   comma-separated key and value type specification; None otherwise.
     */
    def unapply(s: String): Option[String] = {
      TypePatterns.MapPattern.findFirstMatchIn(s).map(_.group(1))
    }
  }

  /**
   * Parses a decimal type from precision and scale strings.
   *
   * This method constructs a DecimalType from string representations of precision and scale
   * parameters. It validates that both parameters are valid integers and that they fall within the
   * acceptable ranges for decimal types in Snowflake.
   *
   * @param precisionStr
   *   The string representation of the decimal precision (total number of digits)
   * @param scaleStr
   *   The optional string representation of the decimal scale (number of digits after decimal
   *   point). If None, defaults to 0.
   * @param original
   *   The original input string for error reporting purposes
   * @return
   *   Either a ParseError if the parameters are invalid, or a DecimalType with the specified
   *   precision and scale
   */
  private def parseDecimalType(
      precisionStr: String,
      scaleStr: Option[String],
      original: String): Either[ParseError, DataType] = {
    for {
      precision <- parseIntSafe(precisionStr, original)
      scale <- scaleStr.fold[Either[ParseError, Int]](Right(0))(parseIntSafe(_, original))
      _ <- validateDecimalParameters(precision, scale, original)
    } yield DecimalType(precision, scale)
  }

  /**
   * Safely parses a string to an integer with error handling.
   *
   * This method attempts to convert a string representation to an integer value, wrapping any
   * parsing failures in a ParseError for consistent error handling throughout the parser.
   *
   * @param str
   *   The string to parse as an integer
   * @param original
   *   The original input string for error reporting context
   * @return
   *   Either a ParseError if the string cannot be parsed as an integer, or the successfully parsed
   *   integer value
   */
  private def parseIntSafe(str: String, original: String): Either[ParseError, Int] =
    Try(str.toInt).toEither.left.map(_ =>
      InvalidDecimal(original, s"'$str' is not a valid integer"))

  /**
   * Validates decimal type parameters for correctness and compliance with Snowflake constraints.
   *
   * This method performs comprehensive validation of decimal precision and scale parameters to
   * ensure they meet Snowflake's requirements:
   *   - Precision must be between 1 and the maximum allowed precision
   *   - Scale must be between 0 and the maximum allowed scale
   *   - Scale cannot exceed precision (as it represents digits after the decimal point)
   *
   * @param precision
   *   The total number of digits in the decimal number (must be > 0 and <= MAX_PRECISION)
   * @param scale
   *   The number of digits after the decimal point (must be >= 0, <= MAX_SCALE, and <= precision)
   * @param original
   *   The original input string for error reporting context
   * @return
   *   Either a ParseError describing the validation failure, or Unit if all parameters are valid
   */
  private def validateDecimalParameters(
      precision: Int,
      scale: Int,
      original: String): Either[ParseError, Unit] = {
    if (precision <= 0 || precision > DecimalType.MAX_PRECISION) {
      Left(
        InvalidDecimal(original, s"precision must be between 1 and ${DecimalType.MAX_PRECISION}"))
    } else if (scale < 0 || scale > DecimalType.MAX_SCALE) {
      Left(InvalidDecimal(original, s"scale must be between 0 and ${DecimalType.MAX_SCALE}"))
    } else if (scale > precision) {
      Left(InvalidDecimal(original, s"scale cannot exceed precision ($precision)"))
    } else {
      Right(())
    }
  }

  /**
   * @param elementTypeStr
   * @param original
   * @return
   */
  private def parseArrayType(
      elementTypeStr: String,
      original: String): Either[ParseError, DataType] = {
    for {
      _ <- validateBrackets(original)
      elementType <- parseDataType(elementTypeStr)
    } yield ArrayType(elementType)
  }

  /**
   * @param innerTypes
   * @param original
   * @return
   */
  private def parseMapType(innerTypes: String, original: String): Either[ParseError, DataType] = {
    validateBrackets(original).flatMap { _ =>
      splitAtTopLevelDelimiter(innerTypes, delimiter = ',') match {
        case Some((keyStr, valueStr)) if keyStr.nonEmpty && valueStr.nonEmpty =>
          for {
            keyType <- parseDataType(keyStr)
            valueType <- parseDataType(valueStr)
          } yield MapType(keyType, valueType)
        case _ =>
          Left(MalformedType(original, "map<key_type, value_type>"))
      }
    }
  }

  /**
   * Immutable state for tracking bracket nesting during parsing. Uses functional updates for
   * thread-safety and clarity.
   *
   * @param angleDepth
   * @param parenDepth
   * @param position
   */
  private case class BracketState(angleDepth: Int = 0, parenDepth: Int = 0, position: Int = 0) {
    def isAtTopLevel: Boolean = angleDepth == 0 && parenDepth == 0
    def isValid: Boolean = angleDepth >= 0 && parenDepth >= 0

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
   * Splits a string at the first top-level delimiter. Top-level means not nested within angle
   * brackets or parentheses.
   *
   * @param s
   *   The string to split
   * @param delimiter
   *   The delimiter character
   * @return
   *   Some((left, right)) if delimiter found, None otherwise
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
      (left.trim, right.drop(1).trim) // drop(1) to skip the delimiter
    }
  }

  /** Validates bracket balance */
  private def validateBrackets(dataType: String): Either[ParseError, Unit] = {
    @tailrec
    def validate(chars: List[Char], state: BracketState): Either[ParseError, Unit] = {
      chars match {
        case Nil =>
          // End of string - check final state
          if (state.angleDepth != 0) {
            Left(UnbalancedBrackets(dataType, BracketTypes.AngleBrackets, state.position))
          } else if (state.parenDepth != 0) {
            Left(UnbalancedBrackets(dataType, BracketTypes.Parentheses, state.position))
          } else {
            Right(())
          }

        case head :: tail =>
          val newState = state.processChar(head)
          if (!newState.isValid) {
            // Negative depth means closing bracket without opening
            val bracketType = if (newState.angleDepth < 0) {
              BracketTypes.AngleBrackets
            } else {
              BracketTypes.Parentheses
            }
            Left(UnbalancedBrackets(dataType, bracketType, state.position))
          } else {
            validate(tail, newState)
          }
      }
    }

    validate(dataType.toList, BracketState())
  }
}
