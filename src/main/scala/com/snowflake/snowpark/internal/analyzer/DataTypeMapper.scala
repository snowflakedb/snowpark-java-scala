package com.snowflake.snowpark.internal.analyzer
import com.snowflake.snowpark.internal.Utils

import java.sql.{Date, Timestamp}
import java.util.TimeZone
import java.math.{BigDecimal => JBigDecimal}

import com.snowflake.snowpark.types._
import com.snowflake.snowpark.types.convertToSFType
import javax.xml.bind.DatatypeConverter
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat

object DataTypeMapper {
  // milliseconds per day
  private val MILLIS_PER_DAY = 24 * 3600 * 1000L
  // microseconds per millisecond
  private val MICROS_PER_MILLIS = 1000L
  private[analyzer] def stringToSql(str: String): String =
    // Escapes all backslashes, single quotes and new line.
    "'" + str
      .replaceAll("\\\\", "\\\\\\\\")
      .replaceAll("'", "''")
      .replaceAll("\n", "\\\\n") + "'"

  /*
   * Convert a value with DataType to a snowflake compatible sql
   */
  private[analyzer] def toSql(value: Any, dataType: Option[DataType]): String = {
    dataType match {
      case None => "NULL"
      case Some(dt) =>
        (value, dt) match {
          case (_, _: ArrayType | _: MapType | _: StructType | GeographyType| GeometryType)
            if value == null => "NULL"
          case (_, IntegerType) if value == null => "NULL :: int"
          case (_, ShortType) if value == null => "NULL :: smallint"
          case (_, ByteType) if value == null => "NULL :: tinyint"
          case (_, LongType) if value == null => "NULL :: bigint"
          case (_, FloatType) if value == null => "NULL :: float"
          case (_, StringType) if value == null => "NULL :: string"
          case (_, DoubleType) if value == null => "NULL :: double"
          case (_, BooleanType) if value == null => "NULL :: boolean"
          case (_, BinaryType) if value == null => "NULL :: binary"
          case _ if value == null => "NULL"
          case (v: String, StringType) => stringToSql(v)
          case (v: Byte, ByteType) => v + s" :: tinyint"
          case (v: Short, ShortType) => v + s" :: smallint"
          case (v: Any, IntegerType) => v + s" :: int"
          case (v: Long, LongType) => v + s" :: bigint"
          case (v: Boolean, BooleanType) => s"$v :: boolean"
          // Float type doesn't have a suffix
          case (v: Float, FloatType) =>
            val castedValue = v match {
              case _ if v.isNaN => "'NaN'"
              case Float.PositiveInfinity => "'Infinity'"
              case Float.NegativeInfinity => "'-Infinity'"
              case _ => s"'$v'"
            }
            s"$castedValue :: FLOAT"
          case (v: Double, DoubleType) =>
            v match {
              case _ if v.isNaN => "'NaN'"
              case Double.PositiveInfinity => "'Infinity'"
              case Double.NegativeInfinity => "'-Infinity'"
              case _ => v + "::DOUBLE"
            }
          case (v: BigDecimal, t: DecimalType) => v + s" :: ${number(t.precision, t.scale)}"
          case (v: JBigDecimal, t: DecimalType) => v + s" :: ${number(t.precision, t.scale)}"
          case (v: Int, DateType) =>
            s"DATE '${SnowflakeDateTimeFormat
              .fromSqlFormat(Utils.DateInputFormat)
              .format(new Date(v * MILLIS_PER_DAY), TimeZone.getTimeZone("GMT"))}'"
          case (v: Long, TimestampType) =>
            s"TIMESTAMP '${SnowflakeDateTimeFormat
              .fromSqlFormat(Utils.TimestampInputFormat)
              .format(new Timestamp(v / MICROS_PER_MILLIS), TimeZone.getDefault, 3)}'"
          case (v: Array[Byte], BinaryType) =>
            s"'${DatatypeConverter.printHexBinary(v)}' :: binary"
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported datatype by ToSql: ${value.getClass.getName} => $dataType")
        }
    }

  }

  private[analyzer] def schemaExpression(dataType: DataType, isNullable: Boolean): String =
    if (isNullable) {
      dataType match {
        case GeographyType => "TRY_TO_GEOGRAPHY(NULL)"
        case GeometryType => "TRY_TO_GEOMETRY(NULL)"
        case ArrayType(_) => "PARSE_JSON('NULL')::ARRAY"
        case _ => "NULL :: " + convertToSFType(dataType)
      }
    } else {
      dataType match {
        case _: NumericType => "0 :: " + convertToSFType(dataType)
        case StringType => "'a' :: STRING"
        case BinaryType => "to_binary(hex_encode(1))"
        case BooleanType => "true"
        case DateType => "date('2020-9-16')"
        case TimeType => "to_time('04:15:29.999')"
        case TimestampType => "to_timestamp_ntz('2020-09-16 06:30:00')"
        case _: ArrayType => "to_array(0)"
        case _: MapType => "to_object(parse_json('0'))"
        case VariantType => "to_variant(0)"
        case GeographyType => "to_geography('POINT(-122.35 37.55)')"
        case GeographyType => "to_geometry('POINT(-122.35 37.55)')"
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported data type: ${dataType.typeName}")
      }
    }

  private[analyzer] def toSqlWithoutCast(value: Any, dataType: DataType): String =
    dataType match {
      case _ if value == null => "NULL"
      case StringType => s"""'$value'"""
      case _ => value.toString
    }
}
