package com.snowflake.snowpark.internal.analyzer
import com.snowflake.snowpark.internal.Utils

import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}
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

  /**
   * Format a [[Duration]] as a Snowflake day-time interval string (without the INTERVAL keyword),
   * suitable for CAST(... AS INTERVAL DAY TO SECOND).
   *
   * Uses explicit day/h/m/s/nano breakdown because the library still targets Java 8
   * ([[Duration.toHoursPart]] and related APIs are Java 9+).
   */
  private[snowpark] def formatDuration(duration: Duration): String = {
    val negative = duration.isNegative
    val abs = duration.abs()
    val days = abs.toDays
    val remSeconds = abs.minusDays(days).getSeconds
    val hours = remSeconds / 3600
    val minutes = (remSeconds % 3600) / 60
    val seconds = remSeconds % 60
    val nanos = abs.getNano
    val sign = if (negative) "-" else ""
    f"$sign$days $hours%02d:$minutes%02d:$seconds%02d.$nanos%09d"
  }

  /**
   * Convert a [[Duration]] to a Snowflake INTERVAL DAY TO SECOND SQL literal.
   */
  private[analyzer] def durationToSql(duration: Duration): String =
    s"INTERVAL '${formatDuration(duration)}' DAY TO SECOND"

  /**
   * Format a [[Period]] as a Snowflake year-month interval string (without the INTERVAL keyword).
   * Days must be zero — year-month intervals do not carry a day component.
   */
  private[snowpark] def formatPeriod(period: Period): String = {
    if (period.getDays != 0) {
      throw new UnsupportedOperationException(
        s"Year-month interval Period must have days=0, got: $period")
    }
    // normalized() folds months into years (|months| < 12) with a consistent sign.
    val normalized = period.normalized()
    val negative = normalized.isNegative
    val abs = if (negative) normalized.negated() else normalized
    val sign = if (negative) "-" else ""
    s"$sign${abs.getYears}-${abs.getMonths}"
  }

  /**
   * Convert a [[Period]] to a Snowflake INTERVAL YEAR TO MONTH SQL literal.
   */
  private[analyzer] def periodToSql(period: Period): String =
    s"INTERVAL '${formatPeriod(period)}' YEAR TO MONTH"

  /*
   * Convert a value with DataType to a snowflake compatible sql
   */
  private[analyzer] def toSql(value: Any, dataType: Option[DataType]): String = {
    dataType match {
      case None => "NULL"
      case Some(dt) =>
        (value, dt) match {
          case (_, _: ArrayType | _: MapType | _: StructType | GeographyType | GeometryType)
              if value == null =>
            "NULL"
          case (_, IntegerType) if value == null => "NULL :: int"
          case (_, ShortType) if value == null => "NULL :: smallint"
          case (_, ByteType) if value == null => "NULL :: tinyint"
          case (_, LongType) if value == null => "NULL :: bigint"
          case (_, FloatType) if value == null => "NULL :: float"
          case (_, StringType) if value == null => "NULL :: string"
          case (_, DoubleType) if value == null => "NULL :: double"
          case (_, BooleanType) if value == null => "NULL :: boolean"
          case (_, BinaryType) if value == null => "NULL :: binary"
          case (_, DayTimeIntervalType) if value == null => "NULL :: INTERVAL DAY TO SECOND"
          case (_, YearMonthIntervalType) if value == null => "NULL :: INTERVAL YEAR TO MONTH"
          case _ if value == null => "NULL"
          case (v: String, StringType) => stringToSql(v)
          case (v: Byte, ByteType) => v + s" :: tinyint"
          case (v: Short, ShortType) => v + s" :: smallint"
          // SNOW-3649401: must match v: Int (not v: Any) to prevent SQL injection
          case (v: Int, IntegerType) => v + s" :: int"
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
          case (v: Duration, DayTimeIntervalType) => durationToSql(v)
          case (v: Period, YearMonthIntervalType) => periodToSql(v)
          case (v: String, DayTimeIntervalType) =>
            s"${stringToSql(v)} :: INTERVAL DAY TO SECOND"
          case (v: String, YearMonthIntervalType) =>
            s"${stringToSql(v)} :: INTERVAL YEAR TO MONTH"
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
        case DayTimeIntervalType => "INTERVAL '1 01:01:01.0001' DAY TO SECOND"
        case YearMonthIntervalType => "INTERVAL '1-0' YEAR TO MONTH"
        case _: ArrayType => "[]::" + convertToSFType(dataType)
        case _: MapType => "{}::" + convertToSFType(dataType)
        case VariantType => "to_variant(0)"
        case GeographyType => "to_geography('POINT(-122.35 37.55)')"
        case GeometryType => "to_geometry('POINT(-122.35 37.55)')"
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported data type: ${dataType.typeName}")
      }
    }

  private[analyzer] def toSqlWithoutCast(value: Any, dataType: DataType): String =
    dataType match {
      case _ if value == null => "NULL"
      case StringType => s"""'$value'"""
      case DayTimeIntervalType =>
        value match {
          case d: Duration => durationToSql(d)
          case s: String => s
          case other => other.toString
        }
      case YearMonthIntervalType =>
        value match {
          case p: Period => periodToSql(p)
          case s: String => s
          case other => other.toString
        }
      case _ => value.toString
    }
}
