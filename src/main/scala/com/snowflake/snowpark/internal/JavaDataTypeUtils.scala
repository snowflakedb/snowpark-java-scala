package com.snowflake.snowpark.internal

import com.snowflake.snowpark.types._
import com.snowflake.snowpark_java.types.{
  ArrayType => JArrayType,
  BinaryType => JBinaryType,
  BooleanType => JBooleanType,
  ByteType => JByteType,
  DataType => JDataType,
  DataTypes => JDataTypes,
  DateType => JDateType,
  DecimalType => JDecimalType,
  DoubleType => JDoubleType,
  FloatType => JFloatType,
  GeographyType => JGeographyType,
  IntegerType => JIntegerType,
  LongType => JLongType,
  MapType => JMapType,
  ShortType => JShortType,
  StringType => JStringType,
  TimestampType => JTimestampType,
  TimeType => JTimeType,
  VariantType => JVariantType
}

object JavaDataTypeUtils {

  def scalaTypeToJavaType(dataType: DataType): JDataType =
    dataType match {
      case ArrayType(elementType) => JDataTypes.createArrayType(scalaTypeToJavaType(elementType))
      case BinaryType => JDataTypes.BinaryType
      case BooleanType => JDataTypes.BooleanType
      case ByteType => JDataTypes.ByteType
      case DateType => JDataTypes.DateType
      case DecimalType(precision, scale) => JDataTypes.createDecimalType(precision, scale)
      case DoubleType => JDataTypes.DoubleType
      case FloatType => JDataTypes.FloatType
      case GeographyType => JDataTypes.GeographyType
      case IntegerType => JDataTypes.IntegerType
      case LongType => JDataTypes.LongType
      case MapType(keyType, valueType) =>
        JDataTypes.createMapType(scalaTypeToJavaType(keyType), scalaTypeToJavaType(valueType))
      case ShortType => JDataTypes.ShortType
      case StringType => JDataTypes.StringType
      case TimestampType => JDataTypes.TimestampType
      case TimeType => JDataTypes.TimeType
      case VariantType => JDataTypes.VariantType
    }

  def javaTypeToScalaType(jDataType: JDataType): DataType =
    jDataType match {
      case at: JArrayType => ArrayType(javaTypeToScalaType(at.getElementType))
      case _: JBinaryType => BinaryType
      case _: JBooleanType => BooleanType
      case _: JByteType => ByteType
      case _: JDateType => DateType
      case dt: JDecimalType => DecimalType(dt.getPrecision, dt.getScale)
      case _: JDoubleType => DoubleType
      case _: JFloatType => FloatType
      case _: JGeographyType => GeographyType
      case _: JIntegerType => IntegerType
      case _: JLongType => LongType
      case mp: JMapType =>
        MapType(javaTypeToScalaType(mp.getKeyType), javaTypeToScalaType(mp.getValueType))
      case _: JShortType => ShortType
      case _: JStringType => StringType
      case _: JTimestampType => TimestampType
      case _: JTimeType => TimeType
      case _: JVariantType => VariantType
    }
}
