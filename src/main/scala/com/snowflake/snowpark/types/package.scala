package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ErrorMessage

/**
 * This package contains all Snowpark logical types.
 * @since 0.1.0
 */
package object types {

  private[snowpark] def toJavaType(datatype: DataType): String =
    datatype match {
      // Java UDFs don't support byte type
      // case ByteType =>
      case ShortType => classOf[java.lang.Short].getCanonicalName
      case IntegerType => classOf[java.lang.Integer].getCanonicalName
      case LongType => classOf[java.lang.Long].getCanonicalName
      case DoubleType => classOf[java.lang.Double].getCanonicalName
      case FloatType => classOf[java.lang.Float].getCanonicalName
      case DecimalType(_, _) => classOf[java.math.BigDecimal].getCanonicalName
      case StringType => classOf[java.lang.String].getCanonicalName
      case BooleanType => classOf[java.lang.Boolean].getCanonicalName
      case DateType => classOf[java.sql.Date].getCanonicalName
      case TimeType => classOf[java.sql.Time].getCanonicalName
      case TimestampType => classOf[java.sql.Timestamp].getCanonicalName
      case BinaryType => "byte[]"
      case ArrayType(StringType) => "String[]"
      case MapType(StringType, StringType) => "java.util.Map<String,String>"
      case GeographyType => "Geography"
      case GeometryType => "Geometry"
      case VariantType => "Variant"
      // StructType is only used for defining schema
      // case StructType(_) => // Not Supported
      case _ =>
        throw new UnsupportedOperationException(
          s"${datatype.toString} not supported for scala UDFs")
    }

  // Server only support passing Geography data as string. Added this function as special handler
  // for translating Geography UDF arguments types and return types to String.
  private[snowpark] def toUDFArgumentType(datatype: DataType): String =
    datatype match {
      case GeographyType => classOf[java.lang.String].getCanonicalName
      case GeometryType => classOf[java.lang.String].getCanonicalName
      case VariantType => classOf[java.lang.String].getCanonicalName
      case ArrayType(VariantType) => "String[]"
      case MapType(StringType, VariantType) => "java.util.Map<String,String>"
      case _ => toJavaType(datatype)
    }

  def convertToSFType(dataType: DataType): String = {
    dataType match {
      case dt: DecimalType => s"NUMBER(${dt.precision}, ${dt.scale})"
      case IntegerType => "INT"
      case ShortType => "SMALLINT"
      case ByteType => "BYTEINT"
      case LongType => "BIGINT"
      case FloatType => "FLOAT"
      case DoubleType => "DOUBLE"
      case StringType => "STRING"
      case BooleanType => "BOOLEAN"
      case DateType => "DATE"
      case TimeType => "TIME"
      case TimestampType => "TIMESTAMP"
      case BinaryType => "BINARY"
      case sa: StructuredArrayType =>
        val nullable = if (sa.nullable) "" else " not null"
        s"ARRAY(${convertToSFType(sa.elementType)}$nullable)"
      case sm: StructuredMapType =>
        val isValueNullable = if (sm.isValueNullable) "" else " not null"
        s"MAP(${convertToSFType(sm.keyType)}, ${convertToSFType(sm.valueType)}$isValueNullable)"
      case StructType(fields) =>
        val fieldStr = fields
          .map(
            field =>
              s"${field.name} ${convertToSFType(field.dataType)} " +
                (if (field.nullable) "" else "not null"))
          .mkString(",")
        s"OBJECT($fieldStr)"
      case ArrayType(_) => "ARRAY"
      case MapType(_, _) => "OBJECT"
      case VariantType => "VARIANT"
      case GeographyType => "GEOGRAPHY"
      case GeometryType => "GEOMETRY"
      case StructType(_) => "OBJECT"
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dataType.typeName}")
    }
  }

  private[snowpark] def javaTypeToDataType(cls: Class[_]): DataType = {
    val className = cls.getCanonicalName
    className match {
      case "short" | "java.lang.Short" => ShortType
      case "int" | "java.lang.Integer" => IntegerType
      case "long" | "java.lang.Long" => LongType
      case "float" | "java.lang.Float" => FloatType
      case "double" | "java.lang.Double" => DoubleType
      case "java.math.BigDecimal" => DecimalType(38, 18)
      case "boolean" | "java.lang.Boolean" => BooleanType
      case "java.lang.String" => StringType
      case "byte[]" => BinaryType
      case "java.sql.Date" => DateType
      case "java.sql.Time" => TimeType
      case "java.sql.Timestamp" => TimestampType
      case "com.snowflake.snowpark_java.types.Variant" => VariantType
      case "java.lang.String[]" => ArrayType(StringType)
      case "com.snowflake.snowpark_java.types.Variant[]" => ArrayType(VariantType)
      case "java.util.Map" => throw ErrorMessage.UDF_CANNOT_INFER_MAP_TYPES()
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $className")
    }
  }
}
