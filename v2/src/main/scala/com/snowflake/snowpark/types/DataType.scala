package com.snowflake.snowpark.types

/**
 * The trait of Snowpark data types
 *
 * @since 0.1.0
 */
abstract class DataType {

  /**
   * Returns a data type name.
   * @since 0.1.0
   */
  def typeName: String =
    this.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type")

  /**
   * Returns a data type name. Alias of [[typeName]]
   * @since 0.1.0
   */
  override def toString: String = typeName

  private[snowpark] def schemaString: String = toString
}

private[snowpark] abstract class AtomicType extends DataType

/**
 * Array data type. This maps to ARRAY data type in Snowflake.
 * @since 0.1.0
 */
case class ArrayType(elementType: DataType) extends DataType {
  override def toString: String = {
    s"ArrayType[${elementType.toString}]"
  }

  override def schemaString: String =
    s"Array"
}

/**
 * Temporary solution for Structured and Semi Structured data types. Two types will be merged in the
 * future BCR.
 */
private[snowpark] class StructuredArrayType(
    override val elementType: DataType,
    val nullable: Boolean)
    extends ArrayType(elementType) {
  override def toString: String = {
    s"ArrayType[${elementType.toString} nullable = $nullable]"
  }

  override def schemaString: String =
    s"Array[${elementType.schemaString} nullable = $nullable]"
}

private[snowpark] object StructuredArrayType {

  def apply(elementType: DataType, nullable: Boolean): StructuredArrayType =
    new StructuredArrayType(elementType, nullable)
}

/**
 * Binary data type. Mapped to BINARY Snowflake data type.
 * @since 0.1.0
 */
object BinaryType extends AtomicType

/**
 * Boolean data type. Mapped to BOOLEAN Snowflake data type.
 * @since 0.1.0
 */
object BooleanType extends AtomicType

/**
 * Date data type. Mapped to DATE Snowflake data type.
 * @since 0.1.0
 */
object DateType extends AtomicType

/**
 * Map data type. This maps to OBJECT data type in Snowflake.
 * @since 0.1.0
 */
case class MapType(keyType: DataType, valueType: DataType) extends DataType {
  override def toString: String = {
    s"MapType[${keyType.toString}, ${valueType.toString}]"
  }

  override private[snowpark] def schemaString =
    s"Map"
}

private[snowpark] class StructuredMapType(
    override val keyType: DataType,
    override val valueType: DataType,
    val isValueNullable: Boolean)
    extends MapType(keyType, valueType) {
  override def toString: String = {
    s"MapType[${keyType.toString}, ${valueType.toString} nullable = $isValueNullable]"
  }

  override private[snowpark] def schemaString =
    s"Map[${keyType.schemaString}, ${valueType.schemaString} nullable = $isValueNullable]"
}

private[snowpark] object StructuredMapType {
  def apply(keyType: DataType, valueType: DataType, isValueType: Boolean): StructuredMapType =
    new StructuredMapType(keyType, valueType, isValueType)
}
private[snowpark] abstract class NumericType extends AtomicType

private[snowpark] abstract class IntegralType extends NumericType

private[snowpark] abstract class FractionalType extends NumericType

/**
 * Byte data type. Mapped to TINYINT Snowflake date type.
 * @since 0.1.0
 */
object ByteType extends IntegralType

/**
 * Short integer data type. Mapped to SMALLINT Snowflake date type.
 * @since 0.1.0
 */
object ShortType extends IntegralType

/**
 * Integer data type. Mapped to INT Snowflake date type.
 * @since 0.1.0
 */
object IntegerType extends IntegralType

/**
 * Long integer data type. Mapped to BIGINT Snowflake date type.
 * @since 0.1.0
 */
object LongType extends IntegralType

/**
 * Float data type. Mapped to FLOAT Snowflake date type.
 * @since 0.1.0
 */
object FloatType extends FractionalType

/**
 * Double data type. Mapped to DOUBLE Snowflake date type.
 * @since 0.1.0
 */
object DoubleType extends FractionalType

/**
 * Decimal data type. Mapped to NUMBER Snowflake date type.
 * @since 0.1.0
 */
case class DecimalType(precision: Int, scale: Int) extends FractionalType {

  /**
   * Returns Decimal Info. Decimal(precision, scale), Alias of [[toString]]
   * @since 0.1.0
   */
  override def typeName: String = toString

  /**
   * Returns Decimal Info. Decimal(precision, scale)
   * @since 0.1.0
   */
  override def toString: String = s"Decimal($precision, $scale)"

}

/**
 * Companion object of DecimalType.
 * @since 0.9.0
 */
object DecimalType {
  private[snowpark] val MAX_PRECISION = 38
  private[snowpark] val MAX_SCALE = 38

  /**
   * Retrieve DecimalType from BigDecimal value.
   * @since 0.9.0
   */
  def apply(decimal: BigDecimal): DecimalType = {
    if (decimal.precision < decimal.scale) {
      // For DecimalType, Snowflake Compiler expects the precision is equal to or large than
      // the scale, however, in BigDecimal, the digit count starts from the leftmost nonzero digit
      // of the exact result. For example, the precision of 0.01 equals to 1 based on the
      // definition, but the scale is 2. The expected precision should be 2.
      DecimalType(decimal.scale, decimal.scale)
    } else if (decimal.scale < 0) {
      DecimalType(decimal.precision - decimal.scale, 0)
    } else {
      DecimalType(decimal.precision, decimal.scale)
    }
  }

}

/**
 * String data type. Mapped to VARCHAR Snowflake data type.
 * @since 0.1.0
 */
object StringType extends AtomicType

/**
 * Timestamp data type. Mapped to TIMESTAMP Snowflake data type.
 * @since 0.1.0
 */
object TimestampType extends AtomicType

/**
 * Time data type. Mapped to TIME Snowflake data type.
 *
 * @since 0.2.0
 */
object TimeType extends AtomicType
