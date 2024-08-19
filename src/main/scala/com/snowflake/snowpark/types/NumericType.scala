package com.snowflake.snowpark.types

private[snowpark] abstract class NumericType extends AtomicType

private[snowpark] abstract class IntegralType extends NumericType

private[snowpark] abstract class FractionalType extends NumericType

/** Byte data type. Mapped to TINYINT Snowflake date type.
  * @since 0.1.0
  */
object ByteType extends IntegralType

/** Short integer data type. Mapped to SMALLINT Snowflake date type.
  * @since 0.1.0
  */
object ShortType extends IntegralType

/** Integer data type. Mapped to INT Snowflake date type.
  * @since 0.1.0
  */
object IntegerType extends IntegralType

/** Long integer data type. Mapped to BIGINT Snowflake date type.
  * @since 0.1.0
  */
object LongType extends IntegralType

/** Float data type. Mapped to FLOAT Snowflake date type.
  * @since 0.1.0
  */
object FloatType extends FractionalType

/** Double data type. Mapped to DOUBLE Snowflake date type.
  * @since 0.1.0
  */
object DoubleType extends FractionalType

/** Decimal data type. Mapped to NUMBER Snowflake date type.
  * @since 0.1.0
  */
case class DecimalType(precision: Int, scale: Int) extends FractionalType {

  /** Returns Decimal Info. Decimal(precision, scale), Alias of [[toString]]
    * @since 0.1.0
    */
  override def typeName: String = toString

  /** Returns Decimal Info. Decimal(precision, scale)
    * @since 0.1.0
    */
  override def toString: String = s"Decimal($precision, $scale)"

}

/** Companion object of DecimalType.
  * @since 0.9.0
  */
object DecimalType {
  private[snowpark] val MAX_PRECISION = 38
  private[snowpark] val MAX_SCALE = 38

  /** Retrieve DecimalType from BigDecimal value.
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
