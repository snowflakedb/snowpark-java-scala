package com.snowflake.snowpark_java.types;

import java.util.Objects;

/**
 * Decimal data type. Mapped to NUMBER Snowflake date type.
 *
 * @since 0.9.0
 */
public class DecimalType extends FractionalType {
  public static final int MAX_PRECISION = 38;
  public static final int MAX_SCALE = 38;
  private final int precision;
  private final int scale;
  private final String typeName;

  DecimalType(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
    typeName = "Decimal(" + precision + ", " + scale + ")";
  }

  /**
   * Retrieves the precision of this decimal number.
   *
   * @return An int value representing the precision of this decimal number
   * @since 0.9.0
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Retrieves the scale of this decimal number.
   *
   * @return An int value representing the scale of this decimal number
   * @since 0.9.0
   */
  public int getScale() {
    return scale;
  }

  /**
   * Generates a String value to represent this DecimalType.
   *
   * @return A String value in the format of "Decimal('precision', 'scale')"
   * @since 0.9.0
   */
  @Override
  public String toString() {
    return typeName;
  }

  /**
   * Verifies if a DataType equals to this one.
   *
   * @param other A DataType object
   * @return true if these data types are equivalent, false for otherwise.
   * @since 0.9.0
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof DecimalType) {
      DecimalType dt = (DecimalType) other;
      return precision == dt.precision && scale == dt.scale;
    }
    return false;
  }

  /**
   * Calculates the hash code of this Decimal Type Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return Objects.hash(precision, scale, typeName);
  }
}
