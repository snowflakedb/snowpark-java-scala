package com.snowflake.snowpark_java.types;

import java.util.Objects;

/**
 * Array data type. This maps to ARRAY data type in Snowflake.
 *
 * @since 0.9.0
 */
public class ArrayType extends DataType {
  private final DataType elementType;
  private final String typeName;

  ArrayType(DataType elementType) {
    this.elementType = elementType;
    this.typeName = "ArrayType[" + elementType.typeName() + "]";
  }

  /**
   * Retrieves the type of this array's element.
   *
   * @return A DataType object representing the element type of this array.
   * @since 0.9.0
   */
  public DataType getElementType() {
    return elementType;
  }

  /**
   * Generates a String value to represent this array type.
   *
   * @return A String value in the format of "ArrayType['element type']"
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
    if (other instanceof ArrayType) {
      return elementType.equals(((ArrayType) other).elementType);
    }
    return false;
  }

  /**
   * Calculates the hash code of this Array Type Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return Objects.hash(elementType, typeName);
  }
}
