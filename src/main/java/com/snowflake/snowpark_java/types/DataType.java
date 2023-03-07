package com.snowflake.snowpark_java.types;

import java.io.Serializable;
import java.util.Objects;

/**
 * Snowpark data types
 *
 * @since 0.9.0
 */
public abstract class DataType implements Serializable {

  /**
   * Retrieves the String name of this DataType object.
   *
   * @return A String name of this DataType object
   * @since 0.9.0
   */
  public String typeName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Generates a String value to represent this DataType object.
   *
   * @return A String value in the format of XXXType
   * @since 0.9.0
   */
  @Override
  public String toString() {
    return typeName();
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
    if (other instanceof DataType) {
      return this.toString().equals(other.toString());
    }
    return false;
  }

  /**
   * Calculates the hash code of this DataType Type Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return Objects.hash(typeName());
  }
}
