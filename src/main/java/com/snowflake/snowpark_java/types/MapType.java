package com.snowflake.snowpark_java.types;

import java.util.Objects;

/**
 * Map data type. This maps to OBJECT data type in Snowflake.
 *
 * @since 0.9.0
 */
public class MapType extends DataType {
  private final DataType keyType;
  private final DataType valueType;
  private final String typeName;

  MapType(DataType keyType, DataType valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.typeName = "MapType[" + keyType.typeName() + ", " + valueType.typeName() + "]";
  }

  /**
   * Retrieves the data type of this MapType's key.
   *
   * @return A DataType object representing the key type of this MapType
   * @since 0.9.0
   */
  public DataType getKeyType() {
    return keyType;
  }

  /**
   * Retrieves the data type of this MapType's value.
   *
   * @return A DataType object representing the value type of this MapType
   * @since 0.9.0
   */
  public DataType getValueType() {
    return valueType;
  }

  /**
   * Generates a String value to represent this map type.
   *
   * @return A String value in the format of "MapType['key type', 'value type']"
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
    if (other instanceof MapType) {
      MapType mt = (MapType) other;
      return keyType.equals(mt.keyType) && valueType.equals(mt.valueType);
    }
    return false;
  }

  /**
   * Calculates the hash code of this Map Type Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType, typeName);
  }
}
