package com.snowflake.snowpark.types

/**
 * Map data type.
 * This maps to OBJECT data type in Snowflake.
 * @since 0.1.0
 */
case class MapType(keyType: DataType, valueType: DataType) extends DataType {
  override def toString: String = {
    s"MapType[${keyType.toString}, ${valueType.toString}]"
  }
}
