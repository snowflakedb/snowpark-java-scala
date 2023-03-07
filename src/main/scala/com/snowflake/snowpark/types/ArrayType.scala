package com.snowflake.snowpark.types

/**
 * Array data type.
 * This maps to ARRAY data type in Snowflake.
 * @since 0.1.0
 */
case class ArrayType(elementType: DataType) extends DataType {
  override def toString: String = {
    s"ArrayType[${elementType.toString}]"
  }
}
