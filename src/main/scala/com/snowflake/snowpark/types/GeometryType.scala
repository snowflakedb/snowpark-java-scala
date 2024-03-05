package com.snowflake.snowpark.types

/**
 * Geometry data type. This maps to GEOMETRY data type in Snowflake.
 * @since 1.12.0
 */
object GeometryType extends DataType {
  override def toString: String = {
    s"GeometryType"
  }
}
