package com.snowflake.snowpark.types

/**
 * Geography data type. This maps to GEOGRAPHY data type in Snowflake.
 * @since 0.2.0
 */
object GeographyType extends DataType {
  override def toString: String = {
    s"GeographyType"
  }
}
