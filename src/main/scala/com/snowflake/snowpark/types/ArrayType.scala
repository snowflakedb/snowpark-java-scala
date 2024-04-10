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

/* temporary solution for Structured and Semi Structured data types.
Two types will be merged in the future BCR. */
private[snowpark] class StructuredArrayType(
                                             override val elementType: DataType,
                                             val nullable: Boolean) extends ArrayType(elementType) {
  override def toString: String = {
    s"ArrayType[${elementType.toString} nullable = $nullable]"
  }
}

private[snowpark] object StructuredArrayType {

  def apply(elementType: DataType, nullable: Boolean): StructuredArrayType =
    new StructuredArrayType(elementType, nullable)
}
