package com.snowflake.snowpark.types

/** Map data type. This maps to OBJECT data type in Snowflake.
  * @since 0.1.0
  */
case class MapType(keyType: DataType, valueType: DataType) extends DataType {
  override def toString: String = {
    s"MapType[${keyType.toString}, ${valueType.toString}]"
  }

  override private[snowpark] def schemaString =
    s"Map"
}

private[snowpark] class StructuredMapType(
    override val keyType: DataType,
    override val valueType: DataType,
    val isValueNullable: Boolean)
    extends MapType(keyType, valueType) {
  override def toString: String = {
    s"MapType[${keyType.toString}, ${valueType.toString} nullable = $isValueNullable]"
  }

  override private[snowpark] def schemaString =
    s"Map[${keyType.schemaString}, ${valueType.schemaString} nullable = $isValueNullable]"
}

private[snowpark] object StructuredMapType {
  def apply(keyType: DataType, valueType: DataType, isValueType: Boolean): StructuredMapType =
    new StructuredMapType(keyType, valueType, isValueType)
}
