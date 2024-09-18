package com.snowflake.snowpark.types

/** The trait of Snowpark data types
  * @since 0.1.0
  */
abstract class DataType {

  /** Returns a data type name.
    * @since 0.1.0
    */
  def typeName: String =
    this.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type")

  /** Returns a data type name. Alias of [[typeName]]
    * @since 0.1.0
    */
  override def toString: String = typeName

  private[snowpark] def schemaString: String = toString
}

private[snowpark] abstract class AtomicType extends DataType
