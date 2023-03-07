package com.snowflake.snowpark_java.types;

import com.snowflake.snowpark.internal.JavaDataTypeUtils;
import java.util.Objects;

/**
 * Represents the content of StructType.
 *
 * @since 0.9.0
 */
public class StructField {
  private final com.snowflake.snowpark.types.StructField field;

  /**
   * Creates a new StructField.
   *
   * @param columnIdentifier A ColumnIdentifier presenting column name
   * @param dataType The column data type
   * @param nullable Whether the column is nullable or not
   * @since 0.9.0
   */
  public StructField(ColumnIdentifier columnIdentifier, DataType dataType, boolean nullable) {
    this(
        new com.snowflake.snowpark.types.StructField(
            columnIdentifier.toScalaColumnIdentifier(),
            JavaDataTypeUtils.javaTypeToScalaType(dataType),
            nullable));
  }

  /**
   * Creates a new StructField with nullable column.
   *
   * @param columnIdentifier A ColumnIdentifier presenting column name
   * @param dataType The column data type
   * @since 0.9.0
   */
  public StructField(ColumnIdentifier columnIdentifier, DataType dataType) {
    this(columnIdentifier, dataType, true);
  }

  /**
   * Creates a new StructField.
   *
   * @param name The column name
   * @param dataType The column data type
   * @param nullable Whether the column is nullable or not
   * @since 0.9.0
   */
  public StructField(String name, DataType dataType, boolean nullable) {
    this(
        com.snowflake.snowpark.types.StructField.apply(
            name, JavaDataTypeUtils.javaTypeToScalaType(dataType), nullable));
  }

  /**
   * Creates a new StructField with nullable column.
   *
   * @param name The column name
   * @param dataType The column data type
   * @since 0.9.0
   */
  public StructField(String name, DataType dataType) {
    this(name, dataType, true);
  }

  StructField(com.snowflake.snowpark.types.StructField field) {
    this.field = field;
  }

  /**
   * Retrieves the column name.
   *
   * @return A String value representing the column name
   * @since 0.9.0
   */
  public String name() {
    return this.field.name();
  }

  /**
   * Retrieves the column identifier.
   *
   * @return A ColumnIdentifier representing the column name
   * @since 0.9.0
   */
  public ColumnIdentifier columnIdentifier() {
    return new ColumnIdentifier(this.field.columnIdentifier());
  }

  /**
   * Retrieves the column data type.
   *
   * @return A DataType object representing the column data type
   * @since 0.9.0
   */
  public DataType dataType() {
    return JavaDataTypeUtils.scalaTypeToJavaType(this.field.dataType());
  }

  /**
   * Verifies if this column is nullable.
   *
   * @return A boolean value representing whether this column is nullable
   * @since 0.9.0
   */
  public boolean nullable() {
    return this.field.nullable();
  }

  /**
   * Generates a String value to represent this StructField.
   *
   * @return A String value in the format of "StructField('name', 'data type', 'nullable')"
   * @since 0.9.0
   */
  @Override
  public String toString() {
    return this.field.toString();
  }

  /**
   * Verifies if a StructField equals to this one.
   *
   * @param other A StructField object
   * @return true if these two StructFields are equivalent, false for otherwise.
   * @since 0.9.0
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof StructField) {
      return this.field.equals(((StructField) other).field);
    }
    return false;
  }

  /**
   * Calculates the hash code of this StructField Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return Objects.hash(name(), dataType(), nullable());
  }

  com.snowflake.snowpark.types.StructField toScalaStructField() {
    return this.field;
  }
}
