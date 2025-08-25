package com.snowflake.snowpark_java.types;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * StructType data type, represents table schema.
 *
 * @since 0.9.0
 */
public class StructType extends DataType implements Iterable<StructField> {
  private final com.snowflake.snowpark.types.StructType scalaStructType;

  StructType(com.snowflake.snowpark.types.StructType structType) {
    this.scalaStructType = structType;
  }

  /**
   * Clones the given StructType object.
   *
   * @param other A StructType object
   * @since 0.9.0
   */
  public StructType(StructType other) {
    this(other.scalaStructType);
  }

  /**
   * Creates a StructType object based on the given Array of StructField.
   *
   * @param fields An Array of StructField
   * @since 0.9.0
   */
  public StructType(StructField[] fields) {
    this(new com.snowflake.snowpark.types.StructType(toScalaFieldsArray(fields)));
  }

  /**
   * Creates a StructType object based on the given StructField
   *
   * @param fields A list of StructFields
   * @since 0.9.0
   * @return A new StructType object
   */
  public static StructType create(StructField... fields) {
    return new StructType(fields);
  }

  private static com.snowflake.snowpark.types.StructField[] toScalaFieldsArray(
      StructField[] fields) {
    com.snowflake.snowpark.types.StructField[] result =
        new com.snowflake.snowpark.types.StructField[fields.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = fields[i].toScalaStructField();
    }
    return result;
  }

  /**
   * Return the index of the specified field.
   *
   * @param fieldName the name of the field.
   * @return the index of the field with the specified name.
   * @throws IllegalArgumentException if the given field name does not exist in the schema.
   * @since 1.15.0
   */
  public int fieldIndex(String fieldName) {
    return this.scalaStructType.fieldIndex(fieldName);
  }

  /**
   * Retrieves the names of all {@link com.snowflake.snowpark_java.types.StructField} in this {@link
   * com.snowflake.snowpark_java.types.StructType}.
   *
   * <p>Example:
   *
   * <pre>{@code
   * StructType schema = new StructType(new StructField[] {
   *   new StructField("c1", DataTypes.IntegerType),
   *   new StructField("c2", DataTypes.StringType),
   * });
   * schema.names();
   * // res: String[] = {"C1", "C2"}
   * }</pre>
   *
   * @return an array representing the names of the fields in this StructType.
   * @since 0.9.0
   */
  public String[] names() {
    return JavaUtils.seqToJavaStringArray(this.scalaStructType.names());
  }

  /**
   * Retrieves the names of all {@link com.snowflake.snowpark_java.types.StructField} in this {@link
   * com.snowflake.snowpark_java.types.StructType}. This is an alias of {@link #names}.
   *
   * <p>Example:
   *
   * <pre>{@code
   * StructType schema = new StructType(new StructField[] {
   *   new StructField("c1", DataTypes.IntegerType),
   *   new StructField("c2", DataTypes.StringType),
   * });
   * schema.fieldNames();
   * // res: String[] = {"C1", "C2"}
   * }</pre>
   *
   * @return an array representing the names of the fields in this StructType.
   * @since 1.17.0
   */
  public String[] fieldNames() {
    return this.names();
  }

  /**
   * Counts the number of StructFields in this StructType object.
   *
   * @return An int number
   * @since 0.9.0
   */
  public int size() {
    return this.scalaStructType.size();
  }

  /**
   * Creates an Iterator of StructFields.
   *
   * @return An Iterator of StructField
   * @since 0.9.0
   */
  @Override
  public Iterator<StructField> iterator() {
    return new Iterator<StructField>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return scalaStructType.size() > index;
      }

      @Override
      public StructField next() {
        if (!hasNext()) {
          throw new NoSuchElementException(
              "Index: " + index + " iterator size: " + scalaStructType.size());
        }
        return new StructField(scalaStructType.apply(index++));
      }
    };
  }

  /**
   * Retrieves the StructField object from the given index.
   *
   * @param index An int number representing the index of StructField
   * @return The StructField object at the given index.
   * @since 0.9.0
   */
  public StructField get(int index) {
    return new StructField(this.scalaStructType.apply(index));
  }

  /**
   * Generates a String value to represent this StructType.
   *
   * @return A String value in the format of "StructType[StructField('name', 'data type',
   *     'nullable')...]"
   * @since 0.9.0
   */
  @Override
  public String toString() {
    return this.scalaStructType.toString();
  }

  /**
   * Creates new StructType by appending the given StructField to the end of this StructType. This
   * function doesn't modify this StructType object, but create and return a new one.
   *
   * @param field The StructField object being appended.
   * @return A new StructType object
   * @since 0.9.0
   */
  public StructType add(StructField field) {
    return new StructType(this.scalaStructType.add(field.toScalaStructField()));
  }

  /**
   * Creates new StructType by appending a new StructField with the given info to the end of this
   * StructType. This function doesn't modify this StructType object, but create or return a new
   * one.
   *
   * @param name The name of the StructField object being appended.
   * @param dataType The data type of the StructField object being appended.
   * @param nullable Whether the new StructField is nullable or not
   * @return A new StructType object
   * @since 0.9.0
   */
  public StructType add(String name, DataType dataType, boolean nullable) {
    return add(new StructField(name, dataType, nullable));
  }

  /**
   * Creates new StructType by appending a new StructField with the given info to the end of this
   * StructType. The new StructField is nullable. This function doesn't modify this StructType
   * object, but create and return a new one.
   *
   * @param name The name of the StructField object being appended.
   * @param dataType The data type of the StructField object being appended.
   * @return A new StructType object
   * @since 0.9.0
   */
  public StructType add(String name, DataType dataType) {
    return add(name, dataType, true);
  }

  /**
   * Retrieves the corresponding StructField object of the given name.
   *
   * @param name The name of StructField
   * @return An Optional StructField object.
   * @since 0.9.0
   */
  public Optional<StructField> nameToField(String name) {
    scala.Option<com.snowflake.snowpark.types.StructField> fields =
        this.scalaStructType.nameToField(name);
    return fields.isEmpty() ? Optional.empty() : Optional.of(new StructField(fields.get()));
  }

  /**
   * Verifies if a StructType equals to this one.
   *
   * @param other A StructType object
   * @return true if these data types are equivalent, false for otherwise.
   * @since 0.9.0
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof StructType) {
      return this.scalaStructType.equals(((StructType) other).scalaStructType);
    }
    return false;
  }

  /**
   * Calculates the hash code of this StructType Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return this.scalaStructType.hashCode();
  }

  /**
   * Prints the schema StructType content in a tree structure diagram.
   *
   * @since 0.9.0
   */
  public void printTreeString() {
    scalaStructType.printTreeString();
  }

  com.snowflake.snowpark.types.StructType getScalaStructType() {
    return scalaStructType;
  }
}
