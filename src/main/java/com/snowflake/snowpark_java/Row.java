package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.types.Geography;
import com.snowflake.snowpark_java.types.Geometry;
import com.snowflake.snowpark_java.types.InternalUtils;
import com.snowflake.snowpark_java.types.Variant;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

/**
 * Represents a row returned by the evaluation of a {@code DataFrame}.
 *
 * @see com.snowflake.snowpark_java.DataFrame DataFram
 * @since 0.9.0
 */
public class Row implements Serializable, Cloneable {
  private final com.snowflake.snowpark.Row scalaRow;

  // package private constructor, internal use only.
  Row(com.snowflake.snowpark.Row row) {
    this.scalaRow = row;
  }

  /**
   * Creates a {@code Row} based on the values in the given array.
   *
   * @param values {@code Row} elements
   * @since 0.9.0
   */
  public Row(Object[] values) {
    this(com.snowflake.snowpark.Row.fromArray(javaObjectToScalaObject(values)));
  }

  private static Object[] javaObjectToScalaObject(Object[] input) {
    Object[] result = Arrays.copyOf(input, input.length);
    for (int i = 0; i < result.length; i++) {
      if (result[i] instanceof Variant) {
        result[i] = InternalUtils.toScalaVariant((Variant) result[i]);
      } else if (result[i] instanceof Variant[]) {
        Variant[] javaVariantArray = (Variant[]) result[i];
        com.snowflake.snowpark.types.Variant[] resultArray =
            new com.snowflake.snowpark.types.Variant[javaVariantArray.length];
        for (int idx = 0; idx < resultArray.length; idx++) {
          resultArray[idx] = InternalUtils.toScalaVariant(javaVariantArray[idx]);
        }
        result[i] = resultArray;
      } else if (result[i] instanceof Map<?, ?>) {
        result[i] = JavaUtils.javaMapToScalaWithVariantConversion((Map<Object, Object>) result[i]);
      } else if (result[i] instanceof Geography) {
        result[i] =
            com.snowflake.snowpark.types.Geography.fromGeoJSON(((Geography) result[i]).asGeoJSON());
      } else if (result[i] instanceof Geometry) {
        result[i] = com.snowflake.snowpark.types.Geometry.fromGeoJSON(result[i].toString());
      }
    }
    return result;
  }

  /**
   * Converts this {@code Row} to a {@code List} of {@code Object}.
   *
   * @return A {@code List} contains {@code Row} elements.
   * @since 0.9.0
   */
  public List<Object> toList() {
    return JavaUtils.seqToList(scalaRow.toSeq());
  }

  /**
   * Counts the number of column in this {@code Row}.
   *
   * @return An integer number represents the size of this {@code Row}
   * @since 0.9.0
   */
  public int size() {
    return scalaRow.size();
  }

  /**
   * Creates a clone of this {@code Row} object.
   *
   * @return A new {@code Row} object containing same elements as this {@code Row}
   * @since 0.9.0
   */
  @Override
  public Row clone() {
    Row cloned;
    try {
      cloned = (Row) super.clone();
    } catch (CloneNotSupportedException e) {
      cloned = new Row(scalaRow);
    }
    return cloned;
  }

  /**
   * Verifies the equality of two {@code Row} objects.
   *
   * @param other The {@code Row} object to be compared to
   * @return true if the given row equals this row.
   * @since 0.9.0
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof Row) {
      Row otherRow = (Row) other;
      return this.scalaRow.equals(otherRow.scalaRow);
    }
    return false;
  }

  /**
   * Calculates the hash code of this Row object
   *
   * @return An int number representing the hash code
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return scalaRow.hashCode();
  }

  /**
   * Retrieves the value of column in the {@code Row} at the given index.
   *
   * @param index The index of the target column
   * @return The value of the column at the given index
   */
  public Object get(int index) {
    return toJavaValue(scalaRow.get(index));
  }

  private static Object toJavaValue(Object value) {
    if (value instanceof com.snowflake.snowpark.types.Variant) {
      return InternalUtils.createVariant((com.snowflake.snowpark.types.Variant) value);
    } else if (value instanceof com.snowflake.snowpark.types.Geography) {
      return Geography.fromGeoJSON(((com.snowflake.snowpark.types.Geography) value).asGeoJSON());
    } else if (value instanceof com.snowflake.snowpark.types.Geometry) {
      return Geometry.fromGeoJSON(value.toString());
    } else if (value instanceof com.snowflake.snowpark.types.Variant[]) {
      com.snowflake.snowpark.types.Variant[] scalaVariantArray =
          (com.snowflake.snowpark.types.Variant[]) value;
      Variant[] resultArray = new Variant[scalaVariantArray.length];
      for (int idx = 0; idx < scalaVariantArray.length; idx++) {
        resultArray[idx] = InternalUtils.createVariant(scalaVariantArray[idx]);
      }
      return resultArray;
    } else if (value instanceof scala.collection.immutable.Map<?, ?>) {
      scala.collection.immutable.Map<?, ?> input = (scala.collection.immutable.Map<?, ?>) value;
      Map<Object, Object> result = new HashMap<>();
      // key is either Long or String, no need to convert values
      input.foreach(x -> result.put(x._1, toJavaValue(x._2)));
      return result;
    } else if (value instanceof Object[]) {
      Object[] arr = (Object[]) value;
      List<Object> result = new ArrayList<>(arr.length);
      for (Object x : arr) {
        result.add(toJavaValue(x));
      }
      return result;
    } else {
      return value;
    }
  }

  /**
   * Verifies if the value of the column at the given index is null.
   *
   * @param index The index of target column
   * @return true if the value is null at the given index
   * @since 0.9.0
   */
  public boolean isNullAt(int index) {
    return scalaRow.isNullAt(index);
  }

  /**
   * Retrieves the value of the column at the given index as a boolean value.
   *
   * @param index The index of target column
   * @return The boolean value of the column at the given index
   * @since 0.9.0
   */
  public boolean getBoolean(int index) {
    return scalaRow.getBoolean(index);
  }

  /**
   * Retrieves the value of the column at the given index as a byte value.
   *
   * @param index The index of target column
   * @return The byte value of the column at the given index
   * @since 0.9.0
   */
  public byte getByte(int index) {
    return scalaRow.getByte(index);
  }

  /**
   * Retrieves the value of the column at the given index as a short value.
   *
   * @param index The index of target column
   * @return The short value of the column at the given index
   * @since 0.9.0
   */
  public short getShort(int index) {
    return scalaRow.getShort(index);
  }

  /**
   * Retrieves the value of the column at the given index as a int value.
   *
   * @param index The index of target column
   * @return The int value of the column at the given index
   * @since 0.9.0
   */
  public int getInt(int index) {
    return scalaRow.getInt(index);
  }

  /**
   * Retrieves the value of the column at the given index as a long value.
   *
   * @param index The index of target column
   * @return The long value of the column at the given index
   * @since 0.9.0
   */
  public long getLong(int index) {
    return scalaRow.getLong(index);
  }

  /**
   * Retrieves the value of the column at the given index as a float value.
   *
   * @param index The index of target column
   * @return The float value of the column at the given index
   * @since 0.9.0
   */
  public float getFloat(int index) {
    return scalaRow.getFloat(index);
  }

  /**
   * Retrieves the value of the column at the given index as a double value.
   *
   * @param index The index of target column
   * @return The double value of the column at the given index
   * @since 0.9.0
   */
  public double getDouble(int index) {
    return scalaRow.getDouble(index);
  }

  /**
   * Retrieves the value of the column at the given index as a String value.
   *
   * @param index The index of target column
   * @return The String value of the column at the given index
   * @since 0.9.0
   */
  public String getString(int index) {
    return scalaRow.getString(index);
  }

  /**
   * Retrieves the value of the column at the given index as an array of byte.
   *
   * @param index The index of target column
   * @return An array of byte representing the binary value
   * @since 0.9.0
   */
  public byte[] getBinary(int index) {
    return scalaRow.getBinary(index);
  }

  /**
   * Retrieves the value of the column at the given index as a Date value.
   *
   * @param index The index of target column
   * @return The Date value of the column at the given index
   * @since 0.9.0
   */
  public Date getDate(int index) {
    return scalaRow.getDate(index);
  }

  /**
   * Retrieves the value of the column at the given index as a Time value.
   *
   * @param index The index of target column
   * @return The Time value of the column at the given index
   * @since 0.9.0
   */
  public Time getTime(int index) {
    return scalaRow.getTime(index);
  }

  /**
   * Retrieves the value of the column at the given index as a Timestamp value.
   *
   * @param index The index of target column
   * @return The Timestamp value of the column at the given index
   * @since 0.9.0
   */
  public Timestamp getTimestamp(int index) {
    return scalaRow.getTimestamp(index);
  }

  /**
   * Retrieves the value of the column at the given index as a BigDecimal value.
   *
   * @param index The index of target column
   * @return The BigDecimal value of the column at the given index
   * @since 0.9.0
   */
  public BigDecimal getDecimal(int index) {
    return scalaRow.getDecimal(index);
  }

  /**
   * Retrieves the value of the column at the given index as a Variant value.
   *
   * @param index The index of target column
   * @return The Variant value of the column at the given index
   * @since 0.9.0
   */
  public Variant getVariant(int index) {
    return InternalUtils.createVariant(scalaRow.getVariant(index));
  }

  /**
   * Retrieves the value of the column at the given index as a Geography value.
   *
   * @param index The index of target column
   * @return The Geography value of the column at the given index
   * @since 0.9.0
   */
  public Geography getGeography(int index) {
    return Geography.fromGeoJSON(scalaRow.getGeography(index).asGeoJSON());
  }

  /**
   * Retrieves the value of the column at the given index as a Geometry value.
   *
   * @param index The index of target column
   * @return The Geometry value of the column at the given index
   * @since 1.12.0
   */
  public Geometry getGeometry(int index) {
    return Geometry.fromGeoJSON(scalaRow.getGeometry(index).toString());
  }

  /**
   * Retrieves the value of the column at the given index as a list of Variant.
   *
   * @param index The index of target column
   * @return A list of Variant
   * @since 0.9.0
   */
  public List<Variant> getListOfVariant(int index) {
    scala.collection.Seq<com.snowflake.snowpark.types.Variant> scalaSeq =
        this.scalaRow.getSeqOfVariant(index);
    List<Variant> result = new ArrayList<>(scalaSeq.size());
    for (int i = 0; i < scalaSeq.size(); i++) {
      result.add(InternalUtils.createVariant(scalaSeq.apply(i)));
    }
    return result;
  }

  /**
   * Retrieves the value of the column at the given index as a map of Variant
   *
   * @param index The index of target column
   * @return A map from String to Variant
   * @since 0.9.0
   */
  public Map<String, Variant> getMapOfVariant(int index) {
    scala.collection.Map<String, com.snowflake.snowpark.types.Variant> scalaMap =
        this.scalaRow.getMapOfVariant(index);
    Map<String, Variant> result = new HashMap<>();
    scala.collection.Seq<String> keys = scalaMap.keys().toSeq();
    for (int i = 0; i < keys.size(); i++) {
      result.put(keys.apply(i), InternalUtils.createVariant(scalaMap.apply(keys.apply(i))));
    }
    return result;
  }

  /**
   * Retrieves the value of the column at the given index as a list of Object.
   *
   * @param index The index of target column
   * @return A list of Object
   * @since 1.13.0
   */
  public List<?> getList(int index) {
    return (List<?>) get(index);
  }

  /**
   * Retrieves the value of the column at the given index as a Java Map
   *
   * @param index The index of target column
   * @return A Java Map
   * @since 1.13.0
   */
  public Map<?, ?> getMap(int index) {
    return (Map<?, ?>) get(index);
  }

  /**
   * Generates a string value to represent the content of this row.
   *
   * @return A String value representing the content of this row
   * @since 0.9.0
   */
  @Override
  public String toString() {
    return this.scalaRow.toString();
  }

  /**
   * Creates a {@code Row} based on the given values.
   *
   * @param values Row elements.
   * @return A {@code Row} object.
   * @since 0.9.0
   */
  public static Row create(Object... values) {
    return new Row(values);
  }

  com.snowflake.snowpark.Row getScalaRow() {
    return this.scalaRow;
  }
}
