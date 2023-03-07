package com.snowflake.snowpark_java.types;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

/**
 * Java representation of Snowflake Geography data.
 *
 * @since 0.8.0
 */
public class Geography implements Serializable {

  private final String data;

  private Geography(String data) {
    if (data == null) {
      throw new UncheckedIOException(
          new IOException("Cannot create geography object from null input"));
    }
    this.data = data;
  }

  /**
   * Checks whether two Geography object are equal.
   *
   * @param other A Geography object
   * @return true if these two object are equal
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof Geography) {
      return data.equals(((Geography) other).data);
    }
    return false;
  }

  /**
   * Calculates the hash code of this Geography Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    return this.data.hashCode();
  }

  /**
   * Converts this Geography object to a GeoJSON string.
   *
   * @return A GeoJSON string.
   * @since 0.8.0
   */
  public String asGeoJSON() {
    return data;
  }

  /**
   * Converts this Geography object to a String value. alias of {@code asGeoJson} function.
   *
   * @return A String value.
   */
  @Override
  public String toString() {
    return asGeoJSON();
  }

  /**
   * Creates a Geography object from a GeoJSON string.
   *
   * @param g GeoJSON String
   * @return a new Geography object
   * @since 0.8.0
   */
  public static Geography fromGeoJSON(String g) {
    return new Geography(g);
  }
}
