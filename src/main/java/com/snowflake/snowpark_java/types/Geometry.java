package com.snowflake.snowpark_java.types;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

/**
 * Java representation of Snowflake Geometry data.
 *
 * @since 1.12.0
 */
public class Geometry implements Serializable {
  private final String data;

  private Geometry(String data) {
    if (data == null) {
      throw new UncheckedIOException(
          new IOException("Cannot create geometry object from null input"));
    }
    this.data = data;
  }

  /**
   * Checks whether two Geometry object are equal.
   *
   * @param other A Geometry object
   * @return true if these two object are equal
   * @since 1.12.0
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof Geometry) {
      return data.equals(((Geometry) other).data);
    }
    return false;
  }

  /**
   * Calculates the hash code of this Geometry Object.
   *
   * @return An int number representing the hash code value
   * @since 1.12.0
   */
  @Override
  public int hashCode() {
    return this.data.hashCode();
  }

  /**
   * Converts this Geometry object to a String value.
   *
   * @return A String value.
   * @since 1.12.0
   */
  @Override
  public String toString() {
    return this.data;
  }

  /**
   * Creates a Geometry object from a GeoJSON string.
   *
   * @param g GeoJSON String
   * @return a new Geometry object
   * @since 1.12.0
   */
  public static Geometry fromGeoJSON(String g) {
    return new Geometry(g);
  }
}
