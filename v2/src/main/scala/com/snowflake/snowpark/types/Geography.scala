package com.snowflake.snowpark.types

import java.io.{IOException, UncheckedIOException}

/**
 * Companion object of Geography class.
 */
object Geography {

  /**
   * Creates a Geography class from a GeoJSON string
   *
   * @param g
   *   GeoJSON string
   * @return
   *   a Geography class
   * @since 0.2.0
   */
  def fromGeoJSON(g: String): Geography = new Geography(g)
}

/**
 * Scala representation of Snowflake Geography data. Only support GeoJSON format.
 *
 * @since 0.2.0
 */
class Geography private (private val stringData: String) {
  if (stringData == null) throwNullInputError()

  /**
   * Returns whether the Geography object equals to the input object.
   *
   * @return
   *   GeoJSON string
   * @since 0.2.0
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case g: Geography => stringData.equals(g.stringData)
      case _ => false
    }
  }

  /**
   * Returns the hashCode of the stored GeoJSON string.
   *
   * @return
   *   hash code
   * @since 0.2.0
   */
  override def hashCode(): Int = stringData.hashCode

  private def throwNullInputError(): Unit =
    throw new UncheckedIOException(
      new IOException("Cannot create geography object from null input"))

  /**
   * Returns the underling string data for GeoJSON.
   *
   * @return
   *   GeoJSON string
   * @since 0.2.0
   */
  def asGeoJSON(): String = stringData

  /**
   * Returns the underling string data for GeoJSON.
   *
   * @return
   *   GeoJSON string
   * @since 0.2.0
   */
  def getString: String = stringData

  /**
   * Returns the underling string data for GeoJSON.
   *
   * @return
   *   GeoJSON string
   * @since 0.2.0
   */
  override def toString: String = stringData
}

/**
 * Geography data type. This maps to GEOGRAPHY data type in Snowflake.
 * @since 0.2.0
 */
object GeographyType extends DataType {
  override def toString: String = {
    s"GeographyType"
  }

  override private[snowpark] def schemaString: String = s"Geography"
}
