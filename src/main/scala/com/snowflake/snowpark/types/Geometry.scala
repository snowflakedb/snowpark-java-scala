package com.snowflake.snowpark.types

import java.io.{IOException, UncheckedIOException}

/** Companion object of Geometry class.
  * @since 1.12.0
  */
object Geometry {

  /** Creates a Geometry class from a GeoJSON string
    *
    * @param g
    *   GeoJSON string
    * @return
    *   a Geometry class
    * @since 1.12.0
    */
  def fromGeoJSON(g: String): Geometry = new Geometry(g)
}

/** Scala representation of Snowflake Geometry data. Only support GeoJSON format.
  *
  * @since 1.12.0
  */
class Geometry private (private val stringData: String) {
  if (stringData == null) {
    throw new UncheckedIOException(new IOException("Cannot create geometry object from null input"))
  }

  /** Returns whether the Geometry object equals to the input object.
    *
    * @return
    *   GeoJSON string
    * @since 1.12.0
    */
  override def equals(obj: Any): Boolean =
    obj match {
      case g: Geometry => stringData.equals(g.stringData)
      case _ => false
    }

  /** Returns the hashCode of the stored GeoJSON string.
    *
    * @return
    *   hash code
    * @since 1.12.0
    */
  override def hashCode(): Int = stringData.hashCode

  /** Returns the underling string data for GeoJSON.
    *
    * @return
    *   GeoJSON string
    * @since 1.12.0
    */
  override def toString: String = stringData
}
