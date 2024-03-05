package com.snowflake.snowpark.types

import java.io.{IOException, UncheckedIOException}

object Geometry {
  def fromGeoJSON(g: String): Geometry = new Geometry(g)
}
class Geometry private (private val stringData: String) {
  if (stringData == null) {
    throw new UncheckedIOException(
      new IOException("Cannot create geometry object from null input"))
  }

  override def equals(obj: Any): Boolean =
    obj match {
      case g: Geometry => stringData.equals(g.stringData)
      case _ => false
    }

  override def hashCode(): Int = stringData.hashCode

  override def toString: String = stringData
}
