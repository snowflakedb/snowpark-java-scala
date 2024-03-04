package com.snowflake.snowpark.types

object Geometry {
  def fromGeoJSON(g: String): Geometry = new Geometry(g)
}
class Geometry private (private val stringData: String) {

}
