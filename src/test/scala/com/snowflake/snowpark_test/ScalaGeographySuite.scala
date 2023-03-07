package com.snowflake.snowpark_test

import com.snowflake.snowpark.SNTestBase
import com.snowflake.snowpark.types.Geography

class ScalaGeographySuite extends SNTestBase {
  val testData = "{\"geometry\":{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}}"

  test("round trip") {
    assert(Geography.fromGeoJSON(testData).asGeoJSON() == testData)
    assert(Geography.fromGeoJSON(testData).getString == testData)
    assert(Geography.fromGeoJSON(testData).toString == testData)
  }

  test("equal") {
    assert(Geography.fromGeoJSON(testData).asGeoJSON() != "abc")
    val testData2 = "{\"geometry\":{\"type\":\"Point\",\"coordinates\":[125.6, 11.1]}}"
    assert(Geography.fromGeoJSON(testData) != Geography.fromGeoJSON(testData2))

    assert(
      Geography.fromGeoJSON(testData).hashCode() ==
        Geography.fromGeoJSON(testData).hashCode())
    assert(
      Geography.fromGeoJSON(testData).hashCode() !=
        Geography.fromGeoJSON(testData2).hashCode())
  }
}
