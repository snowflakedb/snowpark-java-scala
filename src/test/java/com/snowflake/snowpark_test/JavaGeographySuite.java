package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.types.Geography;
import com.snowflake.snowpark_java.types.Geometry;
import org.junit.Test;

public class JavaGeographySuite {

  private final String testData =
      "{\"geometry\":{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}}";

  private final String testData2 =
      "{\"coordinates\": [3.000000000000000e+01,1.000000000000000e+01],\"type\": \"Point\"}";

  @Test
  public void testRoundTrip() {
    assert (Geography.fromGeoJSON(testData).asGeoJSON().equals(testData));
    assert (Geography.fromGeoJSON(testData).toString().equals(testData));

    assert (Geometry.fromGeoJSON(testData2).toString().equals(testData));
  }

  @Test
  public void testEqual() {
    assert (Geography.fromGeoJSON(testData).equals(Geography.fromGeoJSON(testData)));
    String testData2 = "{\"geometry\":{\"type\":\"Point\",\"coordinates\":[125.6, 11.1]}}";
    assert !(Geography.fromGeoJSON(testData).equals(Geography.fromGeoJSON(testData2)));

    // compare to other object
    assert !Geography.fromGeoJSON(testData).equals("abc");

    assert Geography.fromGeoJSON(testData).hashCode() == Geography.fromGeoJSON(testData).hashCode();
    assert Geography.fromGeoJSON(testData2).hashCode()
        != Geography.fromGeoJSON(testData).hashCode();

    assert (Geometry.fromGeoJSON(this.testData2).equals(Geometry.fromGeoJSON(this.testData2)));
    assert !(Geometry.fromGeoJSON(this.testData2).equals(Geometry.fromGeoJSON(testData2)));
  }
}
