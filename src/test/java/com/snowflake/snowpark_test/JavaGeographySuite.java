package com.snowflake.snowpark_test;

import com.snowflake.snowpark_java.types.Geography;
import org.junit.Test;

public class JavaGeographySuite {

  private final String testData =
      "{\"geometry\":{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}}";

  @Test
  public void testRoundTrip() {
    assert (Geography.fromGeoJSON(testData).asGeoJSON().equals(testData));
    assert (Geography.fromGeoJSON(testData).toString().equals(testData));
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
  }
}
