package com.snowflake.snowpark_test

import org.scalatest.FunSuite
import com.snowflake.snowpark.internal.JavaUtils._
import com.snowflake.snowpark.types.Variant
import com.snowflake.snowpark_java.types.{Variant => JavaVariant}
import scala.collection.JavaConverters._
import java.util

// test UDF utils functions
// those functions work on server side.
// can't be detected by test coverage report.
class JavaUtilsSuite extends FunSuite {

  test("geography to string") {
    val data = "{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}"
    assert(geographyToString(com.snowflake.snowpark.types.Geography.fromGeoJSON(data)) == data)
    assert(geographyToString(com.snowflake.snowpark_java.types.Geography.fromGeoJSON(data)) == data)
  }

  test("geometry to string") {
    val data =
      "{\"coordinates\": [2.000000000000000e+01,4.000000000000000e+01],\"type\": \"Point\"}"
    assert(geometryToString(com.snowflake.snowpark.types.Geometry.fromGeoJSON(data)) == data)
    assert(geometryToString(com.snowflake.snowpark_java.types.Geometry.fromGeoJSON(data)) == data)
  }

  test("string to geography") {
    val data = "{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}"
    assert(stringToGeography(data).isInstanceOf[com.snowflake.snowpark.types.Geography])
    assert(stringToJavaGeography(data).isInstanceOf[com.snowflake.snowpark_java.types.Geography])
  }

  test("string to geometry") {
    val data = "{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}"
    assert(stringToGeometry(data).isInstanceOf[com.snowflake.snowpark.types.Geometry])
    assert(stringToJavaGeometry(data).isInstanceOf[com.snowflake.snowpark_java.types.Geometry])
  }

  test("variant to string") {
    assert(variantToString(new com.snowflake.snowpark.types.Variant(1)) == "1")
    assert(variantToString(new com.snowflake.snowpark_java.types.Variant(1)) == "1")
  }

  test("string to variant") {
    assert(stringToJavaVariant("1").isInstanceOf[com.snowflake.snowpark_java.types.Variant])
    assert(stringToVariant("1").isInstanceOf[com.snowflake.snowpark.types.Variant])
  }

  test("variant array to string array") {
    assert(
      variantArrayToStringArray(Array(new com.snowflake.snowpark.types.Variant(1)))
        .isInstanceOf[Array[String]]
    )
    assert(
      variantArrayToStringArray(Array(new com.snowflake.snowpark_java.types.Variant(1)))
        .isInstanceOf[Array[String]]
    )
  }

  test("string array to variant array") {
    assert(
      stringArrayToVariantArray(Array("1"))
        .isInstanceOf[Array[com.snowflake.snowpark.types.Variant]]
    )
    assert(
      stringArrayToJavaVariantArray(Array("1"))
        .isInstanceOf[Array[com.snowflake.snowpark_java.types.Variant]]
    )
  }

  test("string map to variant map") {
    assert(
      stringMapToVariantMap(new util.HashMap[String, String]())
        .isInstanceOf[scala.collection.mutable.Map[String, com.snowflake.snowpark.types.Variant]]
    )
    assert(
      stringMapToJavaVariantMap(new util.HashMap[String, String]())
        .isInstanceOf[util.Map[String, com.snowflake.snowpark_java.types.Variant]]
    )
  }

  test("variant map to string map") {
    assert(
      variantMapToStringMap(
        collection.mutable.Map.empty[String, com.snowflake.snowpark.types.Variant]
      )
        .isInstanceOf[util.Map[String, String]]
    )

    assert(
      javaVariantMapToStringMap(
        new util.HashMap[String, com.snowflake.snowpark_java.types.Variant]()
      )
        .isInstanceOf[util.Map[String, String]]
    )
  }

  test("variant to string array") {
    assert(variantToStringArray(null.asInstanceOf[Variant]) == null)
    assert(variantToStringArray(new Variant(Array("a", "b"))).sameElements(Array("a", "b")))
    assert(
      variantToStringArray(new Variant(Array(new Variant("a"), new Variant("b"))))
        .sameElements(Array("a", "b"))
    )
  }

  test("java variant to string array") {
    assert(variantToStringArray(null.asInstanceOf[JavaVariant]) == null)
    assert(variantToStringArray(new JavaVariant(Array("a", "b"))).sameElements(Array("a", "b")))
    assert(
      variantToStringArray(new JavaVariant(Array(new JavaVariant("a"), new JavaVariant("b"))))
        .sameElements(Array("a", "b"))
    )
  }

  test("variant to string map") {
    assert(variantToStringMap(null.asInstanceOf[Variant]) == null)
    val map1 = variantToStringMap(new Variant(Map("a" -> "b")))
    assert(map1.keySet().size() == 1 && map1.containsKey("a") && map1.get("a").equals("b"))
    val map2 = variantToStringMap(new Variant(Map("a" -> new Variant("b"))))
    assert(map2.keySet().size() == 1 && map2.containsKey("a") && map2.get("a").equals("b"))
  }

  test("java variant to string map") {
    assert(variantToStringMap(null.asInstanceOf[JavaVariant]) == null)
    val map1 = variantToStringMap(new JavaVariant(Map("a" -> "b").asJava))
    assert(map1.keySet().size() == 1 && map1.containsKey("a") && map1.get("a").equals("b"))
    val map2 = variantToStringMap(new JavaVariant(Map("a" -> new JavaVariant("b")).asJava))
    assert(map2.keySet().size() == 1 && map2.containsKey("a") && map2.get("a").equals("b"))
  }

  test("javaMapToScalaWithVariantConversion") {
    // Variant map
    val variantMap: util.Map[String, JavaVariant] = new util.HashMap
    variantMap.put("a", new JavaVariant("av"))
    variantMap.put("b", new JavaVariant("bv"))
    variantMap.put("c", null)
    val resultMap: Map[Any, Any] =
      javaMapToScalaWithVariantConversion(variantMap.asInstanceOf[util.Map[Object, Object]])
    assert(resultMap.size == 3)
    val v1 = resultMap("a").asInstanceOf[Variant]
    assert(v1.asString().equals("av"))
    val v2 = resultMap("b").asInstanceOf[Variant]
    assert(v2.asString().equals("bv"))
    assert(resultMap("c") == null)

    // String map
    val stringMap: util.Map[String, String] = new util.HashMap
    stringMap.put("a", "av")
    stringMap.put("b", "bv")
    stringMap.put("c", null)
    val resultMap2: Map[Any, Any] =
      javaMapToScalaWithVariantConversion(stringMap.asInstanceOf[util.Map[Object, Object]])
    assert(resultMap2.size == 3)
    assert(resultMap2("a").asInstanceOf[String].equals("av"))
    assert(resultMap2("b").asInstanceOf[String].equals("bv"))
    assert(resultMap2("c") == null)
  }

  test("scalaMapToJavaWithVariantConversion") {
    // Variant Map
    val variantMap: Map[String, Variant] =
      Map("a" -> new Variant("av"), "b" -> new Variant("bv"), "c" -> null)
    val javaMap =
      scalaMapToJavaWithVariantConversion(variantMap.asInstanceOf[Map[Any, Any]])
    assert(javaMap.size() == 3)
    val v1 = javaMap.get("a").asInstanceOf[JavaVariant]
    assert(v1.asString().equals("av"))
    val v2 = javaMap.get("b").asInstanceOf[JavaVariant]
    assert(v2.asString().equals("bv"))
    assert(javaMap.get("c") == null)

    // String Map
    val stringMap: Map[String, String] = Map("a" -> "av", "b" -> "bv", "c" -> null)
    val javaMap2 =
      scalaMapToJavaWithVariantConversion(stringMap.asInstanceOf[Map[Any, Any]])
    assert(javaMap2.size() == 3)
    assert(javaMap2.get("a").asInstanceOf[String].equals("av"))
    assert(javaMap2.get("b").asInstanceOf[String].equals("bv"))
    assert(javaMap2.get("c") == null)
  }
}
