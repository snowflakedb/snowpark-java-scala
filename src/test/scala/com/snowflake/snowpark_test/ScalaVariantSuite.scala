package com.snowflake.snowpark_test

import com.snowflake.snowpark.types.{Geography, Variant}
import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import java.io.UncheckedIOException
import java.sql.{Date, Time, Timestamp}

class ScalaVariantSuite extends FunSuite {
  test("scala constructor and extension functions") {
    assert(new Variant(1.1).asDouble() == 1.1)
    assert(new Variant(1.2f).asFloat() == 1.2f)
    assert(new Variant(1234L).asLong() == 1234L)
    assert(new Variant(1.toShort).asShort() == 1.toShort)
    assert(new Variant(BigDecimal(1.23)).asBigDecimal == BigDecimal(1.23))
    assert(new Variant(BigInt(12345)).asBigInt == BigInt(12345))
    assert(new Variant(true).asBoolean())
    assert(new Variant("test").asString() == "test")
    assert(
      new Variant(Array(1.toByte, 2.toByte, 3.toByte))
        .asBinary()
        .sameElements(Array(1.toByte, 2.toByte, 3.toByte)))
    val arr = new Variant(Array(true, 1)).asArray()
    assert(arr(0).asBoolean())
    assert(arr(1).asInt() == 1)
    assert(new Variant(Time.valueOf("01:02:03")).asTime() == Time.valueOf("01:02:03"))
    assert(new Variant(Date.valueOf("2020-10-10")).asDate() == Date.valueOf("2020-10-10"))
    assert(
      new Variant(Timestamp.valueOf("2020-10-10 01:02:03")).asTimestamp() == Timestamp.valueOf(
        "2020-10-10 01:02:03"))
    val seq = new Variant(Seq(1, 2, 3)).asSeq()
    assert(seq.head.asInt() == 1)
    assert(seq(1).asInt() == 2)
    assert(seq(2).asInt() == 3)
    val map = new Variant(Map("a" -> 1, "b" -> 2)).asMap()
    assert(map("a").asInt() == 1)
    assert(map("b").asInt() == 2)
    // test other
    assert(new Variant(BigInt(123).bigInteger).asBigInt == BigInt(123))
  }

  test("number conversion") {
    val vDouble = new Variant(1.1d)
    val vFloat = new Variant(1.1f)
    val vLong = new Variant(1L)
    val vInt = new Variant(1)
    val vShort = new Variant(1.toShort)
    val vBigDecimal = new Variant(BigDecimal.valueOf(1.1))
    val vBigInt = new Variant(BigInt(1))

    assert(vDouble.asDouble() == 1.1d)
    assert(vDouble.asFloat() == 1.1f)
    assert(vDouble.asLong() == 1L)
    assert(vDouble.asInt() == 1)
    assert(vDouble.asShort() == 1.toShort)
    assert(vDouble.asBigDecimal().equals(BigDecimal("1.1")))
    assert(vDouble.asBigInt().equals(BigInt("1")))
    assert(vDouble.asTimestamp().equals(new Timestamp(1L)))
    assert(vDouble.asString().equals("1.1"))
    assert(vDouble.asJsonString().equals("1.1"))

    assert((vFloat.asDouble() - 1.1d).abs < 0.0000001);
    assert(vFloat.asFloat() == 1.1f)
    assert(vFloat.asLong() == 1L)
    assert(vFloat.asInt() == 1)
    assert(vFloat.asShort() == 1.toShort)
    assert((vFloat.asBigDecimal() - BigDecimal("1.1")).abs.doubleValue < 0.0000001)
    assert(vFloat.asBigInt() == BigInt("1"))
    assert(vFloat.asTimestamp().equals(new Timestamp(1L)))
    assert(vFloat.asString().equals("1.1"))
    assert(vFloat.asJsonString().equals("1.1"))

    assert(vLong.asDouble() == 1d)
    assert(vLong.asFloat() == 1f)
    assert(vLong.asLong() == 1L)
    assert(vLong.asInt() == 1)
    assert(vLong.asShort() == 1.toShort)
    assert(vLong.asBigDecimal() == BigDecimal("1"))
    assert(vLong.asBigInt() == BigInt("1"))
    assert(vLong.asTimestamp().equals(new Timestamp(1L)))
    assert(vLong.asString().equals("1"))
    assert(vLong.asJsonString().equals("1"))

    assert(vInt.asDouble() == 1d)
    assert(vInt.asFloat() == 1f)
    assert(vInt.asLong() == 1L)
    assert(vInt.asInt() == 1)
    assert(vInt.asShort() == 1.toShort)
    assert(vInt.asBigDecimal() == BigDecimal("1"))
    assert(vInt.asBigInt() == BigInt("1"))
    assert(vInt.asTimestamp().equals(new Timestamp(1L)))
    assert(vInt.asString().equals("1"))
    assert(vInt.asJsonString().equals("1"))

    assert(vShort.asDouble() == 1d)
    assert(vShort.asFloat() == 1f)
    assert(vShort.asLong() == 1L)
    assert(vShort.asInt() == 1)
    assert(vShort.asShort() == 1.toShort)
    assert(vShort.asBigDecimal() == BigDecimal("1"))
    assert(vShort.asBigInt() == BigInt("1"))
    assert(vShort.asTimestamp().equals(new Timestamp(1L)))
    assert(vShort.asString().equals("1"))
    assert(vShort.asJsonString().equals("1"))

    assert(vBigDecimal.asDouble() == 1.1d)
    assert(vBigDecimal.asFloat() == 1.1f)
    assert(vBigDecimal.asLong() == 1L)
    assert(vBigDecimal.asInt() == 1)
    assert(vBigDecimal.asShort() == 1.toShort)
    assert(vBigDecimal.asBigDecimal() == BigDecimal("1.1"))
    assert(vBigDecimal.asBigInt() == BigInt("1"))
    assert(vBigDecimal.asTimestamp().equals(new Timestamp(1L)))
    assert(vBigDecimal.asString().equals("1.1"))
    assert(vBigDecimal.asJsonString().equals("1.1"))

    assert(vBigInt.asDouble() == 1d)
    assert(vBigInt.asFloat() == 1f)
    assert(vBigInt.asLong() == 1L)
    assert(vBigInt.asInt() == 1)
    assert(vBigInt.asShort() == 1.toShort)
    assert(vBigInt.asBigDecimal() == BigDecimal("1"))
    assert(vBigInt.asBigInt() == BigInt("1"))
    assert(vBigInt.asTimestamp().equals(new Timestamp(1L)))
    assert(vBigInt.asString().equals("1"))
    assert(vBigInt.asJsonString().equals("1"))
  }

  test("boolean conversion") {
    var vBoolean = new Variant(true)
    assert(vBoolean.asDouble() == 1d)
    assert(vBoolean.asFloat() == 1f)
    assert(vBoolean.asLong() == 1L)
    assert(vBoolean.asInt() == 1)
    assert(vBoolean.asShort() == 1.toShort);
    assert(vBoolean.asBigDecimal() == BigDecimal("1"))
    assert(vBoolean.asBigInt() == BigInt("1"))
    assert(vBoolean.asString().equals("true"))
    assert(vBoolean.asJsonString().equals("true"))

    vBoolean = new Variant(false)
    assert(vBoolean.asDouble() == 0d)
    assert(vBoolean.asFloat() == 0f)
    assert(vBoolean.asLong() == 0L)
    assert(vBoolean.asInt() == 0)
    assert(vBoolean.asShort() == 0.toShort)
    assert(vBoolean.asBigDecimal() == BigDecimal("0"))
    assert(vBoolean.asBigInt() == BigInt("0"))
    assert(vBoolean.asString().equals("false"))
    assert(vBoolean.asJsonString().equals("false"))
  }

  test("binary Conversion") {
    val vBinary = new Variant(Array[Byte](0x50, 0x4f, 0x1c))
    assert(vBinary.asBinary() sameElements Array[Byte](0x50, 0x4f, 0x1c))
    assert(vBinary.asString().equals("504f1c"))
    assert(vBinary.asJsonString().equals("\"504f1c\""))
  }

  test("time conversion") {
    val vTime = new Variant(Time.valueOf("01:02:03"))
    assert(vTime.asTime().equals(Time.valueOf("01:02:03")))
    assert(vTime.asString().equals("01:02:03"))
    assert(vTime.asJsonString().equals("\"01:02:03\""))
  }

  test("data conversion") {
    val vDate = new Variant(Date.valueOf("2020-10-10"))
    assert(vDate.asDate().equals(Date.valueOf("2020-10-10")))
    assert(vDate.asString().equals("2020-10-10"))
    assert(vDate.asJsonString().equals("\"2020-10-10\""))
  }

  test("timestamp conversion") {
    val vTimestamp = new Variant(Timestamp.valueOf("2020-10-10 01:02:03"))
    assert(vTimestamp.asTimestamp().equals(Timestamp.valueOf("2020-10-10 " + "01:02:03")))
    assert(vTimestamp.asString().equals("2020-10-10 01:02:03.0"))
    assert(vTimestamp.asJsonString().equals("\"2020-10-10 01:02:03.0\""))
  }

  test("string conversion") {
    var vString = new Variant("1")
    assert(vString.asDouble() == 1d)
    assert(vString.asFloat() == 1f)
    assert(vString.asLong() == 1L)
    assert(vString.asInt() == 1)
    assert(vString.asShort() == 1.toShort)
    assert(vString.asBigDecimal() == BigDecimal("1"))
    assert(vString.asBigInt() == BigInt("1"))
    assert(vString.asTimestamp().equals(new Timestamp(1L)))
    assert(vString.asString().equals("1"))
    assert(vString.asJsonString().equals("1"))

    vString = new Variant("true")
    assert(vString.asDouble() == 1d)
    assert(vString.asFloat() == 1f)
    assert(vString.asLong() == 1L)
    assert(vString.asInt() == 1)
    assert(vString.asShort() == 1.toShort)
    assert(vString.asBigDecimal() == BigDecimal("1"))
    assert(vString.asBigInt() == BigInt("1"))
    assert(vString.asString().equals("true"))
    assert(vString.asJsonString().equals("true"))

    vString = new Variant("01:02:03")
    assert(vString.asTime().equals(Time.valueOf("01:02:03")))
    assert(vString.asString().equals("01:02:03"))
    assert(vString.asJsonString().equals("\"01:02:03\""))

    vString = new Variant("2020-10-10")
    assert(vString.asDate().equals(Date.valueOf("2020-10-10")))
    assert(vString.asString().equals("2020-10-10"))
    assert(vString.asJsonString().equals("\"2020-10-10\""))

    vString = new Variant("2020-10-10 01:02:03")
    assert(vString.asTimestamp().equals(Timestamp.valueOf("2020-10-10 " + "01:02:03")))
    assert(vString.asString().equals("2020-10-10 01:02:03"))
    assert(vString.asJsonString().equals("\"2020-10-10 01:02:03\""))

    vString = new Variant("\"01:02:03\"")
    assert(vString.asTime().equals(Time.valueOf("01:02:03")))
    assert(vString.asString().equals("01:02:03"))
    assert(vString.asJsonString().equals("\"01:02:03\""))

    vString = new Variant("\"2020-10-10\"")
    assert(vString.asDate().equals(Date.valueOf("2020-10-10")))
    assert(vString.asString().equals("2020-10-10"))
    assert(vString.asJsonString().equals("\"2020-10-10\""))

    vString = new Variant("\"2020-10-10 01:02:03\"")
    assert(vString.asTimestamp().equals(Timestamp.valueOf("2020-10-10 " + "01:02:03")))
    assert(vString.asString().equals("2020-10-10 01:02:03"))
    assert(vString.asJsonString().equals("\"2020-10-10 01:02:03\""))

    vString = new Variant("504f1c")
    assert(vString.asBinary() sameElements Array[Byte](0x50, 0x4f, 0x1c))
    assert(vString.asString().equals("504f1c"))
    assert(vString.asJsonString().equals("\"504f1c\""))

    vString = new Variant("\"504f1c\"")
    assert(vString.asBinary() sameElements Array[Byte](0x50, 0x4f, 0x1c))
    assert(vString.asString().equals("504f1c"))
    assert(vString.asJsonString().equals("\"504f1c\""))
  }

  test("string json parsing") {
    var vString = new Variant("{\"a\": [1, 2], \"b\": \"c\"}")
    val resultMap = Map[String, Variant]("a" -> new Variant("[1,2]"), "b" -> new Variant("c"))
    assert(vString.asMap().equals(resultMap))
    assert(vString.asMap()("a").asString() == "[1,2]")
    assert(vString.asMap()("a").asJsonString() == "[1,2]")

    vString = new Variant("[{\"a\": [1, 2], \"b\": \"c\"}]");
    assert(vString.asSeq() == Seq(new Variant("{\"a\":[1,2]," + "\"b\":\"c\"}")))
    assert(vString.asSeq().head.asMap()("b") == new Variant("\"c" + "\""))
    assert(vString.asSeq().head.asMap()("b").asString() == "c")
    assert(vString.asSeq().head.asMap()("b").asJsonString() == "\"c\"")
    assert(vString.asArray()(0).asMap()("b") == new Variant("\"c\""))
    assert(vString.asArray()(0).asMap()("b").asString() == "c")
    assert(vString.asArray()(0).asMap()("b").asJsonString() == "\"c\"")

    val v1 = new Variant("{\"a\": null}")
    val v2 = new Variant("{\"a\": \"foo\"}")
    assert(v1.asMap()("a").asString() == "null")
    assert(v2.asMap()("a").asString() == "foo")
    assert(v1.asMap()("a").asJsonString() == "null")
    assert(v2.asMap()("a").asJsonString() == "\"foo\"")
  }

  test("map and array") {
    val arr1 = Array("a", "b")
    assert(new Variant(arr1).asJsonString().equals("[\"a\",\"b\"]"))
    val arr2 = Array(new Variant(1), new Variant("a"))
    assert(new Variant(arr2).asJsonString().equals("[1,\"a\"]"))
    val arr3 = Array(Geography.fromGeoJSON("point(10 10)"), Geography.fromGeoJSON("point(20 20)"))
    assert(new Variant(arr3).asJsonString().equals("[\"point(10 10)\"," + "\"point(20 20)\"]"))

    val map1 = Map("a" -> "1", "b" -> "1")
    assert(new Variant(map1).asJsonString().equals("{\"a\":\"1\"," + "\"b\":\"1\"}"))
    val map2 = Map("a" -> new Variant(1), "b" -> new Variant("a"))
    assert(new Variant(map2).asJsonString().equals("{\"a\":1,\"b\":\"a\"}"))
    val map3 = Map(
      "a" -> Geography.fromGeoJSON("Point(10 10)"),
      "b" -> Geography.fromGeoJSON("Point(20 20)"))
    assert(
      new Variant(map3)
        .asJsonString()
        .equals("{\"a\":\"Point(10 10)\"," + "\"b\":\"Point(20 20)\"}"));
  }

  test("negative test for conversion") {
    assertThrows[UncheckedIOException] {
      new Variant(1).asBoolean()
    }
  }

  test("hash code") {
    assert(new Variant("123").hashCode() == new Variant("123").hashCode())
    assert(new Variant("123").hashCode() != new Variant(123).hashCode())
  }

  test("equals") {
    val v1 = new Variant(123);
    val v2 = new Variant(123);
    val v3 = new Variant(223);

    assert(v1 == v2)
    assert(v1 != v3)

    assert(v1 != "aa")

    assert(v1.toString() == "123")
  }
}
