package com.snowflake.snowpark_test;

import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.snowpark_java.types.Geography;
import com.snowflake.snowpark_java.types.InternalUtils;
import com.snowflake.snowpark_java.types.Variant;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JavaVariantSuite {
  @Test
  public void variantSelfConversion() {
    assert (new Variant(1.1d).asDouble() == 1.1d);
    assert (new Variant(1.1f).asFloat() == 1.1f);
    assert (new Variant(1L).asLong() == 1L);
    assert (new Variant(1).asInt() == 1);
    assert (new Variant((short) 1).asShort() == (short) 1);
    assert (new Variant(new BigDecimal("1.1")).asBigDecimal().equals(new BigDecimal("1.1")));
    assert (new Variant(new BigInteger("11")).asBigInteger().equals(new BigInteger("11")));
    assert (new Variant(true).asBoolean());
    assert (new Variant("abc").asString().equals("abc"));
    assert (new Variant("abc").asJsonString().equals("\"abc\""));
    assert (new Variant("\"abc\"").asString().equals("abc"));
    assert (new Variant("\"abc\"").asJsonString().equals("\"abc\""));
    assert (Arrays.equals(
        new Variant(new byte[] {0x50, 0x4f}).asBinary(), new byte[] {0x50, 0x4f}));
    assert (new Variant(Time.valueOf("01:02:03")).asTime().equals(Time.valueOf("01:02:03")));
    assert (new Variant(Date.valueOf("2020-10-10")).asDate().equals(Date.valueOf("2020-10-10")));
    assert (new Variant(Timestamp.valueOf("2020-10-10 01:02:03"))
        .asTimestamp()
        .equals(Timestamp.valueOf("2020-10-10 01:02:03")));

    assert (new Variant(Arrays.asList(1, 2))
        .asList()
        .equals(Arrays.asList(new Variant("1"), new Variant("2"))));
    assert (new Variant(Arrays.asList(1, 2))
        .asList()
        .equals(Arrays.asList(new Variant(1), new Variant(2))));

    Map<String, String> inputMap = new HashMap<>();
    inputMap.put("key1", "value1");
    inputMap.put("key2", "value2");
    Map<String, Variant> resultMap = new HashMap<>();
    resultMap.put("key1", new Variant("value1"));
    resultMap.put("key2", new Variant("value2"));
    assert (new Variant(inputMap).asMap().equals(resultMap));
  }

  @Test
  public void numberConversion() {
    Variant vDouble = new Variant(1.1d);
    Variant vFloat = new Variant(1.1f);
    Variant vLong = new Variant(1L);
    Variant vInt = new Variant(1);
    Variant vShort = new Variant((short) 1);
    Variant vBigDecimal = new Variant(new BigDecimal("1.1"));
    Variant vBigInteger = new Variant(new BigInteger("1"));

    assert (vDouble.asDouble() == 1.1d);
    assert (vDouble.asFloat() == 1.1f);
    assert (vDouble.asLong() == 1L);
    assert (vDouble.asInt() == 1);
    assert (vDouble.asShort() == (short) 1);
    assert (vDouble.asBigDecimal().equals(new BigDecimal("1.1")));
    assert (vDouble.asBigInteger().equals(new BigInteger("1")));
    assert (vDouble.asTimestamp().equals(new Timestamp(1L)));
    assert (vDouble.asString().equals("1.1"));
    assert (vDouble.asJsonString().equals("1.1"));

    assert (Math.abs(vFloat.asDouble() - 1.1d) < 0.0000001);
    assert (vFloat.asFloat() == 1.1f);
    assert (vFloat.asLong() == 1L);
    assert (vFloat.asInt() == 1);
    assert (vFloat.asShort() == (short) 1);
    assert (vFloat.asBigDecimal().subtract(new BigDecimal("1.1")).abs().doubleValue() < 0.0000001);
    assert (vFloat.asBigInteger().equals(new BigInteger("1")));
    assert (vFloat.asTimestamp().equals(new Timestamp(1L)));
    assert (vFloat.asString().equals("1.1"));
    assert (vFloat.asJsonString().equals("1.1"));

    assert (vLong.asDouble() == 1d);
    assert (vLong.asFloat() == 1f);
    assert (vLong.asLong() == 1L);
    assert (vLong.asInt() == 1);
    assert (vLong.asShort() == (short) 1);
    assert (vLong.asBigDecimal().equals(new BigDecimal("1")));
    assert (vLong.asBigInteger().equals(new BigInteger("1")));
    assert (vLong.asTimestamp().equals(new Timestamp(1L)));
    assert (vLong.asString().equals("1"));
    assert (vLong.asJsonString().equals("1"));

    assert (vInt.asDouble() == 1d);
    assert (vInt.asFloat() == 1f);
    assert (vInt.asLong() == 1L);
    assert (vInt.asInt() == 1);
    assert (vInt.asShort() == (short) 1);
    assert (vInt.asBigDecimal().equals(new BigDecimal("1")));
    assert (vInt.asBigInteger().equals(new BigInteger("1")));
    assert (vInt.asTimestamp().equals(new Timestamp(1L)));
    assert (vInt.asString().equals("1"));
    assert (vInt.asJsonString().equals("1"));

    assert (vShort.asDouble() == 1d);
    assert (vShort.asFloat() == 1f);
    assert (vShort.asLong() == 1L);
    assert (vShort.asInt() == 1);
    assert (vShort.asShort() == (short) 1);
    assert (vShort.asBigDecimal().equals(new BigDecimal("1")));
    assert (vShort.asBigInteger().equals(new BigInteger("1")));
    assert (vShort.asTimestamp().equals(new Timestamp(1L)));
    assert (vShort.asString().equals("1"));
    assert (vShort.asJsonString().equals("1"));

    assert (vBigDecimal.asDouble() == 1.1d);
    assert (vBigDecimal.asFloat() == 1.1f);
    assert (vBigDecimal.asLong() == 1L);
    assert (vBigDecimal.asInt() == 1);
    assert (vBigDecimal.asShort() == (short) 1);
    assert (vBigDecimal.asBigDecimal().equals(new BigDecimal("1.1")));
    assert (vBigDecimal.asBigInteger().equals(new BigInteger("1")));
    assert (vBigDecimal.asTimestamp().equals(new Timestamp(1L)));
    assert (vBigDecimal.asString().equals("1.1"));
    assert (vBigDecimal.asJsonString().equals("1.1"));

    assert (vBigInteger.asDouble() == 1d);
    assert (vBigInteger.asFloat() == 1f);
    assert (vBigInteger.asLong() == 1L);
    assert (vBigInteger.asInt() == 1);
    assert (vBigInteger.asShort() == (short) 1);
    assert (vBigInteger.asBigDecimal().equals(new BigDecimal("1")));
    assert (vBigInteger.asBigInteger().equals(new BigInteger("1")));
    assert (vBigInteger.asTimestamp().equals(new Timestamp(1L)));
    assert (vBigInteger.asString().equals("1"));
    assert (vBigInteger.asJsonString().equals("1"));
  }

  @Test
  public void booleanConversion() {
    Variant vBoolean = new Variant(true);
    assert (vBoolean.asDouble() == 1d);
    assert (vBoolean.asFloat() == 1f);
    assert (vBoolean.asLong() == 1L);
    assert (vBoolean.asInt() == 1);
    assert (vBoolean.asShort() == (short) 1);
    assert (vBoolean.asBigDecimal().equals(new BigDecimal("1")));
    assert (vBoolean.asBigInteger().equals(new BigInteger("1")));
    assert (vBoolean.asString().equals("true"));
    assert (vBoolean.asJsonString().equals("true"));

    vBoolean = new Variant(false);
    assert (vBoolean.asDouble() == 0d);
    assert (vBoolean.asFloat() == 0f);
    assert (vBoolean.asLong() == 0L);
    assert (vBoolean.asInt() == 0);
    assert (vBoolean.asShort() == (short) 0);
    assert (vBoolean.asBigDecimal().equals(new BigDecimal("0")));
    assert (vBoolean.asBigInteger().equals(new BigInteger("0")));
    assert (vBoolean.asString().equals("false"));
    assert (vBoolean.asJsonString().equals("false"));
  }

  @Test
  public void binaryConversion() {
    Variant vBinary = new Variant(new byte[] {0x50, 0x4f, 0x1c});
    assert (Arrays.equals(vBinary.asBinary(), new byte[] {0x50, 0x4f, 0x1c}));
    assert (vBinary.asString().equals("504f1c"));
    assert (vBinary.asJsonString().equals("\"504f1c\""));
  }

  @Test
  public void timeConversion() {
    Variant vTime = new Variant(Time.valueOf("01:02:03"));
    assert (vTime.asTime().equals(Time.valueOf("01:02:03")));
    assert (vTime.asString().equals("01:02:03"));
    assert (vTime.asJsonString().equals("\"01:02:03\""));
  }

  @Test
  public void dateConversion() {
    Variant vDate = new Variant(Date.valueOf("2020-10-10"));
    assert (vDate.asDate().equals(Date.valueOf("2020-10-10")));
    assert (vDate.asString().equals("2020-10-10"));
    assert (vDate.asJsonString().equals("\"2020-10-10\""));
  }

  @Test
  public void timestampConversion() {
    Variant vTimestamp = new Variant(Timestamp.valueOf("2020-10-10 01:02:03"));
    assert (vTimestamp.asTimestamp().equals(Timestamp.valueOf("2020-10-10 " + "01:02:03")));
    assert (vTimestamp.asString().equals("2020-10-10 01:02:03.0"));
    assert (vTimestamp.asJsonString().equals("\"2020-10-10 01:02:03.0\""));
  }

  @Test
  public void stringConversion() {
    Variant vString = new Variant("1");
    assert (vString.asDouble() == 1d);
    assert (vString.asFloat() == 1f);
    assert (vString.asLong() == 1L);
    assert (vString.asInt() == 1);
    assert (vString.asShort() == (short) 1);
    assert (vString.asBigDecimal().equals(new BigDecimal("1")));
    assert (vString.asBigInteger().equals(new BigInteger("1")));
    assert (vString.asTimestamp().equals(new Timestamp(1L)));
    assert (vString.asString().equals("1"));
    assert (vString.asJsonString().equals("1"));

    vString = new Variant("true");
    assert (vString.asDouble() == 1d);
    assert (vString.asFloat() == 1f);
    assert (vString.asLong() == 1L);
    assert (vString.asInt() == 1);
    assert (vString.asShort() == (short) 1);
    assert (vString.asBigDecimal().equals(new BigDecimal("1")));
    assert (vString.asBigInteger().equals(new BigInteger("1")));
    assert (vString.asString().equals("true"));
    assert (vString.asJsonString().equals("true"));

    vString = new Variant("01:02:03");
    assert (vString.asTime().equals(Time.valueOf("01:02:03")));
    assert (vString.asString().equals("01:02:03"));
    assert (vString.asJsonString().equals("\"01:02:03\""));

    vString = new Variant("2020-10-10");
    assert (vString.asDate().equals(Date.valueOf("2020-10-10")));
    assert (vString.asString().equals("2020-10-10"));
    assert (vString.asJsonString().equals("\"2020-10-10\""));

    vString = new Variant("2020-10-10 01:02:03");
    assert (vString.asTimestamp().equals(Timestamp.valueOf("2020-10-10 " + "01:02:03")));
    assert (vString.asString().equals("2020-10-10 01:02:03"));
    assert (vString.asJsonString().equals("\"2020-10-10 01:02:03\""));

    vString = new Variant("\"01:02:03\"");
    assert (vString.asTime().equals(Time.valueOf("01:02:03")));
    assert (vString.asString().equals("01:02:03"));
    assert (vString.asJsonString().equals("\"01:02:03\""));

    vString = new Variant("\"2020-10-10\"");
    assert (vString.asDate().equals(Date.valueOf("2020-10-10")));
    assert (vString.asString().equals("2020-10-10"));
    assert (vString.asJsonString().equals("\"2020-10-10\""));

    vString = new Variant("\"2020-10-10 01:02:03\"");
    assert (vString.asTimestamp().equals(Timestamp.valueOf("2020-10-10 " + "01:02:03")));
    assert (vString.asString().equals("2020-10-10 01:02:03"));
    assert (vString.asJsonString().equals("\"2020-10-10 01:02:03\""));

    vString = new Variant("504f1c");
    assert (Arrays.equals(vString.asBinary(), new byte[] {0x50, 0x4f, 0x1c}));
    assert (vString.asString().equals("504f1c"));
    assert (vString.asJsonString().equals("\"504f1c\""));

    vString = new Variant("\"504f1c\"");
    assert (Arrays.equals(vString.asBinary(), new byte[] {0x50, 0x4f, 0x1c}));
    assert (vString.asString().equals("504f1c"));
    assert (vString.asJsonString().equals("\"504f1c\""));
  }

  @Test
  public void stringJsonParsing() {
    Variant vString = new Variant("{\"a\": [1, 2], \"b\": \"c\"}");
    Map<String, Variant> resultMap =
        new HashMap<String, Variant>() {
          {
            put("a", new Variant("[1,2]"));
            put("b", new Variant("c"));
          }
        };
    assert (vString.asMap().equals(resultMap));
    assert (vString.asMap().get("a").asString().equals("[1,2]"));
    assert (vString.asMap().get("a").asJsonString().equals("[1,2]"));

    vString = new Variant("[{\"a\": [1, 2], \"b\": \"c\"}]");
    assert (vString.asList().equals(Arrays.asList(new Variant("{\"a\":[1,2]," + "\"b\":\"c\"}"))));
    assert (vString.asList().get(0).asMap().get("b").equals(new Variant("\"c" + "\"")));
    assert (vString.asList().get(0).asMap().get("b").asString().equals("c"));
    assert (vString.asList().get(0).asMap().get("b").asJsonString().equals("\"c\""));
    assert (vString.asArray()[0].asMap().get("b").equals(new Variant("\"c\"")));
    assert (vString.asArray()[0].asMap().get("b").asString().equals("c"));
    assert (vString.asArray()[0].asMap().get("b").asJsonString().equals("\"c" + "\""));
    assert vString.asString().equals("[{\"a\":[1,2],\"b\":\"c\"}]");

    Variant v1 = new Variant("{\"a\": null}");
    Variant v2 = new Variant("{\"a\": \"foo\"}");
    assert (v1.asMap().get("a").asString().equals("null"));
    assert (v2.asMap().get("a").asString().equals("foo"));
    assert (v1.asMap().get("a").asJsonString().equals("null"));
    assert (v2.asMap().get("a").asJsonString().equals("\"foo\""));
    assert (v1.asString().equals("{\"a\":null}"));
  }

  @Test
  public void javaVariantConversion() {
    // Java variant to Scala Variant
    assert InternalUtils.toScalaVariant(null) == null;
    Variant jv = new Variant("{\"a\": [1, 2], \"b\": \"c\"}");
    com.snowflake.snowpark.types.Variant result = InternalUtils.toScalaVariant(jv);
    assert (result.asMap().size() == 2);
    assert (result.asMap().get("a").get().asString().equals("[1,2]"));
    assert (result.asMap().get("a").get().asJsonString().equals("[1,2]"));

    // Scala variant to Java Variant
    com.snowflake.snowpark.types.Variant sv =
        new com.snowflake.snowpark.types.Variant("{\"a\": [1, 2], \"b\": \"c\"}");
    assert InternalUtils.createVariant(null) == null;
    Variant jResult = InternalUtils.createVariant(sv);
    assert (jResult.asMap().size() == 2);
    assert (jResult.asMap().get("a").asString().equals("[1,2]"));
    assert (jResult.asMap().get("a").asJsonString().equals("[1,2]"));
  }

  @Test
  public void mapAndArray() {
    String[] arr1 = {"a", "b"};
    assert (new Variant(arr1).asJsonString().equals("[\"a\",\"b\"]"));
    Variant[] arr2 = {new Variant(1), new Variant("a")};
    assert (new Variant(arr2).asJsonString().equals("[1,\"a\"]"));
    Geography[] arr3 = {
      Geography.fromGeoJSON("point(10 10)"), Geography.fromGeoJSON("point(20 20)")
    };
    assert (new Variant(arr3).asJsonString().equals("[\"point(10 10)\"," + "\"point(20 20)\"]"));

    Map<String, String> map1 = new HashMap<>();
    map1.put("a", "1");
    map1.put("b", "1");
    assert (new Variant(map1).asJsonString().equals("{\"a\":\"1\"," + "\"b\":\"1\"}"));
    Map<String, Variant> map2 = new HashMap<>();
    map2.put("a", new Variant(1));
    map2.put("b", new Variant("a"));
    assert (new Variant(map2).asJsonString().equals("{\"a\":1,\"b\":\"a\"}"));
    Map<String, Geography> map3 = new HashMap<>();
    map3.put("a", Geography.fromGeoJSON("Point(10 10)"));
    map3.put("b", Geography.fromGeoJSON("Point(20 20)"));
    assert (new Variant(map3)
        .asJsonString()
        .equals("{\"a\":\"Point(10 10)\"," + "\"b\":\"Point(20 20)\"}"));
  }

  @Test(expected = UncheckedIOException.class)
  public void negativeTestForConversion() {
    new Variant(1).asBoolean();
  }

  @Test
  public void equalsAndToString() {
    Variant v1 = new Variant(123);
    Variant v2 = new Variant(123);
    Variant v3 = new Variant(223);

    assert v1.equals(v2);
    assert !v1.equals(v3);

    assert !v1.equals("aa");

    assert v1.hashCode() == v2.hashCode();
    assert v1.hashCode() != v3.hashCode();

    assert v1.toString().equals("123");
  }

  @Test
  public void javaJsonNodeVariantConverter() throws IllegalArgumentException {
    // Scala variant to Java Variant
    com.snowflake.snowpark.types.Variant sv =
        new com.snowflake.snowpark.types.Variant(
            "{\"name\": \"abc\",\"grade\": 2, \"Interest\": \"cricket\"}");
    Variant jResult = InternalUtils.createVariant(sv);
    JsonNode jsonNode = jResult.asJsonNode();

    assert jsonNode.get("name").asText().equals("abc");
    assert jsonNode.get("grade").asInt() == 2;
  }
}
