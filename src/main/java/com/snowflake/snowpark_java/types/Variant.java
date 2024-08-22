package com.snowflake.snowpark_java.types;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

/**
 * Representation of Snowflake Variant data
 *
 * @since 0.8.0
 */
public class Variant implements Serializable {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final JsonNode value;
  private final VariantTypes type;

  private Variant(JsonNode value, VariantTypes type) {
    this.value = value;
    this.type = type;
  }

  // package private internal used when converting from Scala
  Variant(JsonNode value, String typeStr) {
    this.value = value;
    VariantTypes dt;
    switch (typeStr) {
      case "RealNumber":
        dt = VariantTypes.RealNumber;
        break;
      case "FixedNumber":
        dt = VariantTypes.FixedNumber;
        break;
      case "Boolean":
        dt = VariantTypes.Boolean;
        break;
      case "String":
        dt = VariantTypes.String;
        break;
      case "Binary":
        dt = VariantTypes.Binary;
        break;
      case "Time":
        dt = VariantTypes.Time;
        break;
      case "Date":
        dt = VariantTypes.Date;
        break;
      case "Timestamp":
        dt = VariantTypes.Timestamp;
        break;
      default:
        throw new IllegalArgumentException("Type: " + typeStr + " doesn't exist");
    }
    this.type = dt;
  }

  /**
   * Creates a Variant from float value.
   *
   * @param num A float number
   * @since 0.8.0
   */
  public Variant(float num) {
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.RealNumber);
  }

  /**
   * Creates a Variant from double value.
   *
   * @param num A double number
   * @since 0.8.0
   */
  public Variant(double num) {
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.RealNumber);
  }

  /**
   * Creates a Variant from long value.
   *
   * @param num A long number
   * @since 0.8.0
   */
  public Variant(long num) {
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber);
  }

  /**
   * Creates a Variant from int value.
   *
   * @param num An int number
   * @since 0.8.0
   */
  public Variant(int num) {
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber);
  }

  /**
   * Creates a Variant from short value.
   *
   * @param num A short number
   * @since 0.8.0
   */
  public Variant(short num) {
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber);
  }

  /**
   * Creates a Variant from BigDecimal value.
   *
   * @param num A BigDecimal number
   * @since 0.8.0
   */
  public Variant(BigDecimal num) {
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber);
  }

  /**
   * Creates a Variant from BigInteger value.
   *
   * @param num A BigInteger number
   * @since 0.8.0
   */
  public Variant(BigInteger num) {
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber);
  }

  /**
   * Creates a Variant from boolean value.
   *
   * @param value A boolean value
   * @since 0.8.0
   */
  public Variant(boolean value) {
    this(JsonNodeFactory.instance.booleanNode(value), VariantTypes.Boolean);
  }

  /**
   * Creates a Variant from String value.
   *
   * @since 0.8.0
   * @param str A string value
   */
  public Variant(String str) {
    JsonNode node;
    try {
      node = MAPPER.readTree(str);
    } catch (Exception e) {
      node = JsonNodeFactory.instance.textNode(str);
    }
    this.value = node;
    this.type = VariantTypes.String;
  }

  /**
   * Creates a Variant from binary value.
   *
   * @param bytes An array of byte representing binary data
   * @since 0.8.0
   */
  public Variant(byte[] bytes) {
    this(JsonNodeFactory.instance.binaryNode(bytes), VariantTypes.Binary);
  }

  /**
   * Creates a Variant from Time value.
   *
   * @param time A Time object
   * @since 0.8.0
   */
  public Variant(Time time) {
    this(JsonNodeFactory.instance.textNode(time.toString()), VariantTypes.Time);
  }

  /**
   * Creates a Variant from Date value.
   *
   * @param date A Date object
   * @since 0.8.0
   */
  public Variant(Date date) {
    this(JsonNodeFactory.instance.textNode(date.toString()), VariantTypes.Date);
  }

  /**
   * Creates a Variant from Timestamp value.
   *
   * @param timestamp A Timestamp object
   * @since 0.8.0
   */
  public Variant(Timestamp timestamp) {
    this(JsonNodeFactory.instance.textNode(timestamp.toString()), VariantTypes.Timestamp);
  }

  /**
   * Creates a Variant from a list.
   *
   * @param list A list of data
   * @since 0.8.0
   */
  public Variant(List<Object> list) {
    ArrayNode arr = MAPPER.createArrayNode();
    list.forEach(obj -> arr.add(objectToJsonNode(obj)));
    this.value = arr;
    this.type = VariantTypes.Array;
  }

  /**
   * Creates a Variant from an array.
   *
   * @param arr An array of data
   * @since 0.8.0
   */
  public Variant(Object[] arr) {
    this(Arrays.asList(arr));
  }

  /**
   * Creates a Variant from Object value.
   *
   * @param obj Any Java object
   * @since 0.8.0
   */
  public Variant(Object obj) {
    JsonNode node;
    if (obj instanceof Map<?, ?>) {
      ObjectNode result = MAPPER.createObjectNode();
      Map<?, ?> map = (Map<?, ?>) obj;
      map.keySet().forEach(key -> result.set(key.toString(), objectToJsonNode(map.get(key))));
      node = result;
      this.type = VariantTypes.Object;
    } else if (obj instanceof Object[]) {
      ArrayNode result = MAPPER.createArrayNode();
      for (Object o : (Object[]) obj) {
        result.add(objectToJsonNode(o));
      }
      node = result;
      this.type = VariantTypes.Array;
    } else {
      node = MAPPER.valueToTree(obj);
      this.type = VariantTypes.String;
    }
    this.value = node;
  }

  /**
   * Converts the variant as float value.
   *
   * @return A float number
   * @since 0.8.0
   */
  public float asFloat() {
    verify(VariantTypes.RealNumber);
    return (float) asDouble();
  }

  /**
   * Converts the variant as double value.
   *
   * @return A double number
   * @since 0.8.0
   */
  public double asDouble() {
    verify(VariantTypes.RealNumber);
    return value.asDouble();
  }

  /**
   * Converts the variant as short value.
   *
   * @return A short number
   * @since 0.8.0
   */
  public short asShort() {
    verify(VariantTypes.FixedNumber);
    return (short) asInt();
  }

  /**
   * Converts the variant as int value.
   *
   * @return An int number
   * @since 0.8.0
   */
  public int asInt() {
    verify(VariantTypes.FixedNumber);
    return value.asInt();
  }

  /**
   * Converts the variant as long value.
   *
   * @return A long number
   * @since 0.8.0
   */
  public long asLong() {
    verify(VariantTypes.FixedNumber);
    return value.asLong();
  }

  /**
   * Converts the variant as BigDecimal value.
   *
   * @return A BigDecimal number
   * @since 0.8.0
   */
  public BigDecimal asBigDecimal() {
    verify(VariantTypes.RealNumber);
    if (value.isBoolean()) {
      return new BigDecimal(asInt());
    } else {
      return value.decimalValue();
    }
  }

  /**
   * Converts the variant as BigInteger value.
   *
   * @return A BigInteger number
   * @since 0.8.0
   */
  public BigInteger asBigInteger() {
    verify(VariantTypes.FixedNumber);
    if (value.isBoolean()) {
      return BigInteger.valueOf(asInt());
    } else {
      return value.bigIntegerValue();
    }
  }

  /**
   * Converts the variant as String value.
   *
   * @return A String number
   * @since 0.8.0
   */
  public String asString() {
    verify(VariantTypes.String);
    if (value.isBinary()) {
      byte[] decoded = Base64.decodeBase64(value.asText());
      return Hex.encodeHexString(decoded);
    } else if (value.isValueNode()) {
      return value.asText();
    } else {
      return value.toString();
    }
  }

  /**
   * Converts the variant as valid Json String.
   *
   * @return A valid json string
   * @since 0.8.0
   */
  public String asJsonString() {
    verify(VariantTypes.String);
    if (value.isBinary()) {
      byte[] decoded = Base64.decodeBase64(value.asText());
      return "\"" + Hex.encodeHexString(decoded) + "\"";
    } else {
      return value.toString();
    }
  }

  /**
   * Converts the variant as valid JsonNode.
   *
   * @return A valid json Node
   * @since 1.14.0
   */
  public JsonNode asJsonNode() {
    verify(VariantTypes.Object);
    return objectToJsonNode(value);
  }

  /**
   * Converts the variant as binary value.
   *
   * @return An array of byte representing binary data
   * @since 0.8.0
   */
  public byte[] asBinary() {
    verify(VariantTypes.Binary);
    try {
      return value.binaryValue();
    } catch (Exception e) {
      try {
        return Hex.decodeHex(value.asText().toCharArray());
      } catch (Exception e1) {
        throw new UncheckedIOException(
            new IOException(
                "Failed to convert "
                    + value.asText()
                    + " to Binary. "
                    + "Only Hex string is supported."));
      }
    }
  }

  /**
   * Converts the variant as Time value.
   *
   * @return A Time data
   * @since 0.8.0
   */
  public Time asTime() {
    verify(VariantTypes.Time);
    return Time.valueOf(value.asText());
  }

  /**
   * Converts the variant as Date value.
   *
   * @return A Date data
   * @since 0.8.0
   */
  public Date asDate() {
    verify(VariantTypes.Date);
    return Date.valueOf(value.asText());
  }

  /**
   * Converts the variant as Timestamp value.
   *
   * @return A Timestamp data
   * @since 0.8.0
   */
  public Timestamp asTimestamp() {
    verify(VariantTypes.Timestamp);
    if (value.isNumber()) {
      return new Timestamp(value.asLong());
    } else {
      return Timestamp.valueOf(value.asText());
    }
  }

  /**
   * Converts the variant as boolean value.
   *
   * @return a boolean value
   * @since 0.8.0
   */
  public boolean asBoolean() {
    verify(VariantTypes.Boolean);
    return value.asBoolean();
  }

  /**
   * Converts the variant as array of Variant.
   *
   * @return An array of Variant
   * @since 0.8.0
   */
  public Variant[] asArray() {
    if (value == null) return null;
    if (value instanceof ArrayNode) {
      ArrayNode arr = (ArrayNode) value;
      int size = arr.size();
      Variant[] result = new Variant[size];
      for (int i = 0; i < size; i++) {
        result[i] = new Variant(arr.get(i).toString());
      }
      return result;
    }
    throw new UncheckedIOException(
        new IOException("Failed to convert " + value.asText() + " to Array"));
  }

  /**
   * Converts the variant as list of Variant.
   *
   * @return A list of Variant
   * @since 0.8.0
   */
  public List<Variant> asList() {
    return Arrays.asList(asArray());
  }

  /**
   * Converts the variant as map of Variant.
   *
   * @return A map from String to Variant
   * @since 0.8.0
   */
  public Map<String, Variant> asMap() {
    if (value == null) return null;
    if (value instanceof ObjectNode) {
      ObjectNode map = (ObjectNode) value;
      Map<String, Variant> result = new HashMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = map.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        result.put(field.getKey(), new Variant(field.getValue().toString()));
      }
      return result;
    }
    throw new UncheckedIOException(
        new IOException("Failed to convert " + value.asText() + " to Map"));
  }

  /**
   * Checks whether two Variants are equal.
   *
   * @return true if they are equal.
   * @since 0.8.0
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof Variant) {
      return value.equals(((Variant) other).value);
    }
    return false;
  }

  /**
   * Calculates the hash code of this Variant Object.
   *
   * @return An int number representing the hash code value
   * @since 0.9.0
   */
  @Override
  public int hashCode() {
    int hash = 13;
    hash = 23 * hash + (value == null ? 0 : value.hashCode());
    hash = 23 * hash + (type == null ? 0 : type.hashCode());
    return hash;
  }

  /**
   * An alias of {@code asString()}
   *
   * @return A string data
   * @since 0.8.0
   */
  @Override
  public String toString() {
    return asString();
  }

  JsonNode getValue() {
    return value;
  }

  VariantTypes getType() {
    return type;
  }

  private static JsonNode objectToJsonNode(Object obj) {
    if (obj instanceof Variant) return ((Variant) obj).value;
    if (obj instanceof Geography) return new Variant(((Geography) obj).asGeoJSON()).value;
    return MAPPER.valueToTree(obj);
  }

  private void verify(VariantTypes target) {
    if (type == target) return;
    if (type == VariantTypes.String) return;
    if (target == VariantTypes.String) return;
    if (type == VariantTypes.RealNumber && target == VariantTypes.Timestamp) return;
    if (type == VariantTypes.FixedNumber && target == VariantTypes.Timestamp) return;
    if (type == VariantTypes.Boolean && target == VariantTypes.RealNumber) return;
    if (type == VariantTypes.Boolean && target == VariantTypes.FixedNumber) return;
    if (type == VariantTypes.FixedNumber && target == VariantTypes.RealNumber) return;
    if (type == VariantTypes.RealNumber && target == VariantTypes.FixedNumber) return;

    throw new UncheckedIOException(
        new IOException(
            "Conversion from Variant of " + type + " to " + target + " is not supported"));
  }
}

enum VariantTypes {
  RealNumber,
  FixedNumber,
  Boolean,
  String,
  Binary,
  Time,
  Date,
  Timestamp,
  Object,
  Array
}
