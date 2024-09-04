package com.snowflake.snowpark.types

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}

import java.math.{BigDecimal => JavaBigDecimal, BigInteger => JavaBigInteger}
import java.sql.{Date, Time, Timestamp}
import java.util.{List => JavaList, Map => JavaMap}
import scala.collection.JavaConverters._
import Variant._
import org.apache.commons.codec.binary.{Base64, Hex}

import java.io.{IOException, UncheckedIOException}
import scala.util.hashing.MurmurHash3

private[snowpark] object Variant {

  private val MAPPER: ObjectMapper = new ObjectMapper()

  sealed trait VariantType {
    override def toString: String = {
      this.getClass.getName.split("\\$").last
    }
  }

  object VariantTypes {
    object RealNumber extends VariantType

    object FixedNumber extends VariantType

    object Boolean extends VariantType

    object String extends VariantType

    object Binary extends VariantType

    object Time extends VariantType

    object Date extends VariantType

    object Timestamp extends VariantType

    object Array extends VariantType

    object Object extends VariantType

    // internal used when converting from Java
    def getType(name: String): VariantType = name match {
      case "RealNumber" => RealNumber
      case "FixedNumber" => FixedNumber
      case "Boolean" => Boolean
      case "String" => String
      case "Binary" => Binary
      case "Time" => Time
      case "Date" => Date
      case "Timestamp" => Timestamp
      case "Array" => Array
      case "Object" => Object
      case _ => throw new IllegalArgumentException(s"Type: $name doesn't exist")
    }
  }

  private def objectToJsonNode(obj: Any): JsonNode = {
    obj match {
      case v: Variant => v.value
      case g: Geography => new Variant(g.asGeoJSON()).value
      case g: Geometry => new Variant(g.toString).value
      case _ => MAPPER.valueToTree(obj)
    }
  }
}

/** Representation of Snowflake Variant data
  *
  * @since 0.2.0
  */
class Variant private[snowpark] (
    private[snowpark] val value: JsonNode,
    private[snowpark] val dataType: VariantType) {

  /** Creates a Variant from double value
    *
    * @since 0.2.0
    */
  def this(num: Double) =
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.RealNumber)

  /** Creates a Variant from float value
    *
    * @since 0.2.0
    */
  def this(num: Float) =
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.RealNumber)

  /** Creates a Variant from long integer value
    *
    * @since 0.2.0
    */
  def this(num: Long) =
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber)

  /** Creates a Variant from integer value
    *
    * @since 0.2.0
    */
  def this(num: Int) = this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber)

  /** Creates a Variant from short integer value
    *
    * @since 0.2.0
    */
  def this(num: Short) =
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber)

  /** Creates a Variant from Java BigDecimal value
    *
    * @since 0.2.0
    */
  def this(num: JavaBigDecimal) =
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber)

  /** Creates a Variant from Scala BigDecimal value
    *
    * @since 0.6.0
    */
  def this(num: BigDecimal) = this(num.bigDecimal)

  /** Creates a Variant from Java BigInteger value
    *
    * @since 0.2.0
    */
  def this(num: JavaBigInteger) =
    this(JsonNodeFactory.instance.numberNode(num), VariantTypes.FixedNumber)

  /** Creates a Variant from Scala BigInt value
    *
    * @since 0.6.0
    */
  def this(num: BigInt) = this(num.bigInteger)

  /** Creates a Variant from Boolean value
    *
    * @since 0.2.0
    */
  def this(bool: Boolean) = this(JsonNodeFactory.instance.booleanNode(bool), VariantTypes.Boolean)

  /** Creates a Variant from String value. By default string is parsed as Json. If the parsing
    * failed, the string is stored as text.
    *
    * @since 0.2.0
    */
  def this(str: String) =
    this(
      {
        try {
          // `ObjectMapper` only reads the first token from
          // the input string but not the whole string.
          // For example, It can successfully
          // convert "null dummy" to `null` value without reporting error.
          if (str.toLowerCase().startsWith("null") && str != "null") {
            JsonNodeFactory.instance.textNode(str)
          } else {
            MAPPER.readTree(str)
          }
        } catch {
          case _: Exception => JsonNodeFactory.instance.textNode(str)
        }
      },
      VariantTypes.String)

  /** Creates a Variant from binary value
    *
    * @since 0.2.0
    */
  def this(bytes: Array[Byte]) =
    this(JsonNodeFactory.instance.binaryNode(bytes), VariantTypes.Binary)

  /** Creates a Variant from time value
    *
    * @since 0.2.0
    */
  def this(time: Time) = this(JsonNodeFactory.instance.textNode(time.toString), VariantTypes.Time)

  /** Creates a Variant from date value
    *
    * @since 0.2.0
    */
  def this(date: Date) = this(JsonNodeFactory.instance.textNode(date.toString), VariantTypes.Date)

  /** Creates a Variant from timestamp value
    *
    * @since 0.2.0
    */
  def this(timestamp: Timestamp) =
    this(JsonNodeFactory.instance.textNode(timestamp.toString), VariantTypes.Timestamp)

  /** Creates a Variant from Scala Seq
    *
    * @since 0.6.0
    */
  def this(seq: Seq[Any]) =
    this(
      {
        val arr = MAPPER.createArrayNode()
        seq.foreach(obj => arr.add(objectToJsonNode(obj)))
        arr
      },
      VariantTypes.String)

  /** Creates a Variant from Java List
    *
    * @since 0.2.0
    */
  def this(list: JavaList[Object]) = this(list.asScala)

  /** Creates a Variant from array
    *
    * @since 0.2.0
    */
  def this(objects: Array[Any]) = this(objects.toSeq)

  /** Creates a Variant from Object
    *
    * @since 0.2.0
    */
  def this(obj: Any) =
    this(
      {
        def mapToNode(map: JavaMap[Object, Object]): ObjectNode = {
          val result = MAPPER.createObjectNode()
          map.keySet().forEach(key => result.set(key.toString, objectToJsonNode(map.get(key))))
          result
        }
        obj match {
          // this(objects: Array[Any]) doesn't work, arrays always go to this constructor
          case array: Array[_] =>
            val arr = MAPPER.createArrayNode()
            array.foreach(obj => arr.add(objectToJsonNode(obj)))
            arr
          case map: JavaMap[Object, Object] => mapToNode(map)
          case map: Map[_, _] =>
            mapToNode(map.map { case (key, value) =>
              key.asInstanceOf[Object] -> value.asInstanceOf[Object]
            }.asJava)
          case _ => MAPPER.valueToTree(obj.asInstanceOf[Object])
        }
      },
      VariantTypes.String)

  /** Converts the variant as double value
    *
    * @since 0.2.0
    */
  def asDouble(): Double = convert(VariantTypes.RealNumber) {
    value.asDouble()
  }

  /** Converts the variant as float value
    *
    * @since 0.2.0
    */
  def asFloat(): Float = convert(VariantTypes.RealNumber) {
    value.asDouble().toFloat
  }

  /** Converts the variant as long value
    *
    * @since 0.2.0
    */
  def asLong(): Long = convert(VariantTypes.FixedNumber) {
    value.asLong()
  }

  /** Converts the variant as integer value
    *
    * @since 0.2.0
    */
  def asInt(): Int = convert(VariantTypes.FixedNumber) {
    value.asInt()
  }

  /** Converts the variant as short value
    *
    * @since 0.2.0
    */
  def asShort(): Short = convert(VariantTypes.FixedNumber) {
    value.asInt().toShort
  }

  /** Converts the variant as BigDecimal value
    *
    * @since 0.6.0
    */
  def asBigDecimal(): BigDecimal = convert(VariantTypes.RealNumber) {
    if (value.isBoolean) {
      BigDecimal(value.asInt())
    } else {
      BigDecimal.javaBigDecimal2bigDecimal(value.decimalValue())
    }
  }

  /** Converts the variant as Scala BigInt value
    *
    * @since 0.6.0
    */
  def asBigInt(): BigInt = convert(VariantTypes.FixedNumber) {
    if (value.isBoolean) {
      BigInt(value.asInt())
    } else {
      BigInt.javaBigInteger2bigInt(value.bigIntegerValue())
    }
  }

  /** Converts the variant as boolean value
    *
    * @since 0.2.0
    */
  def asBoolean(): Boolean = convert(VariantTypes.Boolean) {
    value.asBoolean()
  }

  /** Converts the variant as string value
    *
    * @since 0.2.0
    */
  def asString(): String = convert(VariantTypes.String) {
    if (value.isBinary) {
      val decoded = Base64.decodeBase64(value.asText())
      Hex.encodeHexString(decoded)
    } else if (value.isValueNode) {
      value.asText()
    } else {
      value.toString
    }
  }

  /** An alias of [[asString]]
    *
    * @since 0.2.0
    */
  override def toString: String = asString()

  /** Converts the variant as valid Json String
    * @since 0.2.0
    */
  def asJsonString(): String = convert(VariantTypes.String) {
    if (value.isBinary) {
      val decoded = Base64.decodeBase64(value.asText())
      s""""${Hex.encodeHexString(decoded)}""""
    } else {
      value.toString
    }
  }

  /** Return the variant value as a JsonNode. This function allows to read the JSON object directly
    * as JsonNode from variant column rather parsing it as String Example - to get the first value
    * from array for key "a"
    * {{{
    *   val sv = new Variant("{\"a\": [1, 2], \"b\": 3, \"c\": \"xyz\"}")
    *   println(sv.asJsonNode().get("a").get(0))
    * output
    * 1
    * }}}
    *
    * @since 1.14.0
    */
  def asJsonNode(): JsonNode = {
    value
  }

  /** Converts the variant as binary value
    * @since 0.2.0
    */
  def asBinary(): Array[Byte] = convert(VariantTypes.Binary) {
    try {
      value.binaryValue()
    } catch {
      case _: IOException =>
        try {
          Hex.decodeHex(value.asText.toCharArray)
        } catch {
          case _: Exception =>
            throw new UncheckedIOException(
              new IOException(
                s"Failed to convert ${value.asText()} to Binary. " +
                  "Only Hex string is supported."))
        }
    }
  }

  /** Converts the variant as time value
    * @since 0.2.0
    */
  def asTime(): Time = convert(VariantTypes.Time) {
    Time.valueOf(value.asText())
  }

  /** Converts the variant as date value
    * @since 0.2.0
    */
  def asDate(): Date = convert(VariantTypes.Date) {
    Date.valueOf(value.asText())
  }

  /** Converts the variant as timestamp value
    * @since 0.2.0
    */
  def asTimestamp(): Timestamp = convert(VariantTypes.Timestamp) {
    if (value.isNumber) {
      new Timestamp(value.asLong())
    } else {
      Timestamp.valueOf(value.asText())
    }
  }

  /** Converts the variant as Scala Seq of Variant
    * @since 0.6.0
    */
  def asSeq(): Seq[Variant] = asArray()

  /** Converts the variant as Array of Variant
    * @since 0.2.0
    */
  def asArray(): Array[Variant] = value match {
    case null => null;
    case arr: ArrayNode =>
      val size = arr.size()
      val result = new Array[Variant](size)
      (0 until size).foreach(index => {
        result(index) = new Variant(arr.get(index).toString)
      })
      result
  }

  /** Converts the variant as Scala Map of String to Variant
    * @since 0.6.0
    */
  def asMap(): Map[String, Variant] = value match {
    case null => null
    case obj: ObjectNode =>
      var map = Map.empty[String, Variant]
      val fields = obj.fields()
      while (fields.hasNext) {
        val field = fields.next()
        map = map + (field.getKey -> new Variant(field.getValue.toString))
      }
      map
  }

  /** Checks whether two Variants are equal
    * @since 0.2.0
    */
  override def equals(obj: Any): Boolean = obj match {
    case v: Variant => value.equals(v.value)
    case _ => false
  }

  /** Calculates hashcode of this Variant
    * @since 0.6.0
    */
  override def hashCode(): Int = {
    var h = MurmurHash3.seqSeed
    h = MurmurHash3.mix(h, dataType.##)
    h = MurmurHash3.mix(h, value.toPrettyString.##)
    MurmurHash3.finalizeHash(h, 2)
  }

  private def convert[T](target: VariantType)(thunk: => T): T =
    (dataType, target) match {
      case (from, to) if from == to => thunk
      case (VariantTypes.String, _) => thunk
      case (_, VariantTypes.String) => thunk
      case (VariantTypes.RealNumber, VariantTypes.Timestamp) => thunk
      case (VariantTypes.FixedNumber, VariantTypes.Timestamp) => thunk
      case (VariantTypes.Boolean, VariantTypes.RealNumber) => thunk
      case (VariantTypes.Boolean, VariantTypes.FixedNumber) => thunk
      case (VariantTypes.FixedNumber, VariantTypes.RealNumber) => thunk
      case (VariantTypes.RealNumber, VariantTypes.FixedNumber) => thunk
      case (_, _) =>
        throw new UncheckedIOException(
          new IOException(s"Conversion from Variant of $dataType to $target is not supported"))
    }
}
