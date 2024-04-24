package com.snowflake.snowpark

import java.sql.{Date, Time, Timestamp}
import com.snowflake.snowpark.internal.ErrorMessage
import com.snowflake.snowpark.types.{Geography, Geometry, Variant}

import scala.util.hashing.MurmurHash3

/**
 * @since 0.1.0
 */
object Row {

  /**
   * Returns a [[Row]] based on the given values.
   * @since 0.1.0
   */
  def apply(values: Any*): Row = new Row(values.toArray)

  /**
   * Return a [[Row]] based on the values in the given Seq.
   * @since 0.1.0
   */
  def fromSeq(values: Seq[Any]): Row = new Row(values.toArray)

  /**
   * Return a [[Row]] based on the values in the given Array.
   * @since 0.2.0
   */
  def fromArray(values: Array[Any]): Row = new Row(values)

  private[snowpark] def fromMap(map: Map[String, Any]): Row =
    new SnowflakeObject(map)
}

private[snowpark] class SnowflakeObject private[snowpark]
  (private[snowpark] val map: Map[String, Any]) extends Row(map.values.toArray) {
  override def toString: String = convertValueToString(this)
}

/**
 * Represents a row returned by the evaluation of a [[DataFrame]].
 *
 * @groupname getter Getter Functions
 * @groupname utl Utility Functions
 * @since 0.1.0
 */
class Row protected (values: Array[Any]) extends Serializable {

  /**
   * Converts this [[Row]] to a Seq
   * @since 0.1.0
   * @group utl
   */
  def toSeq: Seq[Any] = values.toSeq

  /**
   * Total number of [[Column]] in this [[Row]]. Alias of [[length]]
   * @group utl
   * @since 0.1.0
   */
  def size: Int = length

  /**
   * Total number of [[Column]] in this [[Row]]
   * @since 0.1.0
   * @group utl
   */
  def length: Int = values.length

  /**
   * Returns the value of the column in the row at the given index. Alias of [[get]]
   * @since 0.1.0
   * @group getter
   */
  def apply(index: Int): Any = get(index)

  /**
   * Returns the value of the column in the row at the given index.
   * @since 0.1.0
   * @group getter
   */
  def get(index: Int): Any = values(index)

  /**
   * Returns a clone of this row.
   * @since 0.1.0
   * @group utl
   */
  def copy(): Row = new Row(values)

  /**
   * Returns a clone of this row object. Alias of [[copy]]
   * @since 0.1.0
   * @group utl
   */
  override def clone(): AnyRef = copy()

  /**
   * Returns true iff the given row equals this row.
   * @since 0.1.0
   * @group utl
   */
  override def equals(obj: Any): Boolean =
    if (!obj.isInstanceOf[Row]) {
      false
    } else {
      val other = obj.asInstanceOf[Row]
      if (length != other.length) {
        false
      } else {
        (0 until length).forall { index =>
          (this(index), other(index)) match {
            case (d1: Double, d2: Double) if d1.isNaN && d2.isNaN => true
            case (v1, v2) => v1 == v2
          }
        }
      }
    }

  /**
   * Calculates hashcode of this row.
   * @since 0.1.0
   * @group utl
   */
  override def hashCode(): Int = {
    var n = 0
    var h = MurmurHash3.seqSeed
    val len = length
    while (n < len) {
      h = MurmurHash3.mix(h, apply(n).##)
      n += 1
    }
    MurmurHash3.finalizeHash(h, n)
  }

  /**
   * Returns true if the value of the column at the given index is null, otherwise, returns false.
   * @since 0.1.0
   * @group utl
   */
  def isNullAt(index: Int): Boolean = get(index) == null

  /**
   * Returns the value of the column at the given index as a Boolean value
   * @since 0.1.0
   * @group getter
   */
  def getBoolean(index: Int): Boolean = getAnyValAs[Boolean](index)

  /**
   * Returns the value of the column at the given index as a Byte value.
   * Casts Short, Int, Long number to Byte if possible.
   * @since 0.1.0
   * @group getter
   */
  def getByte(index: Int): Byte = get(index) match {
    case byte: Byte => byte
    case short: Short if short <= Byte.MaxValue && short >= Byte.MinValue => short.toByte
    case int: Int if int <= Byte.MaxValue && int >= Byte.MinValue => int.toByte
    case long: Long if long <= Byte.MaxValue && long >= Byte.MinValue => long.toByte
    case other =>
      throw ErrorMessage.MISC_CANNOT_CAST_VALUE(other.getClass.getName, s"$other", "Byte")
  }

  /**
   * Returns the value of the column at the given index as a Short value.
   * Casts Byte, Int, Long number to Short if possible.
   * @since 0.1.0
   * @group getter
   */
  def getShort(index: Int): Short = get(index) match {
    case byte: Byte => byte.toShort
    case short: Short => short
    case int: Int if int <= Short.MaxValue && int >= Short.MinValue => int.toShort
    case long: Long if long <= Short.MaxValue && long >= Short.MinValue => long.toShort
    case other =>
      throw ErrorMessage.MISC_CANNOT_CAST_VALUE(other.getClass.getName, s"$other", "Short")
  }

  /**
   * Returns the value of the column at the given index as a Int value.
   * Casts Byte, Short, Long number to Int if possible.
   * @since 0.1.0
   * @group getter
   */
  def getInt(index: Int): Int = get(index) match {
    case byte: Byte => byte.toInt
    case short: Short => short.toInt
    case int: Int => int
    case long: Long if long <= Int.MaxValue && long >= Int.MinValue => long.toInt
    case other =>
      throw ErrorMessage.MISC_CANNOT_CAST_VALUE(other.getClass.getName, s"$other", "Int")
  }

  /**
   * Returns the value of the column at the given index as a Long value.
   * Casts Byte, Short, Int number to Long if possible.
   * @since 0.1.0
   * @group getter
   */
  def getLong(index: Int): Long = get(index) match {
    case byte: Byte => byte.toLong
    case short: Short => short.toLong
    case int: Int => int.toLong
    case long: Long => long
    case other =>
      throw ErrorMessage.MISC_CANNOT_CAST_VALUE(other.getClass.getName, s"$other", "Long")
  }

  /**
   * Returns the value of the column at the given index as a Float value.
   * Casts Byte, Short, Int, Long and Double number to Float if possible.
   * @since 0.1.0
   * @group getter
   */
  def getFloat(index: Int): Float = get(index) match {
    case float: Float => float
    case double: Double if double <= Float.MaxValue && double >= Float.MinValue => double.toFloat
    case byte: Byte => byte.toFloat
    case short: Short => short.toFloat
    case int: Int => int.toFloat
    case long: Long => long.toFloat
    case other =>
      throw ErrorMessage.MISC_CANNOT_CAST_VALUE(other.getClass.getName, s"$other", "Float")
  }

  /**
   * Returns the value of the column at the given index as a Double value.
   * Casts Byte, Short, Int, Long, Float number to Double.
   * @since 0.1.0
   * @group getter
   */
  def getDouble(index: Int): Double = get(index) match {
    case float: Float => float.toDouble
    case double: Double => double
    case byte: Byte => byte.toDouble
    case short: Short => short.toDouble
    case int: Int => int.toDouble
    case long: Long => long.toDouble
    case other =>
      throw ErrorMessage.MISC_CANNOT_CAST_VALUE(other.getClass.getName, s"$other", "Double")
  }

  /**
   * Returns the value of the column at the given index as a String value.
   * Returns geography data as string, if geography data of GeoJSON, WKT or EWKT is found.
   * @since 0.1.0
   * @group getter
   */
  def getString(index: Int): String = {
    get(index) match {
      case variant: Variant => variant.toString
      case geo: Geography => geo.toString
      case geo: Geometry => geo.toString
      case array: Array[_] => new Variant(array).toString
      case seq: Seq[_] => new Variant(seq).toString
      case map: Map[_, _] => new Variant(map).toString
      case _ => getAs[String](index)
    }
  }

  /**
   * Returns the value of the column at the given index as a Byte array value.
   * @since 0.2.0
   * @group getter
   */
  def getBinary(index: Int): Array[Byte] = getAs[Array[Byte]](index)

  /**
   * Returns the value of the column at the given index as a BigDecimal value
   * @since 0.1.0
   * @group getter
   */
  def getDecimal(index: Int): java.math.BigDecimal = getAs[java.math.BigDecimal](index)

  /**
   * Returns the value of the column at the given index as a Date value
   * @since 0.1.0
   * @group getter
   */
  def getDate(index: Int): Date = getAs[Date](index)

  /**
   * Returns the value of the column at the given index as a Time value
   * @since 0.2.0
   * @group getter
   */
  def getTime(index: Int): Time = getAs[Time](index)

  /**
   * Returns the value of the column at the given index as a Timestamp value
   * @since 0.2.0
   * @group getter
   */
  def getTimestamp(index: Int): Timestamp = getAs[Timestamp](index)

  /**
   * Returns the value of the column at the given index as Variant class
   * @since 0.2.0
   * @group getter
   */
  def getVariant(index: Int): Variant = new Variant(getString(index))

  /**
   * Returns the value of the column at the given index as Geography class
   * @since 0.2.0
   * @group getter
   */
  def getGeography(index: Int): Geography = getAs[Geography](index)

  /**
   * Returns the value of the column at the given index as Geometry class
   *
   * @since 1.12.0
   * @group getter
   */
  def getGeometry(index: Int): Geometry = getAs[Geometry](index)

  /**
   * Returns the value of the column at the given index as a Seq of Variant
   * @since 0.2.0
   * @group getter
   */
  def getSeqOfVariant(index: Int): Seq[Variant] =
    new Variant(getString(index)).asSeq()

  /**
   * Returns the value of the column at the given index as a java map of Variant
   * @since 0.2.0
   * @group getter
   */
  def getMapOfVariant(index: Int): Map[String, Variant] =
    new Variant(getString(index)).asMap()

  protected def convertValueToString(value: Any): String =
    value match {
      case null => "null"
      case map: Map[_, _] =>
        map
          .map {
            case (key, value) => s"${convertValueToString(key)}:${convertValueToString(value)}"
          }
          .mkString("Map(", ",", ")")
      case binary: Array[Byte] => s"Binary(${binary.mkString(",")})"
      case strValue: String => s""""$strValue""""
      case arr: Array[_] =>
        arr.map(convertValueToString).mkString("Array(", ",", ")")
      case obj: SnowflakeObject =>
        obj.map.map {
          case (key, value) =>
            s"$key:${convertValueToString(value)}"
        }.mkString("Object(", ",", ")")
      case other => other.toString
    }

  /**
   * Returns a string value to represent the content of this row
   * @since 0.1.0
   * @group utl
   */
  override def toString: String =
    values
      .map(convertValueToString)
      .mkString("Row[", ",", "]")

  private def getAs[T](index: Int): T = get(index).asInstanceOf[T]

  private def getAnyValAs[T <: AnyVal](index: Int): T =
    if (isNullAt(index)) throw new NullPointerException(s"Value at index $index is null")
    else getAs[T](index)

}
