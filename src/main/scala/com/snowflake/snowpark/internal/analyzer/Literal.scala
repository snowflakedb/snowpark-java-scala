package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.internal.ErrorMessage
import com.snowflake.snowpark.types._

import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

private[snowpark] object Literal {
  // Snowflake max precision for decimal is 38
  private lazy val bigDecimalRoundContext = new java.math.MathContext(DecimalType.MAX_PRECISION)

  private def roundBigDecimal(decimal: BigDecimal): BigDecimal = {
    decimal.round(bigDecimalRoundContext)
  }

  def apply(v: Any): TLiteral = v match {
    case i: Int => Literal(i, Option(IntegerType))
    case l: Long => Literal(l, Option(LongType))
    case d: Double => Literal(d, Option(DoubleType))
    case f: Float => Literal(f, Option(FloatType))
    case b: Byte => Literal(b, Option(ByteType))
    case s: Short => Literal(s, Option(ShortType))
    case s: String => Literal(s, Option(StringType))
    case c: Char => Literal(c.toString, Option(StringType))
    case b: Boolean => Literal(b, Option(BooleanType))
    case d: scala.math.BigDecimal =>
      val scalaDecimal = roundBigDecimal(d)
      Literal(scalaDecimal, Option(DecimalType(scalaDecimal)))
    case d: JavaBigDecimal =>
      val scalaDecimal = scala.math.BigDecimal.decimal(d, bigDecimalRoundContext)
      Literal(scalaDecimal, Option(DecimalType(scalaDecimal)))
    case i: Instant => Literal(DateTimeUtils.instantToMicros(i), Option(TimestampType))
    case t: Timestamp => Literal(DateTimeUtils.javaTimestampToMicros(t), Option(TimestampType))
    case ld: LocalDate => Literal(DateTimeUtils.localDateToDays(ld), Option(DateType))
    case d: Date => Literal(DateTimeUtils.javaDateToDays(d), Option(DateType))
    case s: Seq[Any] => ArrayLiteral(s)
    case m: Map[Any, Any] => MapLiteral(m)
    case null => Literal(null, None)
    case v: Literal => v
    case _ =>
      throw ErrorMessage.PLAN_CANNOT_CREATE_LITERAL(v.getClass.getCanonicalName, s"$v")
  }

}

private[snowpark] trait TLiteral extends Expression {
  def value: Any
  def dataTypeOption: Option[DataType]

  override def children: Seq[Expression] = Seq.empty

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    this
}

private[snowpark] case class Literal (value: Any, dataTypeOption: Option[DataType]) extends TLiteral

private[snowpark] case class ArrayLiteral(value: Seq[Any]) extends TLiteral {
  val elementsLiterals: Seq[TLiteral] = value.map(Literal(_))
  val dataTypeOption = inferArrayType
  
  private[analyzer] def inferArrayType(): Option[DataType] = {
    elementsLiterals.flatMap(_.dataTypeOption).distinct match {
      case Seq() => None
      case Seq(ByteType) => Some(BinaryType)
      case Seq(dt) => Some(ArrayType(dt))
      case Seq(_, _*) => Some(ArrayType(VariantType))
    }
  }
}

private[snowpark] case class MapLiteral(value: Map[Any, Any]) extends TLiteral {
  val entriesLiterals = value.map { case (k, v) => Literal(k) -> Literal(v) }
  val dataTypeOption = inferMapType
  
  private[analyzer] def inferMapType(): Option[MapType] = {
    entriesLiterals.keys.flatMap(_.dataTypeOption).toSeq.distinct match {
      case Seq() => None
      case Seq(StringType) =>
        val valuesTypes = entriesLiterals.values.flatMap(_.dataTypeOption).toSeq.distinct
        valuesTypes match {
          case Seq() => None
          case Seq(dt) => Some(MapType(StringType, dt))
          case Seq(_, _*) => Some(MapType(StringType, VariantType))
        }
      case _ =>
        throw ErrorMessage.PLAN_CANNOT_CREATE_LITERAL(value.getClass.getCanonicalName, s"$value")
    }
  }
}
