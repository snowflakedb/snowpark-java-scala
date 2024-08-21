package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.internal.ErrorMessage
import com.snowflake.snowpark.types._
import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.math.BigDecimal

private[snowpark] object Literal {
  // Snowflake max precision for decimal is 38
  private lazy val bigDecimalRoundContext = new java.math.MathContext(DecimalType.MAX_PRECISION)

  private def roundBigDecimal(decimal: BigDecimal): BigDecimal = {
    decimal.round(bigDecimalRoundContext)
  }

  def apply(v: Any): Literal = v match {
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
    case a: Array[Byte] => Literal(a, Option(BinaryType))
    case null => Literal(null, None)
    case v: Literal => v
    case _ =>
      throw ErrorMessage.PLAN_CANNOT_CREATE_LITERAL(v.getClass.getCanonicalName, s"$v")
  }

}

private[snowpark] case class Literal private (value: Any, dataTypeOption: Option[DataType])
    extends Expression {
  override def children: Seq[Expression] = Seq.empty

  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    this
}
