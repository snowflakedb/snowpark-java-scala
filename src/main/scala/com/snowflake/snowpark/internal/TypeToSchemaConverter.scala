package com.snowflake.snowpark.internal

import com.snowflake.snowpark.SnowparkClientException
import com.snowflake.snowpark.types._

import scala.reflect.runtime.universe.{MethodSymbol, Type, TypeTag, typeOf}
import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.lang.{
  Boolean => JavaBoolean,
  Byte => JavaByte,
  Short => JavaShort,
  Long => JavaLong,
  Float => JavaFloat,
  Double => JavaDouble,
  Integer => JavaInteger
}

object TypeToSchemaConverter {

  // construct StructType
  private[snowpark] def inferSchema[T: TypeTag](): StructType = {
    val tpe = typeOf[T]
    // CaseAccessor is the case class arguments.
    // for example, case class Test(a: Int, b: String), a and b are case accessors
    val caseAccessors = {
      tpe.members
        .collect {
          case m: MethodSymbol if m.isCaseAccessor => m
        }
        .toSeq
        // case arguments stored in the reverse order. for example
        // case class A(a: Int, b: Boolean, c: String)
        // case accessor is Array(c, b, a)
        // reverse it back to the normal order.
        .reverse
    }

    val fields: Seq[StructField] = tpe match {
      // case class
      // typeArgs are the general type arguments in class declaration,
      // for example, class Test[A, B], A and B are typeArgs.
      case t if t <:< typeOf[Product] && tpe.typeArgs.isEmpty && caseAccessors.nonEmpty =>
        caseAccessors.map { field =>
          val (dt, nullable) = analyzeType(field.info.resultType)
          StructField(field.name.toString, dt, nullable)
        }
      // tuple
      case t
          if t <:< typeOf[Product] && tpe.typeArgs.nonEmpty &&
            tpe.typeSymbol.name.toString.startsWith("Tuple") =>
        tpe.typeArgs.zipWithIndex.map { case (value, i) =>
          val (dt, nullable) = analyzeType(value)
          StructField(s"_${i + 1}", dt, nullable) // same name as tuple
        }
      // single value
      case _ =>
        val (dt, nullable) = analyzeType(tpe)
        Seq(StructField("VALUE", dt, nullable))
    }
    StructType(fields)
  }

  // analyze data type, returns data type and nullable
  private def analyzeType(tpe: Type): (DataType, Boolean) = {
    tpe match {
      case t if t <:< typeOf[Option[_]] =>
        if (t.typeArgs.head <:< typeOf[Option[_]]) {
          throw ErrorMessage.MISC_NESTED_OPTION_TYPE_IS_NOT_SUPPORTED()
        }
        (analyzeType(t.typeArgs.head)._1, true)
      // Array[Byte] is converted to BinaryType but not ArrayType
      case t if t =:= typeOf[Array[Byte]] => (BinaryType, true)
      case t if t <:< typeOf[Array[_]] =>
        (ArrayType(analyzeType(tpe.typeArgs.head)._1), true)
      case t if t <:< typeOf[Map[_, _]] =>
        (MapType(analyzeType(tpe.typeArgs.head)._1, analyzeType(tpe.typeArgs.last)._1), true)

      // default math context of BigDecimal is (34,6)
      // can't reflect precision and scale
      case t if t =:= typeOf[BigDecimal] => (DecimalType(34, 6), true)
      case t if t =:= typeOf[JavaBigDecimal] => (DecimalType(34, 6), true)

      case t if t =:= typeOf[Variant] => (VariantType, true)
      case t if t =:= typeOf[Geography] => (GeographyType, true)
      case t if t =:= typeOf[Geometry] => (GeometryType, true)
      case t if t =:= typeOf[Date] => (DateType, true)
      case t if t =:= typeOf[Timestamp] => (TimestampType, true)
      case t if t =:= typeOf[Time] => (TimeType, true)
      case t if t =:= typeOf[Boolean] => (BooleanType, false)
      case t if t =:= typeOf[JavaBoolean] => (BooleanType, true)
      case t if t =:= typeOf[Byte] => (ByteType, false)
      case t if t =:= typeOf[JavaByte] => (ByteType, true)
      case t if t =:= typeOf[Short] => (ShortType, false)
      case t if t =:= typeOf[JavaShort] => (ShortType, true)
      case t if t =:= typeOf[Int] => (IntegerType, false)
      case t if t =:= typeOf[JavaInteger] => (IntegerType, true)
      case t if t =:= typeOf[Long] => (LongType, false)
      case t if t =:= typeOf[JavaLong] => (LongType, true)
      case t if t =:= typeOf[String] => (StringType, true)
      case t if t =:= typeOf[Float] => (FloatType, false)
      case t if t =:= typeOf[JavaFloat] => (FloatType, true)
      case t if t =:= typeOf[Double] => (DoubleType, false)
      case t if t =:= typeOf[JavaDouble] => (DoubleType, true)
      case t if t =:= typeOf[Variant] => (VariantType, true)
      // content type of variant can't be reflected
      // add more data types
      case _ =>
        throw ErrorMessage.MISC_CANNOT_INFER_SCHEMA_FROM_TYPE(s"${tpe}")
    }
  }
}
