package com.snowflake.snowpark.internal

import com.google.protobuf.ByteString
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.proto.ast.Expr.Variant
import com.snowflake.snowpark.proto.ast._
import scalapb.GeneratedMessage

import java.math.{BigDecimal => JavaBigDecimal}

trait AstFunc {

  private[snowpark] val ast: GeneratedMessage

  private[snowpark] def createExpr(value: Any): Expr = value match {
    case null => createExpr(Variant.NullVal(NullVal()))
    case expr: Expr => expr
    case variant: Variant => Expr(variant)
    case column: Column => column.ast
    case literal if createExprFromLiteral.isDefinedAt(literal) => createExprFromLiteral(literal)
    case _ => throw new IllegalArgumentException(s"Unsupported value type: ${value.getClass}")
  }

  private val createExprFromLiteral: PartialFunction[Any, Expr] = {
    case str @ (_: String | _: Char) => createExpr(Variant.StringVal(StringVal(v = str.toString)))
    case i: Int => createExpr(i.toLong)
    case b: Byte => createExpr(b.toLong)
    case s: Short => createExpr(s.toLong)
    case l: Long => createExpr(Variant.Int64Val(Int64Val(v = l)))
    case f: Float => createExpr(f.toDouble)
    case d: Double => createExpr(Variant.Float64Val(Float64Val(v = d)))
    case bool: Boolean => createExpr(Variant.BoolVal(BoolVal(v = bool)))
    case decimal: BigDecimal => createExpr(decimal.bigDecimal)
    case decimal: JavaBigDecimal =>
      createExpr(
        Variant.BigDecimalVal(
          BigDecimalVal(
            scale = decimal.scale(),
            unscaledValue = ByteString.copyFrom(decimal.unscaledValue().toByteArray))))
    case bytes: Array[Byte] =>
      createExpr(Variant.BinaryVal(BinaryVal(v = ByteString.copyFrom(bytes))))
    // todo: SNOW-1961939 add support for timestamp and date literals
//    case i: Instant => null
//    case t: Timestamp => null
//    case d: Date => null
//    case ld: LocalDate => null
  }

}
