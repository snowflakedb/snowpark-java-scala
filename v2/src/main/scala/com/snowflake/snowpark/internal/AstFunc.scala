package com.snowflake.snowpark.internal

import com.google.protobuf.ByteString
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.proto.ast.Expr.Variant
import com.snowflake.snowpark.proto.ast._
import scalapb.GeneratedMessage

import java.math.{BigDecimal => JavaBigDecimal}

trait AstFunc {

  private[snowpark] val ast: GeneratedMessage

  private[snowpark] def createExpr(value: Any): Expr = {
    case null => createExpr(Variant.NullVal(NullVal()))
    case expr => expr
    case variant: Variant => Expr(variant)
    case column: Column => column.ast
    case literal if createExprFromLiteral.isDefinedAt(literal) => createExprFromLiteral(literal)
    case _ => throw new IllegalArgumentException(s"Unsupported value type: ${value.getClass}")
  }

  private val createExprFromLiteral: PartialFunction[Any, Expr] = {
    case str @ (_: String | _: Char) => createExpr(Variant.StringVal(StringVal(v = str.toString)))
    case num @ (_: Int | _: Long | _: Byte | _: Short) =>
      createExpr(Variant.Int64Val(Int64Val(v = num.asInstanceOf[Long])))
    case float @ (_: Float | _: Double) =>
      createExpr(Variant.Float64Val(Float64Val(v = float.asInstanceOf[Double])))
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
//    case i: Instant => null
//    case t: Timestamp => null
//    case d: Date => null
//    case ld: LocalDate => null
  }

}
