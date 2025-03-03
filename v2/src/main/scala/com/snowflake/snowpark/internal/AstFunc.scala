package com.snowflake.snowpark.internal

import com.snowflake.snowpark.Column
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.proto.ast.Expr.Variant

trait AstFunc {
  private[snowpark] val ast: scalapb.GeneratedMessage

  protected def createExpr(value: Any): Expr = {
    case null => createExpr(Variant.NullVal(NullVal()))
    case variant: Variant => Expr(variant)
    case column: Column => column.ast
    case literal if createExprFromLiteral.isDefinedAt(literal) => createExprFromLiteral(literal)
    case _ => throw new IllegalArgumentException(s"Unsupported value type: ${value.getClass}")
  }

  private val createExprFromLiteral: PartialFunction[Any, Expr] = {
    case str @ ( _: String | _: Char) => createExpr(Variant.StringVal(StringVal(v = str.toString)))
    case num @ (_: Int | _: Long | _: Byte | _: Short) =>
      createExpr(Variant.Int64Val(Int64Val(v = num.asInstanceOf[Long])))
    case float @ (_: Float | _: Double) =>
      createExpr(Variant.Float64Val(Float64Val(v = float.asInstanceOf[Double])))
    case bool: Boolean => createExpr(Variant.BoolVal(BoolVal(v = bool)))
  }

  protected def createColumn(variant: Variant): Column = Column(createExpr(variant))

}
