package com.snowflake.snowpark

import com.google.protobuf.ByteString
import com.snowflake.snowpark.internal.AstFunc
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.proto.ast.Expr.Variant
import scalapb.GeneratedMessage

class AstFunSuite extends UnitTestBase {

  object TestAstFunc extends AstFunc {
    override private[snowpark] val ast: GeneratedMessage = null
  }

  private val createExpr: Any => Expr = TestAstFunc.createExpr

  test("createExpr with null value") {
    checkAst(Expr(Variant.NullVal(NullVal())), createExpr(null))
  }

  test("createExpr with Expr value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    checkAst(expr, createExpr(expr))
  }

  test("createExpr with Variant value") {
    val variant = Variant.StringVal(StringVal(v = "test"))
    checkAst(Expr(variant), createExpr(variant))
  }

  test("createExpr with Column value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    val column = Column(expr)
    checkAst(expr, createExpr(column))
  }

  test("createExpr with String value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    checkAst(expr, createExpr("test"))
  }

  test("createExpr with Char value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "t")))
    checkAst(expr, createExpr('t'))
  }

  test("createExpr with Int value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1))
  }

  test("createExpr with Byte value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1.toByte))
  }

  test("createExpr with Short value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1.toShort))
  }

  test("createExpr with Long value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1.toLong))
  }

  test("createExpr with Float value") {
    val expr = Expr(Variant.Float64Val(Float64Val(v = 1.0)))
    checkAst(expr, createExpr(1.0f))
  }

  test("createExpr with Double value") {
    val expr = Expr(Variant.Float64Val(Float64Val(v = 1.0)))
    checkAst(expr, createExpr(1.0))
  }

  test("createExpr with Boolean value") {
    val expr = Expr(Variant.BoolVal(BoolVal(v = true)))
    checkAst(expr, createExpr(true))
  }

  test("createExpr with BigDecimal value") {
    val expr = Expr(
      Variant.BigDecimalVal(
        BigDecimalVal(
          scale = 2,
          unscaledValue =
            ByteString.copyFrom(java.math.BigDecimal.valueOf(12.34).unscaledValue().toByteArray))))
    checkAst(expr, createExpr(BigDecimal(12.34)))
  }

  test("createExpr with JavaBigDecimal value") {
    val expr = Expr(
      Variant.BigDecimalVal(
        BigDecimalVal(
          scale = 2,
          unscaledValue =
            ByteString.copyFrom(java.math.BigDecimal.valueOf(12.34).unscaledValue().toByteArray))))
    checkAst(expr, createExpr(java.math.BigDecimal.valueOf(12.34)))
  }

  test("createExpr with Array[Byte] value") {
    val expr = Expr(Variant.BinaryVal(BinaryVal(v = ByteString.copyFrom(Array[Byte](1, 2, 3)))))
    checkAst(expr, createExpr(Array[Byte](1, 2, 3)))
  }

  test("createExpr with unsupported value type") {
    checkException[IllegalArgumentException]("Unsupported value type:") {
      createExpr(this)
    }
  }

}
