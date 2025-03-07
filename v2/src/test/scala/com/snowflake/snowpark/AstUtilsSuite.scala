package com.snowflake.snowpark

import com.google.protobuf.ByteString
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.proto.ast.Expr.Variant
import com.snowflake.snowpark.internal.AstUtils._

class AstUtilsSuite extends UnitTestBase {

  test("createExpr with null value") {
    checkAst(Expr(Variant.NullVal(NullVal())), createExpr(null, null))
  }

  test("createExpr with Expr value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    checkAst(expr, createExpr(expr, null))
  }

  test("createExpr with Variant value") {
    val variant = Variant.StringVal(StringVal(v = "test"))
    checkAst(Expr(variant), createExpr(variant, null))
  }

  test("createExpr with Column value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    val column = Column(expr)
    checkAst(expr, createExpr(column, null))
  }

  test("createExpr with String value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    checkAst(expr, createExpr("test", null))
  }

  test("createExpr with Char value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "t")))
    checkAst(expr, createExpr('t', null))
  }

  test("createExpr with Int value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1, null))
  }

  test("createExpr with Byte value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1.toByte, null))
  }

  test("createExpr with Short value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1.toShort, null))
  }

  test("createExpr with Long value") {
    val expr = Expr(Variant.Int64Val(Int64Val(v = 1.toLong)))
    checkAst(expr, createExpr(1.toLong, null))
  }

  test("createExpr with Float value") {
    val expr = Expr(Variant.Float64Val(Float64Val(v = 1.0)))
    checkAst(expr, createExpr(1.0f, null))
  }

  test("createExpr with Double value") {
    val expr = Expr(Variant.Float64Val(Float64Val(v = 1.0)))
    checkAst(expr, createExpr(1.0, null))
  }

  test("createExpr with Boolean value") {
    val expr = Expr(Variant.BoolVal(BoolVal(v = true)))
    checkAst(expr, createExpr(true, null))
  }

  test("createExpr with BigDecimal value") {
    val expr = Expr(
      Variant.BigDecimalVal(
        BigDecimalVal(
          scale = 2,
          unscaledValue =
            ByteString.copyFrom(java.math.BigDecimal.valueOf(12.34).unscaledValue().toByteArray))))
    checkAst(expr, createExpr(BigDecimal(12.34), null))
  }

  test("createExpr with JavaBigDecimal value") {
    val expr = Expr(
      Variant.BigDecimalVal(
        BigDecimalVal(
          scale = 2,
          unscaledValue =
            ByteString.copyFrom(java.math.BigDecimal.valueOf(12.34).unscaledValue().toByteArray))))
    checkAst(expr, createExpr(java.math.BigDecimal.valueOf(12.34), null))
  }

  test("createExpr with Array[Byte] value") {
    val expr = Expr(Variant.BinaryVal(BinaryVal(v = ByteString.copyFrom(Array[Byte](1, 2, 3)))))
    checkAst(expr, createExpr(Array[Byte](1, 2, 3), null))
  }

  test("createExpr with unsupported value type") {
    checkException[IllegalArgumentException]("Unsupported value type:") {
      createExpr(this, null)
    }
  }
}
