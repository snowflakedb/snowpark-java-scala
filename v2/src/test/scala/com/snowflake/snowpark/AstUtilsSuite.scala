package com.snowflake.snowpark

import com.google.protobuf.ByteString
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.proto.ast.Expr.Variant
import com.snowflake.snowpark.internal.AstUtils._
import com.snowflake.snowpark.internal.SrcPositionInfo

class AstUtilsSuite extends UnitTestBase {

  implicit val dummySrcPositionInfo: SrcPositionInfo = SrcPositionInfo("test", 1, 12)

  def checkAstWithSrcPosition(
      expected: Option[SrcPosition] => Any,
      actual: SrcPositionInfo => Expr)(implicit srcPositionInfo: SrcPositionInfo): Unit = {
    val srcPosition = createSroPosition(srcPositionInfo)
    val expectedExprWithSrc: Expr = expected(Some(srcPosition)) match {
      case expr: Expr => expr
      case variant: Variant => Expr(variant)
    }
    val expectedExprWithoutSrc: Expr = expected(None) match {
      case expr: Expr => expr
      case variant: Variant => Expr(variant)
    }
    // with src position
    checkAst(expectedExprWithSrc, actual(srcPositionInfo))
    // without src position
    checkAst(expectedExprWithoutSrc, actual(null))
  }

  test("createExpr with null value") {
    checkAstWithSrcPosition(src => Variant.NullVal(NullVal(src = src)), createExpr(null, _))
  }

  test("createExpr with Expr value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    checkAstWithSrcPosition(_ => expr, createExpr(expr, _))
  }

  test("createExpr with Variant value") {
    val variant = Variant.StringVal(StringVal(v = "test"))
    checkAstWithSrcPosition(_ => variant, createExpr(variant, _))
  }

  test("createExpr with Column value") {
    val expr = Expr(Variant.StringVal(StringVal(v = "test")))
    val column = Column(expr)
    checkAstWithSrcPosition(_ => expr, createExpr(column, _))
  }

  test("createExpr with String value") {
    checkAstWithSrcPosition(
      src => Variant.StringVal(StringVal(v = "test", src = src)),
      createExpr("test", _))
  }

  test("createExpr with Char value") {
    checkAstWithSrcPosition(
      src => Variant.StringVal(StringVal(v = "t", src = src)),
      createExpr('t', _))
  }

  test("createExpr with Int value") {
    checkAstWithSrcPosition(
      src => Variant.Int64Val(Int64Val(v = 1.toLong, src = src)),
      createExpr(1, _))
  }

  test("createExpr with Byte value") {
    checkAstWithSrcPosition(
      src => Variant.Int64Val(Int64Val(v = 1.toLong, src = src)),
      createExpr(1.toByte, _))
  }

  test("createExpr with Short value") {
    checkAstWithSrcPosition(
      src => Variant.Int64Val(Int64Val(v = 1.toLong, src = src)),
      createExpr(1.toShort, _))
  }

  test("createExpr with Long value") {
    checkAstWithSrcPosition(
      src => Variant.Int64Val(Int64Val(v = 1.toLong, src = src)),
      createExpr(1.toLong, _))
  }

  test("createExpr with Float value") {
    checkAstWithSrcPosition(
      src => Variant.Float64Val(Float64Val(v = 1.0, src = src)),
      createExpr(1.0f, _))
  }

  test("createExpr with Double value") {
    checkAstWithSrcPosition(
      src => Variant.Float64Val(Float64Val(v = 1.0, src = src)),
      createExpr(1.0, _))
  }

  test("createExpr with Boolean value") {
    checkAstWithSrcPosition(
      src => Variant.BoolVal(BoolVal(v = true, src = src)),
      createExpr(true, _))
  }

  test("createExpr with BigDecimal value") {
    checkAstWithSrcPosition(
      src =>
        Variant.BigDecimalVal(
          BigDecimalVal(
            scale = 2,
            unscaledValue =
              ByteString.copyFrom(java.math.BigDecimal.valueOf(12.34).unscaledValue().toByteArray),
            src = src)),
      createExpr(BigDecimal(12.34), _))
  }

  test("createExpr with JavaBigDecimal value") {
    checkAstWithSrcPosition(
      src =>
        Variant.BigDecimalVal(
          BigDecimalVal(
            scale = 2,
            unscaledValue =
              ByteString.copyFrom(java.math.BigDecimal.valueOf(12.34).unscaledValue().toByteArray),
            src = src)),
      createExpr(java.math.BigDecimal.valueOf(12.34), _))
  }

  test("createExpr with Array[Byte] value") {
    checkAstWithSrcPosition(
      src => Variant.BinaryVal(BinaryVal(v = ByteString.copyFrom(Array[Byte](1, 2, 3)), src = src)),
      createExpr(Array[Byte](1, 2, 3), _))
  }

  test("createExpr with unsupported value type") {
    checkException[IllegalArgumentException]("Unsupported value type:") {
      createExpr(this, null)
    }
  }
}
