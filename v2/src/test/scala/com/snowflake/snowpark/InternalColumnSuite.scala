package com.snowflake.snowpark

import com.snowflake.snowpark.internal.SrcPositionInfo
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.internal.AstUtils._

class InternalColumnSuite extends UnitTestBase {

  test("column in") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = Column(expr)
    checkAst(
      Expr(
        Expr.Variant.ColumnIn(ColumnIn(
          col = Some(expr),
          src = src,
          values = Seq(
            Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src))),
            Expr(Expr.Variant.StringVal(StringVal(v = "b", src = src))),
            Expr(Expr.Variant.StringVal(StringVal(v = "c", src = src))))))),
      column.in(Seq("a", "b", "c"))(srcPositionInfo))
  }

  test("column apply with string field name") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val columnName = "dummyColumnName"
    val column = Column(expr).apply(columnName)(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnApplyString(
          ColumnApply_String(
            col = Some(expr),
            field = columnName,
            src = src
          )
        )
      ),
      column
    )
  }

  test("column apply with int field name") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val index = 1
    val column = Column(expr).apply(index)(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnApplyInt(
          ColumnApply_Int(
            col = Some(expr),
            idx = index,
            src = src
          )
        )
      ),
      column
    )
  }

  test("unary minus") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = (-Column(expr))(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.Neg(
          Neg(
            operand = Some(expr),
            src = src
          )
        )
      ),
      column
    )
  }

  test("unary not") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = (!Column(expr))(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.Not(
          Not(
            operand = Some(expr),
            src = src
          )
        )
      ),
      column
    )
  }
}
