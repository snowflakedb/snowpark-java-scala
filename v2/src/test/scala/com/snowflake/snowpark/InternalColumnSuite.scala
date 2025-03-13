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
          ColumnApply_String(col = Some(expr), field = columnName, src = src))),
      column)
  }

  test("column apply with int field name") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val index = 1
    val column = Column(expr).apply(index)(srcPositionInfo)

    checkAst(
      Expr(Expr.Variant.ColumnApplyInt(ColumnApply_Int(col = Some(expr), idx = index, src = src))),
      column)
  }

  test("unary minus") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = (-Column(expr))(srcPositionInfo)

    checkAst(Expr(Expr.Variant.Neg(Neg(operand = Some(expr), src = src))), column)
  }

  test("unary not") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = (!Column(expr))(srcPositionInfo)

    checkAst(Expr(Expr.Variant.Not(Not(operand = Some(expr), src = src))), column)
  }

  test("=== and is_equal") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src)))
    val expr2 = Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.Eq(
        Eq(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 === column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 equal_to column2)(srcPositionInfo))
  }

  test("=!= and not_equal") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src)))
    val expr2 = Expr(Expr.Variant.StringVal(StringVal(v = "b", src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.Neq(
        Neq(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.StringVal(StringVal(v = "b", src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 =!= column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 not_equal column2)(srcPositionInfo))
  }

  test("> gt") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.Gt(
        Gt(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 > column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 gt column2)(srcPositionInfo))
  }

  test("< lt") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.Lt(
        Lt(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 < column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 lt column2)(srcPositionInfo))
  }

  test("<= leq") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.Leq(
        Leq(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 <= column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 leq column2)(srcPositionInfo))
  }

  test(">= geq") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.Geq(
        Geq(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 >= column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 geq column2)(srcPositionInfo))
  }

  test("<=> equal_null") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.NullVal(NullVal(src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.ColumnEqualNull(
        ColumnEqualNull(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.NullVal(NullVal(src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 <=> column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 equal_null column2)(srcPositionInfo))
  }

  test("equal_nan") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = Column(expr)

    val expectedExpr =
      Expr(Expr.Variant.ColumnEqualNan(ColumnEqualNan(col = Some(expr), src = src)))

    checkAst(expectedExpr, column.equal_nan(srcPositionInfo))
  }

  test("is_null and isNull") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = Column(expr)

    val expectedExpr =
      Expr(Expr.Variant.ColumnIsNull(ColumnIsNull(col = Some(expr), src = src)))

    checkAst(expectedExpr, column.is_null(srcPositionInfo))
    checkAst(expectedExpr, column.isNull(srcPositionInfo))
  }

  test("is_not_null") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr()
    val column = Column(expr)

    checkAst(
      Expr(Expr.Variant.ColumnIsNotNull(ColumnIsNotNull(col = Some(expr), src = src))),
      column.is_not_null(srcPositionInfo))
  }

  test("|| or") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.BoolVal(BoolVal(v = true, src = src)))
    val expr2 = Expr(Expr.Variant.BoolVal(BoolVal(v = false, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.Or(
        Or(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.BoolVal(BoolVal(v = false, src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 || column2)(srcPositionInfo))
    checkAst(expectedExpr, (column1 or column2)(srcPositionInfo))
  }

  test("&& and") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.BoolVal(BoolVal(v = true, src = src)))
    val expr2 = Expr(Expr.Variant.BoolVal(BoolVal(v = false, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(
      Expr.Variant.And(
        And(
          lhs = Some(expr1),
          rhs = Some(Expr(Expr.Variant.BoolVal(BoolVal(v = false, src = src)))),
          src = src)))

    checkAst(expectedExpr, (column1 && column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.and(column2)(srcPositionInfo))
  }

  test("between") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr = Expr(Expr.Variant.Int64Val(Int64Val(v = 5, src = src)))
    val lowerExpr = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src)))
    val higherExpr = Expr(Expr.Variant.Int64Val(Int64Val(v = 9, src = src)))

    val col = Column(expr)
    val lowerCol = Column(lowerExpr)
    val higherCol = Column(higherExpr)

    val expectedExpr = Expr(
      Expr.Variant.And(And(
        lhs = Some(Expr(Expr.Variant.Geq(Geq(lhs = Some(expr), rhs = Some(lowerExpr), src = src)))),
        rhs =
          Some(Expr(Expr.Variant.Leq(Leq(lhs = Some(expr), rhs = Some(higherExpr), src = src)))),
        src = src)))

    checkAst(expectedExpr, col.between(lowerCol, higherCol)(srcPositionInfo))
  }

  test("+ plus") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Add(Add(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 + column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.plus(column2)(srcPositionInfo))
  }

  test("- minus") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Sub(Sub(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 - column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.minus(column2)(srcPositionInfo))
  }

  test("* multiply") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Mul(Mul(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 * column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.multiply(column2)(srcPositionInfo))
  }

  test("/ divide") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Div(Div(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 / column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.divide(column2)(srcPositionInfo))
  }

  test("% remainder") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSroPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Mod(Mod(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 % column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.mod(column2)(srcPositionInfo))
  }
}
