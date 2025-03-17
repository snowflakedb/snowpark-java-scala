package com.snowflake.snowpark

import com.snowflake.snowpark.internal.SrcPositionInfo
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.internal.AstUtils._

class InternalColumnSuite extends UnitTestBase {

  test("column in") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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
    val src = createSrcPosition(srcPositionInfo)

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
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val index = 1
    val column = Column(expr).apply(index)(srcPositionInfo)

    checkAst(
      Expr(Expr.Variant.ColumnApplyInt(ColumnApply_Int(col = Some(expr), idx = index, src = src))),
      column)
  }

  test("column unary minus") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val column = (-Column(expr))(srcPositionInfo)

    checkAst(Expr(Expr.Variant.Neg(Neg(operand = Some(expr), src = src))), column)
  }

  test("column unary not") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val column = (!Column(expr))(srcPositionInfo)

    checkAst(Expr(Expr.Variant.Not(Not(operand = Some(expr), src = src))), column)
  }

  test("column === and is_equal") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column =!= and not_equal") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column > gt") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column < lt") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column <= leq") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column >= geq") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column <=> equal_null") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column equal_nan") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val column = Column(expr)

    val expectedExpr =
      Expr(Expr.Variant.ColumnEqualNan(ColumnEqualNan(col = Some(expr), src = src)))

    checkAst(expectedExpr, column.equal_nan(srcPositionInfo))
  }

  test("column is_null and isNull") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val column = Column(expr)

    val expectedExpr =
      Expr(Expr.Variant.ColumnIsNull(ColumnIsNull(col = Some(expr), src = src)))

    checkAst(expectedExpr, column.is_null(srcPositionInfo))
    checkAst(expectedExpr, column.isNull(srcPositionInfo))
  }

  test("column is_not_null") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val column = Column(expr)

    checkAst(
      Expr(Expr.Variant.ColumnIsNotNull(ColumnIsNotNull(col = Some(expr), src = src))),
      column.is_not_null(srcPositionInfo))
  }

  test("column || or") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column && and") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column between") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

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

  test("column + plus") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Add(Add(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 + column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.plus(column2)(srcPositionInfo))
  }

  test("column - minus") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Sub(Sub(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 - column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.minus(column2)(srcPositionInfo))
  }

  test("column * multiply") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Mul(Mul(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 * column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.multiply(column2)(srcPositionInfo))
  }

  test("column / divide") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Div(Div(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 / column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.divide(column2)(srcPositionInfo))
  }

  test("column % remainder") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr = Expr(Expr.Variant.Mod(Mod(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 % column2)(srcPositionInfo))
    checkAst(expectedExpr, column1.mod(column2)(srcPositionInfo))
  }

  test("column desc") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    implicit val src: Option[SrcPosition] = createSrcPosition(srcPositionInfo)

    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))))))
    val column = Column(expr).desc(src = srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnDesc(
          ColumnDesc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderDefault(true))),
            src = src))),
      column)
  }

  test("column desc_nulls_first") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    implicit val src: Option[SrcPosition] = createSrcPosition(srcPositionInfo)

    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))))))
    val column = Column(expr).desc_nulls_first(src = srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnDesc(
          ColumnDesc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsFirst(true))),
            src = src))),
      column)
  }

  test("column desc_nulls_last") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    implicit val src: Option[SrcPosition] = createSrcPosition(srcPositionInfo)

    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))))))
    val column = Column(expr).desc_nulls_last(src = srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnDesc(
          ColumnDesc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsLast(true))),
            src = src))),
      column)
  }

  test("column asc") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    implicit val src: Option[SrcPosition] = createSrcPosition(srcPositionInfo)

    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))))))
    val column = Column(expr).asc(src = srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnAsc(
          ColumnAsc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderDefault(true))),
            src = src))),
      column)
  }

  test("column asc_nulls_first") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    implicit val src: Option[SrcPosition] = createSrcPosition(srcPositionInfo)

    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))))))
    val column = Column(expr).asc_nulls_first(src = srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnAsc(
          ColumnAsc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsFirst(true))),
            src = src))),
      column)
  }

  test("column asc_nulls_last") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    implicit val src: Option[SrcPosition] = createSrcPosition(srcPositionInfo)

    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src)))))))
    val column = Column(expr).asc_nulls_last(src = srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnAsc(
          ColumnAsc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsLast(true))),
            src = src))),
      column)
  }

  test("column bitor") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 6, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr =
      Expr(Expr.Variant.BitOr(BitOr(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 bitor column2)(srcPositionInfo))
  }

  test("column bitand") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 6, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr =
      Expr(Expr.Variant.BitAnd(BitAnd(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 bitand column2)(srcPositionInfo))
  }

  test("column bitxor") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 6, src = src)))
    val column1 = Column(expr1)
    val column2 = Column(expr2)

    val expectedExpr =
      Expr(Expr.Variant.BitXor(BitXor(lhs = Some(expr1), rhs = Some(expr2), src = src)))

    checkAst(expectedExpr, (column1 bitxor column2)(srcPositionInfo))
  }

  test("column like with pattern") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val pattern = "tesT"
    val column = Column(expr).like(Column(createExpr(pattern, srcPositionInfo)))(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnStringLike(
          ColumnStringLike(
            col = Some(expr),
            pattern = Some(createExpr(pattern, srcPositionInfo)),
            src = src))),
      column)
  }

  test("column regexp") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val pattern = "tesT"
    val column = Column(expr).regexp(Column(createExpr(pattern, srcPositionInfo)))(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnRegexp(
          ColumnRegexp(
            col = Some(expr),
            pattern = Some(createExpr(pattern, srcPositionInfo)),
            src = src))),
      column)
  }

  test("column withinGroup") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    implicit val src: Option[SrcPosition] = createSrcPosition(srcPositionInfo)

    val cols = Seq(Column(Expr()), Column(Expr()))
    val expr = Expr()
    val column = Column(expr).withinGroup(cols)(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnWithinGroup(
          ColumnWithinGroup(
            col = Some(expr),
            cols = Some(ExprArgList(cols.map(col => col.expr))),
            src = src))),
      column)
  }

  test("column collate") {
    val srcPositionInfo = SrcPositionInfo("test", 1, 12)
    val src = createSrcPosition(srcPositionInfo)

    val expr = Expr()
    val spec = "tesT"
    val column = Column(expr).collate(spec)(srcPositionInfo)

    checkAst(
      Expr(
        Expr.Variant.ColumnStringCollate(
          ColumnStringCollate(
            col = Some(expr),
            collationSpec = Some(createExpr(spec, srcPositionInfo)),
            src = src))),
      column)
  }
}
