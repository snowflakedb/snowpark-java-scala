package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{AstUtils, SrcPositionInfo}
import com.snowflake.snowpark.proto.ast._
import com.snowflake.snowpark.internal.AstUtils._

class InternalColumnSuite extends UnitTestBase {

  lazy private val srcPositionInfo1 = SrcPositionInfo("test1", 1, 12)
  lazy private val src1 = createSrcPosition(srcPositionInfo1)
  lazy private val expr1 = Expr()
  lazy private val nameIndices1 = Set(AstUtils.filenameTable.getFileId("test1"))
  lazy private val column1 = Column(expr1, nameIndices1)
  lazy private val srcPositionInfo2 = SrcPositionInfo("test2", 1, 12)
  lazy private val src2 = createSrcPosition(srcPositionInfo2)
  lazy private val nameIndices2 = Set(AstUtils.filenameTable.getFileId("test2"))
  lazy private val nameIndices3 = nameIndices2 ++ nameIndices1
  lazy private val srcPositionInfo4 = SrcPositionInfo("test3", 1, 12)
  lazy private val src4 = createSrcPosition(srcPositionInfo4)
  lazy private val nameIndices4 = Set(AstUtils.filenameTable.getFileId("test3"))
  lazy private val nameIndices5 = nameIndices3 ++ nameIndices4
  lazy private val srcPositionInfo6 = SrcPositionInfo("test4", 1, 12)
  lazy private val src6 = createSrcPosition(srcPositionInfo6)
  lazy private val nameIndices6 = Set(AstUtils.filenameTable.getFileId("test4"))
  lazy private val nameIndices7 = nameIndices5 ++ nameIndices6

  test("column in") {
    checkAst(
      Expr(
        Expr.Variant.ColumnIn(ColumnIn(
          col = Some(expr1),
          src = src2,
          values = Seq(
            Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src2))),
            Expr(Expr.Variant.StringVal(StringVal(v = "b", src = src2))),
            Expr(Expr.Variant.StringVal(StringVal(v = "c", src = src2))))))),
      column1.in(Seq("a", "b", "c"))(srcPositionInfo2),
      nameIndices3)
  }

  test("column apply with string field name") {
    val columnName = "dummyColumnName"
    val column = column1.apply(columnName)(srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnApplyString(
          ColumnApply_String(col = Some(expr1), field = columnName, src = src2))),
      column,
      nameIndices3)
  }

  test("column apply with int field name") {
    val index = 1
    val column = column1.apply(index)(srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnApplyInt(ColumnApply_Int(col = Some(expr1), idx = index, src = src2))),
      column,
      nameIndices3)
  }

  test("column unary minus") {

    val column = (-column1)(srcPositionInfo2)

    checkAst(Expr(Expr.Variant.Neg(Neg(operand = Some(expr1), src = src2))), column, nameIndices3)
  }

  test("column unary not") {
    val column = (!column1)(srcPositionInfo2)

    checkAst(Expr(Expr.Variant.Not(Not(operand = Some(expr1), src = src2))), column, nameIndices3)
  }

  test("column === and is_equal") {
    val expr1 = Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src1)))
    val expr2 = Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Eq(Eq(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 === column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, (column1 equal_to column2)(srcPositionInfo4), nameIndices5)
  }

  test("column =!= and not_equal") {
    val expr1 = Expr(Expr.Variant.StringVal(StringVal(v = "a", src = src1)))
    val expr2 = Expr(Expr.Variant.StringVal(StringVal(v = "b", src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Neq(Neq(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 =!= column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, (column1 not_equal column2)(srcPositionInfo4), nameIndices5)
  }

  test("column > gt") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Gt(Gt(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 > column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, (column1 gt column2)(srcPositionInfo4), nameIndices5)
  }

  test("column < lt") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Lt(Lt(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 < column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, (column1 lt column2)(srcPositionInfo4), nameIndices5)
  }

  test("column <= leq") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Leq(Leq(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 <= column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, (column1 leq column2)(srcPositionInfo4), nameIndices5)
  }

  test("column >= geq") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Geq(Geq(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 >= column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, (column1 geq column2)(srcPositionInfo4), nameIndices5)
  }

  test("column <=> equal_null") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.NullVal(NullVal(src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(
      Expr.Variant.ColumnEqualNull(
        ColumnEqualNull(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 <=> column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, (column1 equal_null column2)(srcPositionInfo4), nameIndices5)
  }

  test("column equal_nan") {
    val expectedExpr =
      Expr(Expr.Variant.ColumnEqualNan(ColumnEqualNan(col = Some(expr1), src = src2)))

    checkAst(expectedExpr, column1.equal_nan(srcPositionInfo2), nameIndices3)
  }

  test("column is_null and isNull") {
    val expectedExpr =
      Expr(Expr.Variant.ColumnIsNull(ColumnIsNull(col = Some(expr1), src = src2)))

    checkAst(expectedExpr, column1.is_null(srcPositionInfo2), nameIndices3)
    checkAst(expectedExpr, column1.isNull(srcPositionInfo2), nameIndices3)
  }

  test("column is_not_null") {
    checkAst(
      Expr(Expr.Variant.ColumnIsNotNull(ColumnIsNotNull(col = Some(expr1), src = src2))),
      column1.is_not_null(srcPositionInfo2),
      nameIndices3)
  }

  test("column || or") {
    val expr1 = Expr(Expr.Variant.BoolVal(BoolVal(v = true, src = src1)))
    val expr2 = Expr(Expr.Variant.BoolVal(BoolVal(v = false, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Or(Or(lhs = Some(expr1), rhs = Some(expr2), src = src2)))

    checkAst(expectedExpr, (column1 || column2)(srcPositionInfo2), nameIndices3)
    checkAst(expectedExpr, (column1 or column2)(srcPositionInfo2), nameIndices3)
  }

  test("column && and") {
    val expr1 = Expr(Expr.Variant.BoolVal(BoolVal(v = true, src = src1)))
    val expr2 = Expr(Expr.Variant.BoolVal(BoolVal(v = false, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.And(And(lhs = Some(expr1), rhs = Some(expr2), src = src2)))

    checkAst(expectedExpr, (column1 && column2)(srcPositionInfo2), nameIndices3)
    checkAst(expectedExpr, column1.and(column2)(srcPositionInfo2), nameIndices3)
  }

  test("column between") {
    val expr = Expr(Expr.Variant.Int64Val(Int64Val(v = 5, src = src1)))
    val lowerExpr = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src2)))
    val higherExpr = Expr(Expr.Variant.Int64Val(Int64Val(v = 9, src = src4)))

    val col = Column(expr, nameIndices1)
    val lowerCol = Column(lowerExpr, nameIndices2)
    val higherCol = Column(higherExpr, nameIndices4)

    val expectedExpr = Expr(
      Expr.Variant.ColumnBetween(
        ColumnBetween(
          col = Some(expr),
          upperBound = Some(higherExpr),
          lowerBound = Some(lowerExpr),
          src = src6)))

    checkAst(expectedExpr, col.between(lowerCol, higherCol)(srcPositionInfo6), nameIndices7)
  }

  test("column + plus") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Add(Add(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 + column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, column1.plus(column2)(srcPositionInfo4), nameIndices5)
  }

  test("column - minus") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Sub(Sub(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 - column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, column1.minus(column2)(srcPositionInfo4), nameIndices5)
  }

  test("column * multiply") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Mul(Mul(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 * column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, column1.multiply(column2)(srcPositionInfo4), nameIndices5)
  }

  test("column / divide") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Div(Div(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 / column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, column1.divide(column2)(srcPositionInfo4), nameIndices5)
  }

  test("column % remainder") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 2, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr = Expr(Expr.Variant.Mod(Mod(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 % column2)(srcPositionInfo4), nameIndices5)
    checkAst(expectedExpr, column1.mod(column2)(srcPositionInfo4), nameIndices5)
  }

  test("column desc") {
    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src1))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))))))
    val column = Column(expr, nameIndices1).desc(src = srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnDesc(
          ColumnDesc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderDefault(true))),
            src = src2))),
      column,
      nameIndices3)
  }

  test("column desc_nulls_first") {
    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src1))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))))))
    val column = Column(expr, nameIndices1).desc_nulls_first(src = srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnDesc(
          ColumnDesc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsFirst(true))),
            src = src2))),
      column,
      nameIndices3)
  }

  test("column desc_nulls_last") {
    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src1))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))))))
    val column = Column(expr, nameIndices1).desc_nulls_last(src = srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnDesc(
          ColumnDesc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsLast(true))),
            src = src2))),
      column,
      nameIndices3)
  }

  test("column asc") {
    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src1))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))))))
    val column = Column(expr, nameIndices1).asc(src = srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnAsc(
          ColumnAsc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderDefault(true))),
            src = src2))),
      column,
      nameIndices3)
  }

  test("column asc_nulls_first") {
    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src1))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))))))
    val column = Column(expr, nameIndices1).asc_nulls_first(src = srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnAsc(
          ColumnAsc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsFirst(true))),
            src = src2))),
      column,
      nameIndices3)
  }

  test("column asc_nulls_last") {
    val expr = Expr(
      Expr.Variant.ListVal(
        ListVal(vs = Seq(
          Expr(Expr.Variant.NullVal(NullVal(src = src1))),
          Expr(Expr.Variant.Int64Val(Int64Val(v = 1, src = src1)))))))
    val column = Column(expr, nameIndices1).asc_nulls_last(src = srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnAsc(
          ColumnAsc(
            col = Some(expr),
            nullOrder = Some(NullOrder(variant = NullOrder.Variant.NullOrderNullsLast(true))),
            src = src2))),
      column,
      nameIndices3)
  }

  test("column bitor") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 6, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr =
      Expr(Expr.Variant.BitOr(BitOr(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 bitor column2)(srcPositionInfo4), nameIndices5)
  }

  test("column bitand") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 6, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr =
      Expr(Expr.Variant.BitAnd(BitAnd(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 bitand column2)(srcPositionInfo4), nameIndices5)
  }

  test("column bitxor") {
    val expr1 = Expr(Expr.Variant.Int64Val(Int64Val(v = 3, src = src1)))
    val expr2 = Expr(Expr.Variant.Int64Val(Int64Val(v = 6, src = src2)))
    val column1 = Column(expr1, nameIndices1)
    val column2 = Column(expr2, nameIndices2)

    val expectedExpr =
      Expr(Expr.Variant.BitXor(BitXor(lhs = Some(expr1), rhs = Some(expr2), src = src4)))

    checkAst(expectedExpr, (column1 bitxor column2)(srcPositionInfo4), nameIndices5)
  }

  test("column like with pattern") {
    val pattern = "tesT"
    val column =
      column1.like(Column(createExpr(pattern, srcPositionInfo2), nameIndices2))(srcPositionInfo4)

    checkAst(
      Expr(
        Expr.Variant.ColumnStringLike(
          ColumnStringLike(
            col = Some(expr1),
            pattern = Some(createExpr(pattern, srcPositionInfo2)),
            src = src4))),
      column,
      nameIndices5)
  }

  test("column regexp") {
    val pattern = "tesT"
    val column =
      column1.regexp(Column(createExpr(pattern, srcPositionInfo2), nameIndices2))(srcPositionInfo4)

    checkAst(
      Expr(
        Expr.Variant.ColumnRegexp(
          ColumnRegexp(
            col = Some(expr1),
            pattern = Some(createExpr(pattern, srcPositionInfo2)),
            src = src4))),
      column,
      nameIndices5)
  }

  test("column withinGroup") {
    val cols = Seq(column1, column1)
    val column = column1.withinGroup(cols)(srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnWithinGroup(
          ColumnWithinGroup(
            col = Some(expr1),
            cols = Some(ExprArgList(cols.map(col => col.expr))),
            src = src2))),
      column,
      nameIndices3)

    checkAst(
      Expr(
        Expr.Variant.ColumnWithinGroup(
          ColumnWithinGroup(
            col = Some(expr1),
            cols = Some(ExprArgList(cols.map(col => col.expr))),
            src = src2))),
      column1.withinGroup(column1, column1)(srcPositionInfo2),
      nameIndices3)
  }

  test("column collate") {
    val spec = "tesT"
    val column = column1.collate(spec)(srcPositionInfo2)

    checkAst(
      Expr(
        Expr.Variant.ColumnStringCollate(
          ColumnStringCollate(
            col = Some(expr1),
            collationSpec = Some(createExpr(spec, srcPositionInfo2)),
            src = src2))),
      column,
      nameIndices3)
  }
}
