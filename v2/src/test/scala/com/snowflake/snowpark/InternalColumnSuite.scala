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
}
