package com.snowflake.snowpark

import com.snowflake.snowpark.internal.AstUtils.createSrcPosition
import com.snowflake.snowpark.internal.{AstBatch, AstUtils, FilenameTable, SrcPositionInfo}
import com.snowflake.snowpark.proto.ast.{Assign, Eval, Expr, Neg, Not, Request, Stmt, VarId}

class AstBatchSuite extends UnitTestBase {
  test("add and flush") {
    val filenameTable = new FilenameTable
    val batch = new AstBatch(filenameTable)
    val srcPositionInfo1 = SrcPositionInfo("test1", 12, 123)
    val src1 = createSrcPosition(srcPositionInfo1, filenameTable)
    val expr1: Expr = Expr(Expr.Variant.Neg(Neg(operand = Some(Expr()), src = src1)))
    val srcPositionInfo2 = SrcPositionInfo("test2", 22, 223)
    val src2 = createSrcPosition(srcPositionInfo2, filenameTable)
    val expr2 = Expr(Expr.Variant.Not(Not(operand = Some(Expr()), src = src2)))

    batch.add(expr = expr1)
    val request = batch.flush(expr = expr2)

    checkAst(
      Request(
        internedValueTable = filenameTable.getInternedValueTable,
        body = Seq(
          Stmt(
            Stmt.Variant.Assign(
              Assign(expr = Option(expr1), symbol = None, uid = 1, varId = Some(VarId(1))))),
          Stmt(
            Stmt.Variant.Assign(
              Assign(expr = Option(expr2), symbol = None, uid = 2, varId = Some(VarId(2))))),
          Stmt(Stmt.Variant.Eval(Eval(uid = 3, varId = Some(VarId(2)))))),
        clientAstVersion = AstUtils.astVersion,
        clientLanguage = AstUtils.language,
        clientVersion = Some(AstUtils.clientVersion)),
      request)

  }
}
