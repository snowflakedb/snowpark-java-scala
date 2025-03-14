package com.snowflake.snowpark.internal

import com.snowflake.snowpark.proto.ast.{Assign, Eval, Expr, Request, Stmt, VarId}

import scala.collection.mutable.ListBuffer

class AstBatch(filenameTable: FilenameTable = AstUtils.filenameTable) {
  private var stmts: ListBuffer[Stmt] = ListBuffer.empty

  def add(expr: Expr = null, symbol: String = null): VarId = this.synchronized {
    addWithoutLock(expr, symbol)
  }

  def flush(expr: Expr = null, symbol: String = null): Request = this.synchronized {
    val varId = addWithoutLock(expr, symbol)
    stmts += Stmt(Stmt.Variant.Eval(Eval(uid = getStmtId, varId = Some(varId))))

    val body = stmts.toSeq
    // reset stmts list
    stmts = ListBuffer.empty

    Request(
      internedValueTable = filenameTable.getInternedValueTable,
      body = body,
      clientAstVersion = AstUtils.astVersion,
      clientLanguage = AstUtils.language,
      clientVersion = Some(AstUtils.clientVersion))
  }

  private def addWithoutLock(expr: Expr = null, symbol: String = null): VarId = {
    val id: Long = getStmtId
    val varId: VarId = VarId(id)
    stmts +=
      Stmt(
        Stmt.Variant.Assign(
          Assign(expr = Option(expr), symbol = Option(symbol), uid = id, varId = Some(varId))))
    varId
  }

  // statement id generator
  private var lastId: Long = 0
  private def getStmtId: Long = {
    lastId += 1
    lastId
  }
}
