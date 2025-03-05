package com.snowflake.snowpark.internal

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class SrcPosition (fileName: String, line: Int, column: Int)

object SrcPosition {
  implicit def srcPosition: SrcPosition = macro getSrcPosition

  def getSrcPosition(c: blackbox.Context): c.Expr[SrcPosition] = {
    import c.universe._
    val pos = c.enclosingPosition
    val fileName = pos.source.file.name
    val line = pos.line
    val column = pos.column

    c.Expr(q"com.snowflake.snowpark.internal.SrcPosition($fileName, $line, $column)")
  }
}
