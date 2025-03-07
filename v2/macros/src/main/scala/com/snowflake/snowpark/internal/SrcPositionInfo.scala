package com.snowflake.snowpark.internal

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class SrcPositionInfo(filename: String, line: Int, column: Int)
object SrcPositionInfo {
  implicit def srcPosition: SrcPositionInfo = macro getSrcPosition

  def getSrcPosition(c: blackbox.Context): c.Expr[SrcPositionInfo] = {
    import c.universe._
    val pos = c.enclosingPosition
    val filename = pos.source.file.name
    val line = pos.line
    val column = pos.column

    // scalastyle:off
    c.Expr(
      q"com.snowflake.snowpark.internal.SrcPositionInfo($filename, $line, $column)")
    // scalastyle:on
  }
}
