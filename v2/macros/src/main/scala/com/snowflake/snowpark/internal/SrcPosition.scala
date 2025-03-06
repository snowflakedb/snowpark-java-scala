package com.snowflake.snowpark.internal

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class SrcPosition(filename: String, line: Int, column: Int)
object SrcPosition {
  implicit def srcPosition: SrcPosition = macro getSrcPosition

  def getSrcPosition(c: blackbox.Context): c.Expr[SrcPosition] = {
    import c.universe._
    val pos = c.enclosingPosition
    val filename = pos.source.file.name
    val line = pos.line
    val column = pos.column

    // scalastyle:off
    c.Expr(
      q"com.snowflake.snowpark.internal.SrcPosition($filename, $line, $column)")
    // scalastyle:on
  }
}
