package com.snowflake.snowpark

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class SrcPosition(filename: String, line: Int, column: Int)

object SrcPosition {
  implicit def srcPosition: SrcPosition = macro getSrcPosition

  def getSrcPosition(c: blackbox.Context): c.Expr[SrcPosition] = {
    import c.universe._
    val pos = c.macroApplication.pos
    val filename = pos.source.file.name
    val line = pos.line
    val column = pos.column
    // TODO: more to explore here -- we do get the beginning/end point of the entire invocation.
    c.Expr[SrcPosition] {
      q"""com.snowflake.snowpark.SrcPosition($filename, $line, $column)"""
    }
  }
}
