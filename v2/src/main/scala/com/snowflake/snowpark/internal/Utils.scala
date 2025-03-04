package com.snowflake.snowpark.internal

import scala.reflect.macros.blackbox
import scala.language.experimental.macros

object Utils {

  def withSourcePosition[T](expr: T): T = macro sourcePositionImpl[T]

  def sourcePositionImpl[T](c: blackbox.Context)(expr: c.Expr[T]): c.Expr[T] = {
    import c.universe._
    val pos = c.enclosingPosition
    val fileName = pos.source.file.name
    val lineNumber = pos.line
    val columnNumber = pos.column
    val srcPosition = s"Source position:  $fileName, $lineNumber, $columnNumber"
    c.Expr[T] (
      q"""
         {
              println($srcPosition)
              $expr
         }
       """
    )
  }
}
