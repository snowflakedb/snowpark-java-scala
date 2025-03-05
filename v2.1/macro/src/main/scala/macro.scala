package com.snowflake.snowpark

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class SrcPosition(filename: String, line: Int)

object DataframeMacro {
  // TODO: provide an instance of Liftable[_] for `SrcPosition` above.
  // ...or maybe just an implicit conversion from this tuple type to SrcPosition.
  type SrcPosition = (String, Int, Int)

  def fillna(c: blackbox.Context)(n: c.Expr[Int]): c.Expr[Int] = {
    import c.universe._

    val self = c.prefix.tree

    val pos = c.macroApplication.pos
    val filename = pos.source.file.name
    val line = pos.line
    val column = pos.column
    // TODO: more to explore here -- we do get the beginning/end point of the entire invocation.

    c.Expr[Int] {
      q"""$self.fillnaImpl($n, ($filename, $line, $column))"""
    }
  }
}
