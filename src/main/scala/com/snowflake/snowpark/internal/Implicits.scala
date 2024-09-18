package com.snowflake.snowpark.internal

import com.snowflake.snowpark.{Column, DataFrame, Session}
import scala.reflect.runtime.universe.TypeTag

abstract class Implicits {
  protected def _session: Session

  /** Converts $"col name" into a [[Column]].
    */
  implicit class ColumnFromString(val sc: StringContext) {
    def $(args: Any*): Column = {
      Column(sc.s(args: _*))
    }
  }

  implicit def symbolToCol(s: Symbol): Column = Column(s.name)

  implicit def localSeqToDataframe[T: TypeTag](s: Seq[T]): DataFrame =
    _session.createDataFrame(s)

}
