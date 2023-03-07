package com.snowflake.snowpark

import com.snowflake.snowpark.internal.UdfColumnSchema

/**
 * The reference to a Stored Procedure which can be created by
 * `Session.sproc.register` methods, and used in `Session.storedProcedure`
 * method.
 *
 * For example:
 * {{{
 *   val sp = session.sproc.registerTemporary(
 *     (session: Session, num: Int) => {
 *       val result = session.sql(s"select $num").collect().head.getInt(0)
 *       result + 100
 *     })
 *   session.storedProcedure(sp, 123).show()
 * }}}
 *
 * @since 1.8.0
 */
case class StoredProcedure private[snowpark] (
    sp: AnyRef,
    private[snowpark] val returnType: UdfColumnSchema,
    private[snowpark] val inputTypes: Seq[UdfColumnSchema] = Nil,
    name: Option[String] = None) {
  private[snowpark] def withName(name: String): StoredProcedure =
    StoredProcedure(sp, returnType, inputTypes, Some(name))
}
