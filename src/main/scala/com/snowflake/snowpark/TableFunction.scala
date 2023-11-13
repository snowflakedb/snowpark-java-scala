package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer
import com.snowflake.snowpark.internal.analyzer.{
  NamedArgumentsTableFunction,
  TableFunctionExpression
}

/**
 * Looks up table functions by funcName and returns tableFunction object
 * which can be used in DataFrame.join and Session.tableFunction methods.
 *
 * It can reference both system-defined table function and
 * user-defined table functions.
 *
 * Example
 * {{{
 *    import com.snowflake.snowpark.functions._
 *    import com.snowflake.snowpark.TableFunction
 *
 *    session.tableFunction(
 *      TableFunction("flatten"),
 *      Map("input" -> parse_json(lit("[1,2]")))
 *    )
 *
 *    df.join(TableFunction("split_to_table"), df("a"), lit(","))
 * }}}
 *
 * @param funcName table function name,
 *                 can be a short name like func or
 *                 a fully qualified name like database.schema.func
 * @since 0.4.0
 */
case class TableFunction(funcName: String) {
  private[snowpark] def call(args: Column*): TableFunctionExpression =
    analyzer.TableFunction(funcName, args.map(_.expr))

  private[snowpark] def call(args: Map[String, Column]): TableFunctionExpression =
    NamedArgumentsTableFunction(funcName, args.map {
      case (key, value) => key -> value.expr
    })

  def apply(args: Column*): Column = Column(this.call(args: _*))

  def apply(args: Map[String, Column]): Column = Column(this.call(args))
}
