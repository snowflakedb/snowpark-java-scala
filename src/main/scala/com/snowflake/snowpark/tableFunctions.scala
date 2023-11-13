package com.snowflake.snowpark

import com.snowflake.snowpark.functions.lit

// scalastyle:off
/**
 * Provides utility functions that generate table function expressions that can be
 * passed to DataFrame join method and Session tableFunction method.
 *
 * This object also provides functions that correspond to Snowflake
 * [[https://docs.snowflake.com/en/sql-reference/functions-table.html system-defined table functions]].
 *
 * The following examples demonstrate the use of some of these functions:
 * {{{
 *   import com.snowflake.snowpark.functions.parse_json
 *
 *   // Creates DataFrame from Session.tableFunction
 *   session.tableFunction(tableFunctions.flatten, Map("input" -> parse_json(lit("[1,2]"))))
 *   session.tableFunction(tableFunctions.split_to_table, "split by space", " ")
 *
 *   // DataFrame joins table function
 *   df.join(tableFunctions.flatten, Map("input" -> parse_json(df("a"))))
 *   df.join(tableFunctions.split_to_table, df("a"), ",")
 *
 *   // Invokes any table function including user-defined table function
 *    df.join(tableFunctions.tableFunction("flatten"), Map("input" -> parse_json(df("a"))))
 *    session.tableFunction(tableFunctions.tableFunction("split_to_table"), "split by space", " ")
 * }}}
 *
 * @since 0.4.0
 */
object tableFunctions {
  // scalastyle:on

  /**
   * This table function splits a string (based on a specified delimiter)
   * and flattens the results into rows.
   *
   * Argument List:
   *
   * First argument (no name): Required. Text to be split.
   *
   * Second argument (no name): Required. Text to split string by.
   *
   * Example
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   import com.snowflake.snowpark.tableFunctions._
   *
   *   df.join(tableFunctions.split_to_table, df("a"), lit(","))
   *   session.tableFunction(
   *     tableFunctions.split_to_table,
   *     lit("split by space"),
   *     lit(" ")
   *   )
   * }}}
   *
   * @since 0.4.0
   */
  lazy val split_to_table: TableFunction = TableFunction("split_to_table")

  /**
   * Flattens (explodes) compound values into multiple rows.
   *
   * Argument List:
   *
   * input: Required. The expression that will be unseated into rows.
   * The expression must be of data type VariantType, MapType or ArrayType.
   *
   * path: Optional. The path to the element within a VariantType data structure
   * which needs to be flattened. Can be a zero-length string (i.e. empty path)
   * if the outermost element is to be flattened.
   * Default: Zero-length string (i.e. empty path)
   *
   * outer: Optional boolean value.
   * If FALSE, any input rows that cannot be expanded,
   * either because they cannot be accessed in the path or because they have
   * zero fields or entries, are completely omitted from the output.
   * If TRUE, exactly one row is generated for zero-row expansions
   * (with NULL in the KEY, INDEX, and VALUE columns).
   * Default: FALSE
   *
   * recursive: Optional boolean value
   * If FALSE, only the element referenced by PATH is expanded.
   * If TRUE, the expansion is performed for all sub-elements recursively.
   * Default: FALSE
   *
   * mode: Optional String ("object", "array", or "both")
   * Specifies whether only objects, arrays, or both should be flattened.
   * Default: both
   *
   * Example
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   import com.snowflake.snowpark.tableFunctions._
   *
   *   df.join(
   *     tableFunctions.flatten,
   *     Map("input" -> parse_json(df("a"), "outer" -> lit(true)))
   *   )
   *
   *   session.tableFunction(
   *     tableFunctions.flatten,
   *     Map("input" -> parse_json(lit("[1,2]"), "mode" -> lit("array")))
   *   )
   * }}}
   *
   * @since 0.4.0
   */
  lazy val flatten: TableFunction = TableFunction("flatten")

  def flatten(input: Column): Column = Column(flatten.apply(input))

  def flatten(input: Column,
              path: String, outer: Boolean, recursive: Boolean, mode: String): Column =
    Column(
      flatten.apply(
        Map(
          "input" -> input,
          "path" -> lit(path),
          "outer" -> lit(outer),
          "recursive" -> lit(recursive),
          "mode" -> lit(mode))))
}
