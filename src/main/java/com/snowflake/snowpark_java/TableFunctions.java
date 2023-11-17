package com.snowflake.snowpark_java;

/**
 * Provides utility functions that generate table function expressions that can be passed to
 * DataFrame join method and Session tableFunction method.
 *
 * <p>This object also provides functions that correspond to Snowflake <a
 * href="https://docs.snowflake.com/en/sql-reference/functions-table.html">system-defined table
 * functions</a>
 *
 * @since 1.2.0
 */
public class TableFunctions {

  // create a private default constructor,
  // to disable creating instance of this class
  private TableFunctions() {}

  /**
   * This table function splits a string (based on a specified delimiter) and flattens the results
   * into rows.
   *
   * <p>Argument List:
   *
   * <p>First argument (no name): Required. Text to be split.
   *
   * <p>Second argument (no name): Required. Text to split string by.
   *
   * <p>Example
   *
   * <pre>{@code
   * session.tableFunction(TableFunctions.split_to_table(),
   *   Functions.lit("split by space"), Functions.lit(" "));
   * }</pre>
   *
   * @since 1.2.0
   * @return The result TableFunction reference
   */
  public static TableFunction split_to_table() {
    return new TableFunction(com.snowflake.snowpark.tableFunctions.split_to_table());
  }

  /**
   * This table function splits a string (based on a specified delimiter) and flattens the results
   * into rows.
   *
   * <p>Example
   *
   * <pre>{@code
   * session.tableFunction(TableFunctions.split_to_table(,
   *   Functions.lit("split by space"), Functions.lit(" ")));
   * }</pre>
   *
   * @since 1.10.0
   * @param str Text to be split.
   * @param delimiter Text to split string by.
   * @return The result Column reference
   */
  public static Column split_to_table(Column str, String delimiter) {
    return new Column(
        com.snowflake.snowpark.tableFunctions.split_to_table(str.toScalaColumn(), delimiter));
  }

  /**
   * Flattens (explodes) compound values into multiple rows.
   *
   * <p>Argument List:
   *
   * <p>input: Required. The expression that will be unseated into rows. The expression must be of
   * data type VariantType, MapType or ArrayType.
   *
   * <p>path: Optional. The path to the element within a VariantType data structure which needs to
   * be flattened. Can be a zero-length string (i.e. empty path) if the outermost element is to be
   * flattened. Default: Zero-length string (i.e. empty path)
   *
   * <p>outer: Optional boolean value. If FALSE, any input rows that cannot be expanded, either
   * because they cannot be accessed in the path or because they have zero fields or entries, are
   * completely omitted from the output. If TRUE, exactly one row is generated for zero-row
   * expansions (with NULL in the KEY, INDEX, and VALUE columns). Default: FALSE
   *
   * <p>recursive: Optional boolean value If FALSE, only the element referenced by PATH is expanded.
   * If TRUE, the expansion is performed for all sub-elements recursively. Default: FALSE
   *
   * <p>mode: Optional String ("object", "array", or "both") Specifies whether only objects, arrays,
   * or both should be flattened. Default: both
   *
   * <p>Example
   *
   * <pre>{@code
   * Map<String, Column> args = new HashMap<>();
   * args.put("input", Functions.parse_json(Functions.lit("[1,2]")));
   * session.tableFunction(TableFunctions.flatten(), args);
   * }</pre>
   *
   * @since 1.2.0
   * @return The result TableFunction reference
   */
  public static TableFunction flatten() {
    return new TableFunction(com.snowflake.snowpark.tableFunctions.flatten());
  }

  /**
   * Flattens (explodes) compound values into multiple rows.
   *
   * <p>Example
   *
   * <pre>{@code
   * df.join(TableFunctions.flatten(
   *   Functions.parse_json(df.col("col")), "path", true, true, "both"));
   * }</pre>
   *
   * @since 1.10.0
   * @param input The expression that will be unseated into rows. The expression must be of data
   *     type VariantType, MapType or ArrayType.
   * @param path The path to the element within a VariantType data structure which needs to be
   *     flattened. Can be a zero-length string (i.e. empty path) if the outermost element is to be
   *     flattened. Default: Zero-length string (i.e. empty path)
   * @param outer If FALSE, any input rows that cannot be expanded, either because they cannot be
   *     accessed in the path or because they have zero fields or entries, are completely omitted
   *     from the output. If TRUE, exactly one row is generated for zero-row expansions (with NULL
   *     in the KEY, INDEX, and VALUE columns).
   * @param recursive If FALSE, only the element referenced by PATH is expanded. If TRUE, the
   *     expansion is performed for all sub-elements recursively. Default: FALSE
   * @param mode ("object", "array", or "both") Specifies whether only objects, arrays, or both
   *     should be flattened.
   * @return The result Column reference
   */
  public static Column flatten(
      Column input, String path, boolean outer, boolean recursive, String mode) {
    return new Column(
        com.snowflake.snowpark.tableFunctions.flatten(
            input.toScalaColumn(), path, outer, recursive, mode));
  }

  /**
   * Flattens (explodes) compound values into multiple rows.
   *
   * <p>Example
   *
   * <pre>{@code
   * df.join(TableFunctions.flatten(
   *   Functions.parse_json(df.col("col"))));
   * }</pre>
   *
   * @since 1.10.0
   * @param input The expression that will be unseated into rows. The expression must be of data
   *     type VariantType, MapType or ArrayType.
   * @return The result Column reference
   */
  public static Column flatten(Column input) {
    return new Column(com.snowflake.snowpark.tableFunctions.flatten(input.toScalaColumn()));
  }
}
