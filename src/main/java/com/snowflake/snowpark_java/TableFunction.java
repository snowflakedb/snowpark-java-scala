package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.HashMap;
import java.util.Map;

/**
 * Looks up table functions by funcName and returns tableFunction object which can be used in {@code
 * DataFrame.join} and {@code Session.tableFunction} methods.
 *
 * <p>It can reference both system-defined table function and user-defined table functions.
 *
 * @since 1.2.0
 */
public class TableFunction {
  private final com.snowflake.snowpark.TableFunction func;

  TableFunction(com.snowflake.snowpark.TableFunction func) {
    this.func = func;
  }

  /**
   * Create a new table function reference.
   *
   * @param funcName A string function name.
   * @since 1.2.0
   */
  public TableFunction(String funcName) {
    this(new com.snowflake.snowpark.TableFunction(funcName));
  }

  com.snowflake.snowpark.TableFunction getScalaTableFunction() {
    return this.func;
  }

  /**
   * Returns the function name.
   *
   * @return A string function name.
   * @since 1.4.0
   */
  public String funcName() {
    return func.funcName();
  }

  /**
   * Create a Column reference by passing arguments in the TableFunction object.
   *
   * @param args A list of Column objects representing the arguments of the given table function
   * @return A Column reference
   * @since 1.10.0
   */
  public Column call(Column... args) {
    return new Column(this.func.apply(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(args))));
  }

  /**
   * Create a Column reference by passing arguments in the TableFunction object.
   *
   * @param args function arguments map of the given table function. Some functions, like flatten,
   *             have named parameters. use this map to assign values to the corresponding parameters.
   * @return A Column reference
   * @since 1.10.0
   */
  public Column call(Map<String, Column> args) {
    Map<String, com.snowflake.snowpark.Column> scalaArgs = new HashMap<>();
    for (Map.Entry<String, Column> entry : args.entrySet()) {
      scalaArgs.put(entry.getKey(), entry.getValue().toScalaColumn());
    }
    return new Column(this.func.apply(JavaUtils.javaStringColumnMapToScala(scalaArgs)));
  }
}
