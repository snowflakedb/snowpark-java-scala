package com.snowflake.snowpark_java;

/**
 * Represents a <a href="https://docs.snowflake.com/en/sql-reference/functions/case.html">CASE</a>
 * expression.
 *
 * <p>To construct this object for a CASE expression, call the {@code
 * com.snowflake.snowpark_java.Functions.when}. specifying a condition and the corresponding result
 * for that condition. Then, call the {@code when} and {@code otherwise} methods to specify
 * additional conditions and results.
 *
 * @since 0.12.0
 */
public class CaseExpr extends Column {
  private final com.snowflake.snowpark.CaseExpr caseExpr;

  CaseExpr(com.snowflake.snowpark.CaseExpr caseExpr) {
    super(caseExpr);
    this.caseExpr = caseExpr;
  }

  /**
   * Appends one more WHEN condition to the CASE expression.
   *
   * @since 0.12.0
   * @param condition The case condition
   * @param value The result value in the given condition
   * @return The result case expression
   */
  public CaseExpr when(Column condition, Column value) {
    return new CaseExpr(caseExpr.when(condition.toScalaColumn(), value.toScalaColumn()));
  }

  /**
   * Sets the default result for this CASE expression.
   *
   * @since 0.12.0
   * @param value The default value
   * @return The result column
   */
  public Column otherwise(Column value) {
    return new Column(caseExpr.otherwise(value.toScalaColumn()));
  }
}
