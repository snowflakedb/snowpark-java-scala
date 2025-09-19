package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;

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
   * Appends one more WHEN condition to the CASE expression. This method handles any literal value
   * and converts it into a `Column` if applies.
   *
   * <p><b>Example:</b>
   *
   * <pre>{@code
   * Column result = when(col("age").lt(lit(18)), "Minor")
   * .when(col("age").lt(lit(65)), "Adult")
   * .otherwise("Senior");
   * }</pre>
   *
   * @param condition The case condition
   * @param value The result value in the given condition
   * @return The result case expression
   * @since 0.12.0
   */
  public CaseExpr when(Column condition, Object value) {
    return new CaseExpr(
        caseExpr.when(condition.toScalaColumn(), JavaUtils.toJavaColumn(value).toScalaColumn()));
  }

  /**
   * Sets the default result for this CASE expression. This method handles any literal value and
   * converts it into a `Column` if applies.
   *
   * <p><b>Example:</b>
   *
   * <pre>{@code
   * Column result = when(col("state").equal(lit("CA")), lit(1000))
   * .when(col("state").equal(lit("NY")), lit(2000))
   * .otherwise(1000);
   * }</pre>
   *
   * @param value The default value, which can be any literal (e.g., String, int, boolean) or a
   *     `Column`.
   * @return The result column.
   * @since 0.12.0
   */
  public Column otherwise(Object value) {
    return new Column(caseExpr.otherwise(JavaUtils.toJavaColumn(value).toScalaColumn()));
  }
}
