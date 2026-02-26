package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;

/**
 * Encapsulates a user-defined aggregate function (UDAF) that is returned by {@code
 * UDAFRegistration.registerTemporary} or {@code UDAFRegistration.registerPermanent}.
 *
 * <p>Use {@code AggregateFunction.apply} to generate {@code Column} expressions from an instance.
 *
 * <pre>{@code
 * AggregateFunction myAvg = session.udaf().registerTemporary("my_avg", new MyAvgUDAF());
 * df.select(myAvg.apply(df.col("a")));
 * }</pre>
 *
 * @since 1.19.0
 */
public class AggregateFunction {
  private final com.snowflake.snowpark.AggregateFunction udaf;

  AggregateFunction(com.snowflake.snowpark.AggregateFunction udaf) {
    this.udaf = udaf;
  }

  com.snowflake.snowpark.AggregateFunction toScalaAggregateFunction() {
    return this.udaf;
  }

  /**
   * Apply the UDAF to one or more columns to generate a Column expression.
   *
   * @param exprs The list of column expressions to pass to the UDAF.
   * @return The result column.
   * @since 1.19.0
   */
  public Column apply(Column... exprs) {
    return new Column(udaf.apply(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(exprs))));
  }
}
