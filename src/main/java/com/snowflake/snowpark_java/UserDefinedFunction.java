package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;

/**
 * Encapsulates a user defined lambda or function that is returned by {@code
 * UDFRegistration.registerTemporary}, {@code UDFRegistration.registerPermanent}, or {@code
 * Functions.udf}.
 *
 * <p>Use {@code UserDefinedFunction.apply} to generate {@code Column} expressions from an instance.
 *
 * <pre>{@code
 * import com.snowflake.snowpark_java.functions;
 * import com.snowflake.snowpark_java.types.DataTypes;
 *
 * UserDefinedFunction myUdf = Functions.udf(
 *   (Integer x, Integer y) -> x + y,
 *   new DataType[]{DataTypes.IntegerType, DataTypes.IntegerType},
 *   DataTypes.IntegerType
 * );
 * df.select(myUdf.apply(df.col("col2"), df.col("col2")));
 * }</pre>
 *
 * @since 0.12.0
 */
public class UserDefinedFunction {
  private final com.snowflake.snowpark.UserDefinedFunction udf;

  UserDefinedFunction(com.snowflake.snowpark.UserDefinedFunction udf) {
    this.udf = udf;
  }

  /**
   * Apply the UDF to one or more columns to generate a Column expression.
   *
   * @since 0.12.0
   * @param exprs The list of expression.
   * @return The result column
   */
  public Column apply(Column... exprs) {
    return new Column(udf.apply(JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(exprs))));
  }
}
