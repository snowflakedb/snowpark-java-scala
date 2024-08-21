package com.snowflake.snowpark.internal

import com.snowflake.snowpark.internal.analyzer.Expression
import com.snowflake.snowpark.types.DataType

case class SnowflakeUDF(
    udfName: String,
    override val children: Seq[Expression],
    dataType: DataType,
    override val nullable: Boolean = true,
    udfDeterministic: Boolean = true)
    extends Expression {
  override protected def createAnalyzedExpression(analyzedChildren: Seq[Expression]): Expression =
    SnowflakeUDF(udfName, analyzedChildren, dataType, nullable, udfDeterministic)
}
