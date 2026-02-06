package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ErrorMessage
import com.snowflake.snowpark.internal.analyzer.Expression
import com.snowflake.snowpark.internal.{SnowflakeUDF, UdfColumnSchema}

/**
 * Encapsulates a user-defined aggregate function (UDAF) that is returned by
 * [[UDAFRegistration.registerTemporary(udaf* UDAFRegistration.registerTemporary]] or
 * [[UDAFRegistration.registerPermanent]].
 *
 * Use [[AggregateFunction!.apply AggregateFunction.apply]] to generate [[Column]] expressions from
 * an instance.
 * {{{
 *   val myAvg = session.udaf.registerTemporary("my_avg", new MyAvgUDAF())
 *   df.select(myAvg(col("a")))
 * }}}
 *
 * @since 1.19.0
 */
case class AggregateFunction private[snowpark] (
    f: AnyRef,
    private[snowpark] val returnType: UdfColumnSchema,
    private[snowpark] val inputTypes: Seq[UdfColumnSchema] = Nil,
    name: Option[String] = None) {

  /**
   * Apply the UDAF to one or more columns to generate a [[Column]] expression.
   *
   * @since 1.19.0
   */
  def apply(exprs: Column*): Column = {
    new Column(createUDAFExpression(exprs.map(_.expr)))
  }

  private[snowpark] def withName(name: String): AggregateFunction = {
    copy(name = Some(name))
  }

  private def createUDAFExpression(exprs: Seq[Expression]): SnowflakeUDF = {
    val udafName =
      name.getOrElse(throw ErrorMessage.UDF_FOUND_UNREGISTERED_UDF())
    if (exprs.length != inputTypes.length) {
      throw ErrorMessage.UDF_INCORRECT_ARGS_NUMBER(inputTypes.length, exprs.length)
    }
    SnowflakeUDF(udafName, exprs, returnType.dataType, returnType.isOption)
  }
}
