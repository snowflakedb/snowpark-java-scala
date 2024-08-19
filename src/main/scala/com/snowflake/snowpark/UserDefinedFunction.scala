package com.snowflake.snowpark

import com.snowflake.snowpark.internal.ErrorMessage
import com.snowflake.snowpark.internal.analyzer.Expression
import com.snowflake.snowpark.internal.{SnowflakeUDF, UdfColumnSchema}

/** Encapsulates a user defined lambda or function that is returned by
  * [[UDFRegistration.registerTemporary[RT](name* UDFRegistration.registerTemporary]] or by
  * [[com.snowflake.snowpark.functions.udf[RT](* com.snowflake.snowpark.functions.udf]].
  *
  * Use [[UserDefinedFunction!.apply UserDefinedFunction.apply]] to generate [[Column]] expressions
  * from an instance.
  * {{{
  *   import com.snowflake.snowpark.functions._
  *   val myUdf = udf((x: Int, y: String) => y + x)
  *   df.select(myUdf(col("i"), col("s")))
  * }}}
  * @since 0.1.0
  */
case class UserDefinedFunction private[snowpark] (
    f: AnyRef,
    private[snowpark] val returnType: UdfColumnSchema,
    private[snowpark] val inputTypes: Seq[UdfColumnSchema] = Nil,
    name: Option[String] = None
) {

  /** Apply the UDF to one or more columns to generate a [[Column]] expression.
    * @since 0.1.0
    */
  def apply(exprs: Column*): Column = {
    new Column(createUDFExpression(exprs.map(_.expr)))
  }

  private[snowpark] def withName(name: String): UserDefinedFunction = {
    copy(name = Some(name))
  }

  private def createUDFExpression(exprs: Seq[Expression]): SnowflakeUDF = {
    val udfName =
      name.getOrElse(throw ErrorMessage.UDF_FOUND_UNREGISTERED_UDF())
    if (exprs.length != inputTypes.length) {
      throw ErrorMessage.UDF_INCORRECT_ARGS_NUMBER(inputTypes.length, exprs.length)
    }
    SnowflakeUDF(udfName, exprs, returnType.dataType, returnType.isOption)
  }
}
