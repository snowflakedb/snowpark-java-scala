package com.snowflake.snowpark.internal

import com.snowflake.snowpark.DataFrame
import io.opentelemetry.api.GlobalOpenTelemetry

object OpenTelemetry extends Logging {
  def test(): Unit = {
    val stack = Thread.currentThread().getStackTrace
    // scalastyle:off println
    stack.foreach(e => {

      println(s"""
           |file name: ${e.getFileName}
           |line #: ${e.getLineNumber}
           |class name: ${e.getClassName}
           |method name: ${e.getMethodName}
           |""".stripMargin)
    })
    // scalastyle:on println
  }

  // class name format: snow.snowpark.<class name>
  // method chain: Dataframe.filter.join.select.collect
  // todo: track line number in SNOW-1480775
  def emit(
      className: String,
      funcName: String,
      fileName: String,
      lineNumber: Int,
      methodChain: String): Unit = {
    val name = s"snow.snowpark.$className"
    val tracer = GlobalOpenTelemetry.getTracer(name)
    val span = tracer.spanBuilder(funcName).startSpan()
    try {
      val scope = span.makeCurrent()
      // Using Manager is not available in Scala 2.12 yet
      try {
        span.setAttribute("code.filepath", fileName)
        span.setAttribute("code.lineno", lineNumber)
        span.setAttribute("method.chain", methodChain)
      } catch {
        case e: Exception =>
          logWarning(s"Error when acquiring span attributes. ${e.getMessage}")
      } finally {
        scope.close()
      }
    } finally {
      span.end()
    }
  }

  // todo: Snow-1480779
  def buildMethodChain(funcName: String, df: DataFrame): String = {
    ""
  }
}
