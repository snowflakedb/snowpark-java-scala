package com.snowflake.snowpark.internal

import com.snowflake.snowpark.DataFrame
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, StatusCode}

object OpenTelemetry extends Logging {
  // class name format: snow.snowpark.<class name>
  // method chain: Dataframe.filter.join.select.collect
  def emit(
      className: String,
      funcName: String,
      fileName: String,
      lineNumber: Int,
      methodChain: String): Unit =
    emit(className, funcName) { span =>
      {
        span.setAttribute("code.filepath", fileName)
        span.setAttribute("code.lineno", lineNumber)
        span.setAttribute("method.chain", methodChain)
      }
    }

  def emit(spanInfo: SpanInfo): Unit =
    emit(
      spanInfo.className,
      spanInfo.funcName,
      spanInfo.fileName,
      spanInfo.lineNumber,
      spanInfo.methodChain)

  def reportError(className: String, funcName: String, error: Throwable): Unit =
    emit(className, funcName) { span =>
      {
        span.setStatus(StatusCode.ERROR, error.getMessage)
        span.recordException(error)
      }
    }

  private def emit(className: String, funcName: String)(report: Span => Unit): Unit = {
    val name = s"snow.snowpark.$className"
    val tracer = GlobalOpenTelemetry.getTracer(name)
    val span = tracer.spanBuilder(funcName).startSpan()
    try {
      val scope = span.makeCurrent()
      // Using Manager is not available in Scala 2.12 yet
      try {
        report(span)
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

case class SpanInfo(
    className: String,
    funcName: String,
    fileName: String,
    lineNumber: Int,
    methodChain: String)
