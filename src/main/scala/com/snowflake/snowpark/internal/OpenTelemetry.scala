package com.snowflake.snowpark.internal

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, StatusCode}

import scala.util.DynamicVariable

object OpenTelemetry extends Logging {

  // only report the top function info in case of recursion.
  private val actionInfo = new DynamicVariable[Option[ActionInfo]](None)

  // wrapper of all action functions
  def action[T](
      className: String,
      funcName: String,
      methodChain: String,
      isScala: Boolean,
      javaOffSet: Int = 0)(func: => T): T = {
    try {
      actionInfo.withValue[T](actionInfo.value match {
        // empty info means this is the entry of the recursion
        case None =>
          val stacks = Thread.currentThread().getStackTrace
          val index = if (isScala) 4 else 5 + javaOffSet
          val fileName = stacks(index).getFileName
          val lineNumber = stacks(index).getLineNumber
          Some(ActionInfo(className, funcName, fileName, lineNumber, s"$methodChain.$funcName"))
        // if value is not empty, this function call should be recursion.
        // do not issue new SpanInfo, use the info inherited from previous.
        case other => other
      }) {
        val result: T = func
        OpenTelemetry.emit(actionInfo.value.get)
        result
      }
    } catch {
      case error: Throwable =>
        OpenTelemetry.reportError(className, funcName, error)
        throw error
    }
  }
  // class name format: snow.snowpark.<class name>
  // method chain: Dataframe.filter.join.select.collect
  def emit(spanInfo: ActionInfo): Unit =
    emit(spanInfo.className, spanInfo.funcName) { span =>
      {
        span.setAttribute("code.filepath", spanInfo.fileName)
        span.setAttribute("code.lineno", spanInfo.lineNumber)
        span.setAttribute("method.chain", spanInfo.methodChain)
      }
    }

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

}

case class ActionInfo(
    className: String,
    funcName: String,
    fileName: String,
    lineNumber: Int,
    methodChain: String)
