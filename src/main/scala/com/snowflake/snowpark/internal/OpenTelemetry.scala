package com.snowflake.snowpark.internal

import com.snowflake.snowpark.UserDefinedFunction
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, StatusCode}

import scala.util.DynamicVariable

object OpenTelemetry extends Logging {

  private val udfInfo = new DynamicVariable[Option[UdfInfo]](None)

  def udf(
      className: String,
      funcName: String,
      execName: String,
      execHandler: String,
      execFilePath: String,
      stackOffset: Int)(func: => UserDefinedFunction): UserDefinedFunction = {
    try {
      udfInfo.withValue[UserDefinedFunction](udfInfo.value match {
        // empty info means this is the entry of the recursion
        case None =>
          val stacks = Thread.currentThread().getStackTrace
          val index = 4 + stackOffset
          val fileName = stacks(index).getFileName
          val lineNumber = stacks(index).getLineNumber
          Some(
            UdfInfo(
              className,
              funcName,
              fileName,
              lineNumber,
              execName,
              execHandler,
              execFilePath))
        // if value is not empty, this function call should be recursion.
        // do not issue new SpanInfo, use the info inherited from previous.
        case other => other
      }) {
        val result: UserDefinedFunction = func
        OpenTelemetry.emit(udfInfo.value.get)
        result
      }
    } catch {
      case error: Throwable =>
        OpenTelemetry.reportError(className, funcName, error)
        throw error
    }
  }

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

  def emit(udfInfo: UdfInfo): Unit =
    emit(udfInfo.className, udfInfo.funcName) { span =>
      {
        span.setAttribute("code.filepath", udfInfo.fileName)
        span.setAttribute("code.lineno", udfInfo.lineNumber)
        span.setAttribute("snow.executable.name", udfInfo.execName)
        span.setAttribute("snow.executable.handler", udfInfo.execHandler)
        span.setAttribute("snow.executable.filepath", udfInfo.execFilePath)
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

case class UdfInfo(
    className: String,
    funcName: String,
    fileName: String,
    lineNumber: Int,
    execName: String,
    execHandler: String,
    execFilePath: String)
