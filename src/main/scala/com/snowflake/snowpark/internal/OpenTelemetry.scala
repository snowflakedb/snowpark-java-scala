package com.snowflake.snowpark.internal

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, StatusCode}

import java.util.function.Supplier
import scala.util.DynamicVariable
import com.snowflake.snowpark_java.{UserDefinedFunction => JavaUDF, TableFunction => JavaTableFunction}

object OpenTelemetry extends Logging {

  private val spanInfo = new DynamicVariable[Option[SpanInfo]](None)

  // Java API
  def javaUDF(
      className: String,
      funcName: String,
      execName: String,
      execFilePath: String,
      stackOffset: Int,
      func: Supplier[JavaUDF]): JavaUDF = {
    udx(
      className,
      funcName,
      execName,
      s"${UDXRegistrationHandler.className}.${UDXRegistrationHandler.methodName}",
      execFilePath,
      stackOffset + 2)(func.get())
  }

  def javaUDTF(
               className: String,
               funcName: String,
               execName: String,
               execFilePath: String,
               stackOffset: Int,
               func: Supplier[JavaTableFunction]): JavaTableFunction = {
    udx(
      className,
      funcName,
      execName,
      s"${UDXRegistrationHandler.className}.${UDXRegistrationHandler.methodName}",
      execFilePath,
      stackOffset + 2)(func.get())
  }
  def JavaSProc(): Unit = {}

  // Scala API
  def udx[T](
      className: String,
      funcName: String,
      execName: String,
      execHandler: String,
      execFilePath: String,
      stackOffset: Int)(func: => T): T = {
    try {
      spanInfo.withValue[T](spanInfo.value match {
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
        val result: T = func
        OpenTelemetry.emit(spanInfo.value.get)
        result
      }
    } catch {
      case error: Throwable =>
        OpenTelemetry.reportError(className, funcName, error)
        throw error
    }
  }
  // wrapper of all action functions
  def action[T](
      className: String,
      funcName: String,
      methodChain: String,
      isScala: Boolean,
      javaOffSet: Int = 0)(func: => T): T = {
    try {
      spanInfo.withValue[T](spanInfo.value match {
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
        OpenTelemetry.emit(spanInfo.value.get)
        result
      }
    } catch {
      case error: Throwable =>
        OpenTelemetry.reportError(className, funcName, error)
        throw error
    }
  }

  def emit(info: SpanInfo): Unit =
    emit(info.className, info.funcName) { span =>
      {
        span.setAttribute("code.filepath", info.fileName)
        span.setAttribute("code.lineno", info.lineNumber)
        info match {
          case ActionInfo(_, _, _, _, methodChain) =>
            span.setAttribute("method.chain", methodChain)
          case UdfInfo(_, _, _, _, execName, execHandler, execFilePath) =>
            span.setAttribute("snow.executable.name", execName)
            span.setAttribute("snow.executable.handler", execHandler)
            span.setAttribute("snow.executable.filepath", execFilePath)
        }
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

trait SpanInfo {
  val className: String
  val funcName: String
  val fileName: String
  val lineNumber: Int
}

case class ActionInfo(
    override val className: String,
    override val funcName: String,
    override val fileName: String,
    override val lineNumber: Int,
    methodChain: String)
    extends SpanInfo

case class UdfInfo(
    override val className: String,
    override val funcName: String,
    override val fileName: String,
    override val lineNumber: Int,
    execName: String,
    execHandler: String,
    execFilePath: String)
    extends SpanInfo
