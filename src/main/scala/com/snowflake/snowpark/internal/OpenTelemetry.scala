package com.snowflake.snowpark.internal

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, StatusCode}

import java.util.function.Supplier
import scala.util.DynamicVariable
import com.snowflake.snowpark_java.{
  UserDefinedFunction => JavaUDF,
  TableFunction => JavaTableFunction,
  StoredProcedure => JavaSProc
}

object OpenTelemetry extends Logging {

  private val spanInfo = new DynamicVariable[Option[SpanInfo]](None)

  // Java API
  def javaUDF(
      className: String,
      funcName: String,
      execName: String,
      execFilePath: String,
      func: Supplier[JavaUDF]): JavaUDF = {
    udx(
      className,
      funcName,
      execName,
      s"${UDXRegistrationHandler.className}.${UDXRegistrationHandler.methodName}",
      execFilePath)(func.get())
  }

  def javaUDTF(
      className: String,
      funcName: String,
      execName: String,
      execFilePath: String,
      func: Supplier[JavaTableFunction]): JavaTableFunction = {
    udx(className, funcName, execName, UDXRegistrationHandler.udtfClassName, execFilePath)(
      func.get())
  }
  def javaSProc(
      className: String,
      funcName: String,
      execName: String,
      execFilePath: String,
      func: Supplier[JavaSProc]): JavaSProc = {
    udx(
      className,
      funcName,
      execName,
      s"${UDXRegistrationHandler.className}.${UDXRegistrationHandler.methodName}",
      execFilePath)(func.get())
  }

  // Scala API
  def udx[T](
      className: String,
      funcName: String,
      execName: String,
      execHandler: String,
      execFilePath: String)(func: => T): T = {
    val stacks = Thread.currentThread().getStackTrace
    val (fileName, lineNumber) = findLineNumber(stacks)
    val newSpan =
      UdfInfo(className, funcName, fileName, lineNumber, execName, execHandler, execFilePath)
    emitSpan(newSpan, className, funcName, func)
  }
  // wrapper of all action functions
  def action[T](className: String, funcName: String, methodChain: String)(func: => T): T = {
    val stacks = Thread.currentThread().getStackTrace
    val (fileName, lineNumber) = findLineNumber(stacks)
    val newInfo =
      ActionInfo(className, funcName, fileName, lineNumber, s"$methodChain.$funcName")
    emitSpan(newInfo, className, funcName, func)
  }

  private def emitSpan[T](span: SpanInfo, className: String, funcName: String, thunk: => T): T = {
    try {
      spanInfo.value match {
        case None =>
          spanInfo.withValue(Some(span)) {
            val result: T = thunk
            // only emit one time, in the top level action
            OpenTelemetry.emit(spanInfo.value.get)
            result
          }
        case _ =>
          thunk
      }
    } catch {
      case error: Throwable =>
        OpenTelemetry.reportError(className, funcName, error)
        throw error
    }
  }

  private def findLineNumber(stacks: Array[StackTraceElement]): (String, Int) = {
    var index: Int = 0
    // start with OpenTelemetry class
    while (index < stacks.length && stacks(index).getFileName != "OpenTelemetry.scala") {
      index += 1
    }
    if (index == stacks.length) {
      // if can't find open telemetry class, make it N/A
      ("N/A", 0)
    } else {
      while (index < stacks.length &&
             (stacks(index).getClassName.startsWith("com.snowflake.snowpark.") ||
             stacks(index).getClassName.startsWith("com.snowflake.snowpark_java."))) {
        index += 1
      }
      if (index == stacks.length) {
        // all class inside of snowpark/snowpark-java package, make it N/A
        ("N/A", 0)
      } else {
        (stacks(index).getFileName, stacks(index).getLineNumber)
      }
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
