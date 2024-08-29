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
      execFilePath: String)(thunk: => T): T = {
    val stacks = Thread.currentThread().getStackTrace
    val (fileName, lineNumber) = findLineNumber(stacks)
    val newSpan =
      UdfInfo(className, funcName, fileName, lineNumber, execName, execHandler, execFilePath)
    emitSpan(newSpan, thunk)
  }
  // wrapper of all action functions
  def action[T](className: String, funcName: String, methodChain: String)(thunk: => T): T = {
    val stacks = Thread.currentThread().getStackTrace
    val (fileName, lineNumber) = findLineNumber(stacks)
    val newInfo =
      ActionInfo(className, funcName, fileName, lineNumber, s"$methodChain.$funcName")
    emitSpan(newInfo, thunk)
  }

  private def emitSpan[T](span: SpanInfo, thunk: => T): T = {
    spanInfo.value match {
      case None =>
        spanInfo.withValue(Some(span)) {
          span.emit(thunk)
        }
      case _ =>
        thunk
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
}
trait SpanInfo {
  val className: String
  val funcName: String
  val fileName: String
  val lineNumber: Int

  lazy private val span =
    GlobalOpenTelemetry
      .getTracer(s"snow.snowpark.$className")
      .spanBuilder(funcName)
      .startSpan()

  def emit[T](thunk: => T): T = {
    val scope = span.makeCurrent()
    // Using Manager is not available in Scala 2.12 yet
    try {
      span.setAttribute("code.filepath", fileName)
      span.setAttribute("code.lineno", lineNumber)
      addAdditionalInfo(span)
      thunk
    } catch {
      case error: Exception =>
        OpenTelemetry.logWarning(s"Error when acquiring span attributes. ${error.getMessage}")
        span.setStatus(StatusCode.ERROR, error.getMessage)
        span.recordException(error)
        throw error
    } finally {
      scope.close()
      span.end()
    }
  }

  protected def addAdditionalInfo(span: Span): Unit
}

case class ActionInfo(
    override val className: String,
    override val funcName: String,
    override val fileName: String,
    override val lineNumber: Int,
    methodChain: String)
    extends SpanInfo {

  override protected def addAdditionalInfo(span: Span): Unit = {
    span.setAttribute("method.chain", methodChain)
  }
}

case class UdfInfo(
    override val className: String,
    override val funcName: String,
    override val fileName: String,
    override val lineNumber: Int,
    execName: String,
    execHandler: String,
    execFilePath: String)
    extends SpanInfo {

  override protected def addAdditionalInfo(span: Span): Unit = {
    span.setAttribute("snow.executable.name", execName)
    span.setAttribute("snow.executable.handler", execHandler)
    span.setAttribute("snow.executable.filepath", execFilePath)
  }
}
