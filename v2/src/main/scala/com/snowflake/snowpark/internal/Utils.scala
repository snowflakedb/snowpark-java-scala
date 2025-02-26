package com.snowflake.snowpark.internal

import scala.collection.mutable.ArrayBuffer

object Utils {

  val Version: String = BuildInfo.version

  def getUserCodeMeta: String = {
    var lastInternalLine = "<unknown>"
    var internalCode = true
    val stackTrace = new ArrayBuffer[String]()
    val stackDepth = 3 // TODO: Configurable ?
    Thread.currentThread.getStackTrace.foreach { ste: StackTraceElement =>
      if (ste != null && ste.getMethodName != null
        && !ste.getMethodName.contains("getStackTrace")) {
        if (internalCode) {
          if (ste.getClassName.startsWith("net.snowflake.client.")
            || ste.getClassName.startsWith("com.snowflake.snowpark.")
            || ste.getClassName.startsWith("scala.")) {
            lastInternalLine = ste.getClassName + "." + ste.getMethodName
          } else {
            stackTrace += ste.toString.replaceAll("(\\$\\$iw)+", "\\$iw")
            // Update this, so that remaining stack trace elements are skipped.
            internalCode = false
          }
        }
      }
    }
    lastInternalLine + "\n" + stackTrace.take(stackDepth).mkString("\n")
  }
}
