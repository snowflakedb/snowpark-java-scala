package com.snowflake.snowpark.internal

import com.snowflake.snowpark.SnowparkClientException
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.client.jdbc.telemetry.TelemetryUtil
import Telemetry._

final class Telemetry(conn: ServerConnection) extends Logging {

  // Get telemetry client from JDBC. Will return NoOpTelemetryClient for Java SP.
  private val telemetry = conn.connection.getSFBaseSession.getTelemetryClient

  private def send(telemetryType: String, data: JsonNode): Unit = {
    val msg = MAPPER.createObjectNode()
    msg.put(SOURCE, SNOWPARK)
    msg.put(TYPE, telemetryType)
    msg.set(DATA, data)
    msg.put(VERSION, Utils.Version)
    msg.put(SCALA_VERSION, Utils.ScalaVersion)
    msg.put(JAVA_VERSION, Utils.JavaVersion)
    msg.put(OS, Utils.OSName)
    if (conn.isScalaAPI) {
      msg.put(CLIENT_LANGUAGE, SCALA)
    } else {
      msg.put(CLIENT_LANGUAGE, JAVA)
    }
    try {
      telemetry.addLogToBatch(TelemetryUtil.buildJobData(msg))
      logDebug(s"sending telemetry data: ${data.toString}")
      telemetry.sendBatchAsync()
    } catch {
      case e: Exception =>
        logError(s"Failed to send telemetry data: ${data.toString}, Error: ${e.getMessage}")
    }
  }

  def reportSimplifierUsage(
      queryID: String,
      beforeSimplification: String,
      afterSimplification: String): Unit = {
    val msg = MAPPER.createObjectNode()
    msg.put(QUERY_ID, queryID)
    msg.put(BEFORE_SIMPLIFICATION, beforeSimplification)
    msg.put(AFTER_SIMPLIFICATION, afterSimplification)
    send(SIMPLIFIER_USAGE, msg)
  }

  def reportSessionCreated(): Unit = {
    val msg = MAPPER.createObjectNode()
    msg.put(START_TIME, System.currentTimeMillis())
    send(SESSION_CREATED, msg)
  }

  def reportErrorMessage(ex: Exception): Unit = {
    val msg = MAPPER.createObjectNode()
    ex match {
      case e: SnowparkClientException =>
        msg.put(ERROR_CODE, e.errorCode)
        msg.put(MESSAGE, Logging.maskSecrets(e.telemetryMessage))
        val stacktrace = ex.getStackTrace
          .map(_.toString.replace(e.message, e.telemetryMessage))
          .map(Logging.maskSecrets)
          .mkString("\n")
        msg.put(STACK_TRACE, stacktrace)
      case _ =>
        msg.put(ERROR_CODE, ErrorMessage.ERROR_CODE_UNKNOWN)
        msg.put(MESSAGE, Logging.maskSecrets(ex.getMessage))
        msg.put(
          STACK_TRACE,
          ex.getStackTrace.map(_.toString).map(Logging.maskSecrets).mkString("\n"))
    }
    send(ERROR, msg)
  }

  def reportUsageOfCopyPattern(): Unit =
    reportFunctionUsage(FunctionNames.COPY_PATTERN, FunctionCategory.COPY)

  def reportNameAliasInJoin(): Unit =
    reportFunctionUsage(FunctionNames.NAME_ALIAS_IN_JOIN, FunctionCategory.JOIN)

  def reportActionCacheResult(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_CACHE_RESULT, FunctionCategory.ACTION)

  def reportActionCollect(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_COLLECT, FunctionCategory.ACTION)

  def reportActionToLocalIterator(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_TO_LOCAL_ITERATOR, FunctionCategory.ACTION)

  def reportActionCount(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_COUNT, FunctionCategory.ACTION)

  def reportActionShow(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_SHOW, FunctionCategory.ACTION)

  def reportActionCreateOrReplaceView(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_CREATE_OR_REPLACE_VIEW, FunctionCategory.ACTION)

  def reportActionFirst(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_FIRST, FunctionCategory.ACTION)

  def reportActionRandomSplit(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_RANDOM_SPLIT, FunctionCategory.ACTION)

  def reportActionCopyInto(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_COPY_INTO, FunctionCategory.ACTION)

  def reportActionSaveAsTable(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_SAVE_AS_TABLE, FunctionCategory.ACTION)

  def reportActionSaveAsFile(fileType: String): Unit =
    reportFunctionUsage(
      FunctionNames.ACTION_SAVE_AS_FILE,
      FunctionCategory.ACTION,
      Map("file_type" -> fileType))

  def reportActionUpdate(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_UPDATE, FunctionCategory.ACTION)

  def reportActionDelete(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_DELETE, FunctionCategory.ACTION)

  def reportActionMerge(): Unit =
    reportFunctionUsage(FunctionNames.ACTION_MERGE, FunctionCategory.ACTION)

  def reportAddDependency(): Unit =
    reportFunctionUsage(FunctionNames.MISC_ADD_DEPENDENCY, FunctionCategory.DEPENDENCY)

  def reportGetDependency(): Unit =
    reportFunctionUsage(FunctionNames.MISC_GET_DEPENDENCY, FunctionCategory.DEPENDENCY)

  private def reportFunctionUsage(
      funcName: String,
      category: String,
      options: Map[String, String] = Map.empty): Unit = {
    val msg = MAPPER.createObjectNode()
    msg.put(NAME, funcName)
    msg.put(CATEGORY, category)
    options.foreach(x => msg.put(x._1, x._2))
    send(FUNCTION_USAGE, msg)
  }
}

object Telemetry {
  private val MAPPER: ObjectMapper = new ObjectMapper()

  // constants
  @inline private final val SOURCE: String = "source"
  @inline private final val SNOWPARK: String = "snowpark"
  @inline private final val TYPE: String = "type"
  @inline private final val DATA: String = "data"
  @inline private final val VERSION: String = "version"
  @inline private final val START_TIME: String = "start_time"
  @inline private final val SCALA_VERSION: String = "scala_version"
  @inline private final val JAVA_VERSION: String = "java_version"
  @inline private final val CLIENT_LANGUAGE: String = "client_language"
  @inline private final val JAVA: String = "java"
  @inline private final val SCALA: String = "scala"
  @inline private final val MESSAGE: String = "message"
  @inline private final val NAME: String = "name"
  @inline private final val ERROR_CODE: String = "error_code"
  @inline private final val STACK_TRACE: String = "stack_trace"
  @inline private final val OS: String = "operating_system"
  @inline private final val CATEGORY: String = "category"
  @inline private final val QUERY_ID: String = "query_id"
  @inline private final val BEFORE_SIMPLIFICATION: String = "before_simplification"
  @inline private final val AFTER_SIMPLIFICATION: String = "after_simplification"

  // message types, all message types should start with "snowpark_"
  @inline private final val SESSION_CREATED: String = "snowpark_session_created"
  @inline private final val FUNCTION_USAGE: String = "snowpark_function_usage"
  @inline private final val ERROR: String = "snowpark_error"
  @inline private final val SIMPLIFIER_USAGE: String = "snowpark_simplifier_usage"

  // function names

  private object FunctionNames {
    @inline final val COPY_PATTERN: String = "copy_pattern"
    @inline final val NAME_ALIAS_IN_JOIN: String = "name_alias_in_join"
    @inline final val ACTION_CACHE_RESULT: String = "action_cache_result"
    @inline final val ACTION_COLLECT: String = "action_collect"
    @inline final val ACTION_TO_LOCAL_ITERATOR: String = "action_to_local_iterator"
    @inline final val ACTION_COUNT: String = "action_count"
    @inline final val ACTION_SHOW: String = "action_show"
    @inline final val ACTION_CREATE_OR_REPLACE_VIEW: String = "action_create_or_replace_view"
    @inline final val ACTION_FIRST: String = "action_first"
    @inline final val ACTION_RANDOM_SPLIT: String = "action_random_split"
    @inline final val ACTION_COPY_INTO: String = "action_copy_into"
    @inline final val ACTION_SAVE_AS_TABLE: String = "action_save_as_table"
    @inline final val ACTION_SAVE_AS_FILE: String = "action_save_as_file"
    @inline final val ACTION_UPDATE: String = "action_update"
    @inline final val ACTION_DELETE: String = "action_delete"
    @inline final val ACTION_MERGE: String = "action_merge"
    @inline final val MISC_ADD_DEPENDENCY: String = "misc_add_dependency"
    @inline final val MISC_GET_DEPENDENCY: String = "misc_get_dependency"
  }

  private object FunctionCategory {
    @inline final val ACTION: String = "action"
    @inline final val JOIN: String = "join"
    @inline final val COPY: String = "copy"
    @inline final val DEPENDENCY: String = "dependency"
  }
}
