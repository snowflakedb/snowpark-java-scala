package com.snowflake.snowpark.internal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.internal.analyzer.{
  Attribute,
  LogicalPlan,
  TableFunctionExpression,
  singleQuote
}

import java.io.{File, FileInputStream}
import java.lang.invoke.SerializedLambda
import java.security.{DigestInputStream, MessageDigest}
import java.util.Locale
import com.snowflake.snowpark.udtf.UDTF
import net.snowflake.client.jdbc.SnowflakeSQLException

import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsScalaMapConverter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Utils extends Logging {
  val Version: String = BuildInfo.version
  // Package name of snowpark on server side
  val SnowparkPackageName = "com.snowflake:snowpark"
  val PackageNameDelimiter = ":"
  // Define the compat scala version instead of reading from property file
  // because it fails to read the property file in some environment such as
  // VSCode worksheet.
  val ScalaCompatVersion: String = BuildInfo.scalaVersion.split("\\.").take(2).mkString(".")
  val ScalaLibraryJarPath = System
    .getProperty("java.class.path")
    .split(File.pathSeparator)
    .find(_.contains("scala-library"))
    .getOrElse("")

  // Minimum GS version for us to identify as Snowpark client
  val MinimumGSVersionForSnowparkClientType: String = "5.20.0"

  // Define the toString format for java.sql.Date and java.sql.Timestamp
  // Need to define those types because SnowflakeDateTimeFormat does not have these as variable
  // Auto does not work as SqlFormat for class SnowflakeDateTimeFormat
  val DateInputFormat: String = "YYYY-MM-DD"
  val TimestampInputFormat: String = "YYYY-MM-DD HH24:MI:SS.FF"

  // Copied from GS parameter SP_TEMP_OBJECT_NAME_REGEX.
  // No need to read this parameter from server, because we cannot really change this parameter.
  // Otherwise older Snowpark clients will be broken.
  val TempObjectNamePattern: String =
    "^SNOWPARK_TEMP_(TABLE|VIEW|STAGE|FUNCTION|TABLE_FUNCTION|FILE_FORMAT|PROCEDURE)_[0-9A-Z]+$"

  val SnowflakePathPrefixes: List[String] = List("@", "snow://", "/")

  // Temp object name generation
  val randomGenerator = new Random(System.nanoTime())
  @inline private final val _TEMP_OBJECT_PREFIX: String = "SNOWPARK_TEMP"

  private[snowpark] def convertWindowsPathToLinux(path: String): String = {
    path.replace("\\", "/")
  }

  private[snowpark] def convertPathIfNecessary(path: String): String = {
    if (isWindows) {
      convertWindowsPathToLinux(path)
    } else {
      path
    }
  }

  lazy val OSName: String = System.getProperty("os.name")

  lazy private[snowpark] val isWindows: Boolean =
    OSName.toLowerCase(Locale.ENGLISH).contains("win")

  // Scala runtime version
  lazy val ScalaVersion: String = util.Properties.versionNumberString

  lazy val JavaVersion: String = System.getProperty("java.version")

  def isSnowparkJar(path: String): Boolean = {
    val fileName = path.split("/").last.toLowerCase(Locale.ROOT)
    fileName.startsWith("snowpark") && (fileName.endsWith(".jar") || fileName.endsWith(".jar.gz"))
  }

  def getUserCodeMeta(): String = {

    var lastInternalLine = "<unknown>"

    var internalCode = true
    val stackTrace = new ArrayBuffer[String]()
    val stackDepth = 3 // TODO: Configurable ?
    Thread.currentThread.getStackTrace().foreach { ste: StackTraceElement =>
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

  def addToDataframeAliasMap(
      result: Map[String, Seq[Attribute]],
      child: LogicalPlan): Map[String, Seq[Attribute]] = {
    if (child != null) {
      val map = child.dfAliasMap
      val duplicatedAlias = result.keySet.intersect(map.keySet)
      if (duplicatedAlias.nonEmpty) {
        throw ErrorMessage.DF_ALIAS_DUPLICATES(duplicatedAlias)
      }
      result ++ map
    } else {
      result
    }
  }

  def logTime[T](f: => T, funcDescription: String): T = {
    logInfo(funcDescription)
    val start = System.currentTimeMillis()
    val ret = f
    val end = System.currentTimeMillis()
    logInfo(s"Finished $funcDescription in ${end - start} ms")
    ret
  }

  def getContainingClass(closure: AnyRef): Option[Class[_]] = {
    try {
      closure match {
        case udtf: UDTF => Some(udtf.getClass)
        case _ =>
          val writeReplace = closure.getClass.getDeclaredMethod("writeReplace")
          writeReplace.setAccessible(true)
          val lambda = writeReplace.invoke(closure).asInstanceOf[SerializedLambda]
          val className = lambda.getCapturingClass.replaceAll("/", ".")
          Some(Class.forName(className))
      }
    } catch {
      // There could be a ClassNotFoundException in invoking writeReplace on closure
      // or in loading the closure class with name.
      case e: Exception => None
    }

  }

  def normalizeStageLocation(name: String): String = {
    val trimName = name.trim
    if (SnowflakePathPrefixes.exists(trimName.startsWith(_))) trimName else s"@$trimName"
  }

  private def isSingleQuoted(name: String): Boolean =
    name.startsWith("'") && name.endsWith("'")

  def normalizeLocalFile(file: String): String = {
    val trimFile = file.trim
    // For PUT/GET commands, if there are any special characters including spaces in
    // directory and file names, it needs to be quoted with single quote. For example,
    // 'file:///tmp/load data' for a path containing a directory named "load data").
    // So, if `file` is single quoted, it doesn't make sense to add "file://".
    if (trimFile.startsWith("file://") || isSingleQuoted(trimFile)) {
      trimFile
    } else {
      s"file://$trimFile"
    }
  }

  def calculateMD5(file: File): String = {
    val buffer = new Array[Byte](8192)
    val md5 = MessageDigest.getInstance("MD5")
    val dis = new DigestInputStream(new FileInputStream(file), md5)
    try { while (dis.read(buffer) != -1) {} }
    finally { dis.close() }
    md5.digest.map("%02x".format(_)).mkString
  }

  def checkUDFStage(normalized: String): String = {
    if (!normalized.endsWith("/")) {
      normalized + "/"
    } else {
      normalized
    }
  }

  def stageFilePrefixLength(stageLocation: String): Int = {
    val normalized = checkUDFStage(normalizeStageLocation(stageLocation))
    if (normalized.startsWith("@~")) {
      // Remove the first three characters from @~/...
      return normalized.length - 3
    }
    var res = 0
    var isQuoted: Boolean = false
    normalized.zipWithIndex.foreach {
      case ('\"', _) => isQuoted = !isQuoted
      case ('/', i) =>
        if (!isQuoted && res == 0) {
          // Find the first unquoted '/', then the stage name is before it, the path is after it
          val fullStageName = normalized.substring(0, i)
          val path = normalized.substring(i + 1)
          // The stage name can be either quoted or non-quoted
          val stageNamePattern = """%?[A-Za-z0-9_$]+|"([^"]|"")+"""".r
          // Find the last match, which should be the stage name.
          val stageName = stageNamePattern.findAllIn(fullStageName).toSeq.last
          // This is a table stage, stage name is not in the prefix
          if (stageName.startsWith("%")) {
            // Prefix is path
            res = path.length
          } else {
            // Prefix is stageName + "/" + path
            res = stageName.stripSuffix("\"").stripPrefix("\"").length + path.length + 1
          }
        }
      case (_, _) =>
    }
    res
  }

  /**
   * Parses a stage file location into stageName, path and fileName
   * @param stageLocation
   *   a string that represent a file on a stage
   * @return
   *   stageName, path and fileName
   */
  private[snowpark] def parseStageFileLocation(stageLocation: String): (String, String, String) = {
    val normalized = normalizeStageLocation(stageLocation)
    if (stageLocation.endsWith("/")) {
      throw ErrorMessage.MISC_INVALID_STAGE_LOCATION(
        stageLocation,
        "Stage file location must point to a file, not a folder")
    }

    var isQuoted: Boolean = false
    normalized.zipWithIndex.foreach {
      case ('\"', _) => isQuoted = !isQuoted
      case ('/', i) =>
        if (!isQuoted) {
          // Find the first unquoted '/', then the stage name is before it, the path is after it
          val fullStageName = normalized.substring(0, i)
          val pathAndFileName = normalized.substring(i + 1)
          if (pathAndFileName.isEmpty) {
            throw ErrorMessage.MISC_INVALID_STAGE_LOCATION(
              stageLocation,
              "Missing file name after the stage name")
          }
          val pathList = pathAndFileName.split("/")
          val path = pathList.take(pathList.size - 1).mkString("/")
          return (fullStageName, path, pathList.last)
        }
      case (_, _) =>
    }
    throw ErrorMessage.MISC_INVALID_STAGE_LOCATION(
      stageLocation,
      "Missing '/' to separate stage name and file name")
  }

  // Refactored as a wrapper for testing purpose
  private[snowpark] def checkScalaVersionCompatibility(): Unit = {
    if (!ScalaVersion.startsWith(ScalaCompatVersion)) {
      throw ErrorMessage.MISC_SCALA_VERSION_NOT_SUPPORTED(ScalaVersion, ScalaCompatVersion)
    }
  }

  // Valid name can be:
  //     identifier,
  //     identifier.identifier,
  //     identifier.identifier.identifier
  //     identifier..identifier
  private[snowpark] def validateObjectName(name: String): Unit = {
    val unQuotedIdPattern = """([a-zA-Z_][\w$]*)"""
    val quotedIdPattern = """("([^"]|"")+")"""
    val idPattern = s"($unQuotedIdPattern|$quotedIdPattern)"
    // scalastyle:off
    val pattern = s"^(($idPattern\\.){0,2}|($idPattern\\.\\.))$idPattern$$"
    // scalastyle:on
    if (!name.matches(pattern)) {
      throw ErrorMessage.MISC_INVALID_OBJECT_NAME(name)
    }
  }

  private[snowpark] def isValidJavaIdentifier(name: String): Boolean =
    name.length > 0 &&
      Character.isJavaIdentifierStart(name.head) &&
      name.drop(1).filter(!Character.isJavaIdentifierPart(_)).isEmpty

  private[snowpark] def getUDFUploadPrefix(udfName: String): String =
    if (udfName.matches("[\\w]+")) {
      udfName
    } else {
      udfName.replaceAll("\\W", "") + "_" + udfName.hashCode.abs
    }

  private[snowpark] def createConcurrentSet[T](): mutable.Set[T] = {
    import scala.collection.JavaConverters._
    java.util.Collections
      .newSetFromMap(new java.util.concurrent.ConcurrentHashMap[T, java.lang.Boolean])
      .asScala
  }

  private[snowpark] sealed trait TempObjectType {
    override def toString: String = this.getClass.getName.split("\\$").last.stripSuffix("$")
  }

  /**
   * Define types of temporary objects that will be created by Snowpark.
   */
  private[snowpark] object TempObjectType {
    case object Table extends TempObjectType
    case object Stage extends TempObjectType
    case object View extends TempObjectType
    case object Function extends TempObjectType
    case object Procedure extends TempObjectType
    case object FileFormat extends TempObjectType {
      override def toString: String = "File Format"
    }
  }

  private[snowpark] def randomNameForTempObject(tempObjectType: TempObjectType): String = {
    // Replace white space in the type name with `_`
    val typeStr = tempObjectType.toString.replaceAll("\\s", "_").toUpperCase()
    // Making random digits to 15 to match the length of session id. Also object names are case
    // insensitive, more random digits gives smaller chance of name collision.
    val randStr = randomGenerator.alphanumeric.take(15).mkString.toUpperCase()
    val name = s"${_TEMP_OBJECT_PREFIX}_${typeStr}_$randStr"

    assert(
      name.matches(TempObjectNamePattern),
      "Generated temp object name does not match the required pattern")
    name
  }

  private[snowpark] def escapePath(path: String): String =
    if (isWindows) { path.replace("\\", "\\\\") }
    else { path }

  private val RETRY_SLEEP_TIME_UNIT_IN_MS: Int = 1500
  private val MAX_SLEEP_TIME_IN_MS: Int = 60 * 1000
  // The first 10 sleep time in second will be like
  // 1.5, 3, 6, 12, 24, 48, 60, 60, 60, 60
  private[snowpark] def retrySleepTimeInMS(retry: Int): Int = {
    var expectedTime =
      RETRY_SLEEP_TIME_UNIT_IN_MS * Math.pow(2, retry).toInt
    // One sleep should be less than 3 minutes
    expectedTime = Math.min(expectedTime, MAX_SLEEP_TIME_IN_MS)
    // jitter factor is 0.5
    expectedTime = expectedTime / 2 + Random.nextInt(expectedTime / 2).abs
    expectedTime
  }

  private[snowpark] def isRetryable(t: Throwable): Boolean = t match {
    case e: SnowflakeSQLException
        if e.getMessage.contains("JDBC driver internal error") ||
          e.getMessage.contains("JDBC driver encountered IO error") =>
      true
    case _ => false
  }

  private[snowpark] def withRetry[T](maxRetry: Int, logPrefix: String)(thunk: => T): Unit = {
    var retry = 0
    var done = false
    var lastError: Option[Throwable] = None
    while (retry < maxRetry && !done) {
      try {
        thunk
        done = true
      } catch {
        case t: Throwable if isRetryable(t) =>
          logError(
            s"withRetry() failed: $logPrefix, sleep ${retrySleepTimeInMS(retry)} ms" +
              s" and retry: $retry error message: ${t.getMessage}")
          Thread.sleep(retrySleepTimeInMS(retry))
          lastError = Some(t)
          retry = retry + 1
        case t: Throwable =>
          logError(
            s"withRetry() failed: $logPrefix, but don't retry because it is not retryable," +
              s" error message: ${t.getMessage}")
          throw t
      }
    }
    if (!done && lastError.nonEmpty) {
      throw lastError.get
    }
  }

  private[snowpark] def isPutOrGetCommand(sql: String): Boolean =
    if (sql != null) {
      val adjustSql = sql.trim.toLowerCase(Locale.ENGLISH)
      adjustSql.startsWith("put") || adjustSql.startsWith("get")
    } else {
      false
    }

  private[snowpark] def isStringEmpty(str: String): Boolean = str == null || str.isEmpty

  private[snowpark] lazy val clientPackageName: String =
    SnowparkPackageName + PackageNameDelimiter + SnowparkSFConnectionHandler
      .extractValidVersionNumber(Utils.Version)

  /*
   * Snowflake SQL syntax for option values requires some value types to be singleQuoted:
   * https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html
   */
  private[snowpark] def quoteForOption(v: Any): String = {
    v match {
      case b: Boolean => b.toString
      case i: Int => i.toString
      case it: Integer => it.toString
      case s: String if s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false") => s
      case _ => singleQuote(v.toString)
    }
  }

  // rename the internal alias to its original name
  private[snowpark] def getDisplayColumnNames(
      attrs: Seq[Attribute],
      renamedColumns: Map[String, String]): Seq[Attribute] = {
    attrs.map(att =>
      renamedColumns
        .get(att.name)
        .map(newName => Attribute(newName, att.dataType, att.nullable, att.exprId))
        .getOrElse(att))
  }

  private[snowpark] def getTableFunctionExpression(col: Column): TableFunctionExpression = {
    col.expr match {
      case tf: TableFunctionExpression => tf
      case _ => throw ErrorMessage.DF_JOIN_WITH_WRONG_ARGUMENT()
    }
  }

  private val objectMapper = new ObjectMapper()

  private[snowpark] def jsonToMap(jsonString: String): Option[Map[String, Any]] = {
    try {
      val node = objectMapper.readTree(jsonString)
      assert(node.getNodeType == JsonNodeType.OBJECT)
      Some(jsonToScala(node).asInstanceOf[Map[String, Any]])
    } catch {
      case ex: Exception =>
        logError(ex.getMessage)
        None
    }
  }

  private def jsonToScala(node: JsonNode): Any = {
    node.getNodeType match {
      case JsonNodeType.STRING => node.asText()
      case JsonNodeType.NULL => null
      case JsonNodeType.OBJECT =>
        node
          .fields()
          .asScala
          .map(entry => {
            entry.getKey -> jsonToScala(entry.getValue)
          })
          .toMap
      case JsonNodeType.ARRAY =>
        node.elements().asScala.map(entry => jsonToScala(entry)).toSeq
      case JsonNodeType.BOOLEAN => node.asBoolean()
      case JsonNodeType.NUMBER => node.numberValue()
      case other =>
        throw new UnsupportedOperationException(s"Unsupported Type: ${other.name()}")
    }
  }

  private[snowpark] def mapToJson(map: Map[String, Any]): Option[String] = {
    try {
      Some(scalaToJson(map))
    } catch {
      case ex: Exception =>
        logError(ex.getMessage)
        None
    }
  }

  private def scalaToJson(input: Any): String =
    input match {
      case null => "null"
      case str: String => s""""$str""""
      case _: Int | _: Short | _: Long | _: Byte | _: Double | _: Float | _: Boolean =>
        input.toString
      case map: Map[String, _] =>
        map
          .map { case (key, value) =>
            s"${scalaToJson(key)}:${scalaToJson(value)}"
          }
          .mkString("{", ",", "}")
      case seq: Seq[_] => seq.map(scalaToJson).mkString("[", ",", "]")
      case arr: Array[_] => scalaToJson(arr.toSeq)
      case list: java.util.List[_] => scalaToJson(list.toArray)
      case map: java.util.Map[String, _] => scalaToJson(map.asScala.toMap)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported Type: ${input.getClass.getName}")
    }
}
