package com.snowflake.snowpark

import java.io.{File, FileInputStream, FileNotFoundException}
import java.net.URI
import java.sql.{Connection, Date, Time, Timestamp}
import java.util.{Properties, Map => JMap, Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, ForkJoinPool, ForkJoinWorkerThread}
import com.snowflake.snowpark.internal.analyzer._
import com.snowflake.snowpark.internal._
import com.snowflake.snowpark.internal.analyzer.{TableFunction => TFunction}
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.ErrorMessage.{
  UDF_CANNOT_ACCEPT_MANY_DF_COLS,
  UDF_UNEXPECTED_COLUMN_ORDER
}
import com.snowflake.snowpark.internal.ParameterUtils.ClosureCleanerMode
import com.snowflake.snowpark.internal.Utils.{
  TempObjectNamePattern,
  TempObjectType,
  getTableFunctionExpression,
  randomNameForTempObject
}
import net.snowflake.client.jdbc.{SnowflakeConnectionV1, SnowflakeDriver, SnowflakeSQLException}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

/**
 *
 * Establishes a connection with a Snowflake database and provides methods for creating DataFrames
 * and accessing objects for working with files in stages.
 *
 * When you create a {@code Session} object, you provide configuration settings to establish a
 * connection with a Snowflake database (e.g. the URL for the account, a user name, etc.). You can
 * specify these settings in a configuration file or in a Map that associates configuration
 * setting names with values.
 *
 * To create a Session from a file:
 * {{{
 *   val session = Session.builder.configFile("/path/to/file.properties").create
 * }}}
 *
 * To create a Session from a map of configuration properties:
 * {{{
 *   val configMap = Map(
 *   "URL" -> "demo.snowflakecomputing.com",
 *   "USER" -> "testUser",
 *   "PASSWORD" -> "******",
 *   "ROLE" -> "myrole",
 *   "WAREHOUSE" -> "warehouse1",
 *   "DB" -> "db1",
 *   "SCHEMA" -> "schema1"
 *   )
 *   Session.builder.configs(configMap).create
 * }}}
 *
 * Session contains functions to construct [[DataFrame]]s like
 * [[Session.table(name* Session.table]], [[Session.sql]], and [[Session.read]]
 * @since 0.1.0
 */
class Session private (private[snowpark] val conn: ServerConnection) extends Logging {
  private val STAGE_PREFIX = "@"
  // URI and file name with md5
  private val classpathURIs = new ConcurrentHashMap[URI, Option[String]]().asScala
  private val sessionStage = randomNameForTempObject(TempObjectType.Stage)
  private var stageCreated = false
  // For UDFs registered from within a stored proc, the snowpark jar is already in the imports
  // list, so we don't have to add it as a dependency
  private[snowpark] var snowparkJarInDeps = conn.isStoredProc

  // Server side package dependencies
  private[snowpark] val packageNames = ConcurrentHashMap.newKeySet[String]().asScala
  // If server side support this version of snowpark, we should skip upload snowpark jar and use
  // server package when registering UDFs
  private[snowpark] lazy val serverPackages: Set[String] = conn.listServerPackages()
  private[snowpark] lazy val isVersionSupportedByServerPackages: Boolean =
    serverPackages.contains(Utils.clientPackageName)

  private[snowpark] def sessionId: String = conn.getJDBCSessionID
  private[snowpark] lazy val sessionInfo: String =
    s"""{
       | "snowpark.version" : "${Utils.Version}",
       | "client.language": "${if (conn.isScalaAPI) "Scala" else "Java"}",
       | "java.version" : "${Utils.JavaVersion}",
       | "scala.version" : "${Utils.ScalaVersion}",
       | "jdbc.session.id" : "$sessionId",
       | "os.name" : "${Utils.OSName}",
       | "jdbc.version" : "${SnowflakeDriver.implementVersion}",
       | "snowpark.library" : "${Utils.escapePath(
         UDFClassPath.snowparkJar.location.getOrElse("snowpark library not found"))}",
       | "scala.library" : "${Utils.escapePath(
         UDFClassPath
           .getPathForClass(classOf[scala.Product])
           .getOrElse("Scala library not found"))}",
       | "jdbc.library" : "${Utils.escapePath(
         UDFClassPath
           .getPathForClass(classOf[net.snowflake.client.jdbc.SnowflakeDriver])
           .getOrElse("JDBC library not found"))}"
       |}""".stripMargin

  // report session created
  conn.telemetry.reportSessionCreated()

  private var lastActionID: Long = 0
  // all query has ID smaller than or equal to this should be canceled.
  private var lastCanceledID: Long = 0

  private val factory = new ForkJoinPool.ForkJoinWorkerThreadFactory() {
    override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
      val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
      worker.setName("snowpark-" + worker.getPoolIndex)
      worker
    }
  }

  private var threadPool =
    new ForkJoinPool(Runtime.getRuntime.availableProcessors, factory, null, true)

  private var executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(threadPool)

  private[snowpark] def getExecutionContext: ExecutionContext = synchronized {
    executionContext
  }

  private[snowpark] def generateNewActionID: Long = synchronized {
    lastActionID += 1
    lastActionID
  }

  private[snowflake] def getLastCanceledID: Long = synchronized {
    lastCanceledID
  }

  // for test use only
  private[snowpark] def getLastActionID: Long = synchronized {
    lastActionID
  }

  /**
   * Cancel all action methods that are running currently. This does not affect on any action
   * methods called in the future.
   *
   * @since 0.5.0
   */
  def cancelAll(): Unit = synchronized {
    logInfo("Canceling all running query")
    lastCanceledID = lastActionID
    threadPool.shutdownNow()
    threadPool = new ForkJoinPool(Runtime.getRuntime.availableProcessors, factory, null, true)
    executionContext = ExecutionContext.fromExecutor(threadPool)
    conn.runQuery(s"select system$$cancel_all_queries(${conn.getJDBCSessionID})")
  }

  /**
   * Returns the list of URLs for all the dependencies that were added for user-defined functions
   * (UDFs). This list includes any JAR files that were added automatically by the library.
   *
   * @return Set[URI]
   * @since 0.1.0
   */
  def getDependencies: collection.Set[URI] = {
    conn.telemetry.reportGetDependency()
    // make a clone of result, but not just return a pointer
    classpathURIs.keySet.filter(_ => true)
  }

  private[snowpark] def getLocalFileDependencies: collection.Set[URI] = {
    getDependencies.filterNot(_.getPath.startsWith(STAGE_PREFIX))
  }

  /**
   * Returns a Java Set of URLs for all the dependencies that were added for user-defined functions
   * (UDFs). This list includes any JAR files that were added automatically by the library.
   *
   * @since 0.2.0
   */
  def getDependenciesAsJavaSet: JSet[URI] = getDependencies.asJava

  private[snowpark] val plans: SnowflakePlanBuilder = new SnowflakePlanBuilder(this)

  private[snowpark] val analyzer: Analyzer = new Analyzer(this)

  private[snowpark] def explainQuery(query: String): Option[String] = {
    try {
      val rows = conn.runQueryGetRows(s"explain using text $query")
      require(rows.nonEmpty)
      // result has only one column and only one row.
      Option(rows.head.getString(0))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Returns the JDBC
   * [[https://docs.snowflake.com/en/user-guide/jdbc-api.html#object-connection Connection]]
   * object used for the connection to the Snowflake database.
   *
   * @return JDBC Connection object
   */
  def jdbcConnection: Connection = conn.connection

  /**
   * Registers a file in stage or a local file as a dependency of a user-defined function (UDF).
   *
   * The local file can be a JAR file, a directory, or any other file resource.
   * If you pass the path to a local file to {@code addDependency}, the Snowpark library uploads
   * the file to a temporary stage and imports the file when executing a UDF.
   *
   * If you pass the path to a file in a stage to {@code addDependency}, the file is included in
   * the imports when executing a UDF.
   *
   * Note that in most cases, you don't need to add the Snowpark JAR file and the JAR file (or
   * directory) of the currently running application as dependencies. The Snowpark library
   * automatically attempts to detect and upload these JAR files. However, if this automatic
   * detection fails, the Snowpark library reports this in an error message, and you must add these
   * JAR files explicitly by calling {@code addDependency}.
   *
   * The following example demonstrates how to add dependencies on local files and files in a stage:
   *
   * {{{
   *   session.addDependency("@my_stage/http-commons.jar")
   *   session.addDependency("/home/username/lib/language-detector.jar")
   *   session.addDependency("./resource-dir/")
   *   session.addDependency("./resource.xml")
   * }}}
   * @since 0.1.0
   * @param path Path to a local directory, local file, or file in a stage.
   */
  def addDependency(path: String): Unit = {
    val trimmedPath = path.trim
    if (trimmedPath.startsWith(STAGE_PREFIX)) {
      if (!snowparkJarInDeps && Utils.isSnowparkJar(trimmedPath)) {
        snowparkJarInDeps = true
      }
      classpathURIs.put(new URI(trimmedPath), None)
    } else {
      val file = new File(trimmedPath)
      if (!file.exists()) {
        throw new FileNotFoundException(s"File ${file.getAbsolutePath} not found")
      }
      if (file.isDirectory) {
        classpathURIs.put(file.toURI, None)
      } else {
        classpathURIs.put(file.toURI, Some(s"${Utils.calculateMD5(file)}/${file.getName}"))
      }
    }
    conn.telemetry.reportAddDependency()
  }

  /**
   * Removes a path from the set of dependencies.
   * @since 0.1.0
   * @param path Path to a local directory, local file, or file in a stage.
   */
  def removeDependency(path: String): Unit = {
    val trimmedPath = path.trim
    if (trimmedPath.startsWith(STAGE_PREFIX)) {
      classpathURIs.remove(new URI(trimmedPath))
    } else {
      classpathURIs.remove(new File(trimmedPath).toURI)
    }
  }

  /**
   * Adds a server side JVM package as a dependency of a user-defined function (UDF).
   * @param packageName Name of the package, formatted as `groupName:packageName:version`
   */
  private[snowpark] def addPackage(packageName: String): Unit = {
    packageNames.add(packageName.trim.toLowerCase())
  }

  /**
   * Removes a server side JVM package from the set of dependencies.
   * @param packageName Name of the package
   */
  private[snowpark] def removePackage(packageName: String): Unit = {
    packageNames.remove(packageName.trim.toLowerCase())
  }

  /**
   * List server supported JVM packages
   * @return Set of supported package names
   */
  private[snowpark] def listPackages(): Set[String] = serverPackages

  /**
   * Sets a query tag for this session. You can use the query tag to find all queries run for this
   * session.
   *
   * If not set, the default value of query tag is the Snowpark library call and the class and
   * method in your code that invoked the query (e.g.
   * `com.snowflake.snowpark.DataFrame.collect Main$.main(Main.scala:18)`).
   *
   * @param queryTag String to use as the query tag for this session.
   * @since 0.1.0
   */
  def setQueryTag(queryTag: String): Unit = synchronized {
    this.conn.setQueryTag(queryTag)
  }

  /**
   * Unset query_tag parameter for this session.
   *
   * If not set, the default value of query tag is the Snowpark library call and the class and
   * method in your code that invoked the query (e.g.
   * `com.snowflake.snowpark.DataFrame.collect Main$.main(Main.scala:18)`).
   *
   * @since 0.10.0
   */
  def unsetQueryTag(): Unit = synchronized {
    this.conn.unsetQueryTag()
  }

  /**
   * Returns the query tag that you set by calling [[setQueryTag]].
   * @since 0.1.0
   */
  def getQueryTag(): Option[String] = this.conn.getQueryTag()

  /**
   * Updates the query tag that is a JSON encoded string for the current session.
   *
   * Keep in mind that assigning a value via [[setQueryTag]] will remove any current query tag
   * state.
   *
   * Example 1:
   * {{{
   *   session.setQueryTag("""{"key1":"value1"}""")
   *   session.updateQueryTag("""{"key2":"value2"}""")
   *   print(session.getQueryTag().get)
   *   {"key1":"value1","key2":"value2"}
   * }}}
   *
   * Example 2:
   * {{{
   *   session.sql("""ALTER SESSION SET QUERY_TAG = '{"key1":"value1"}'""").collect()
   *   session.updateQueryTag("""{"key2":"value2"}""")
   *   print(session.getQueryTag().get)
   *   {"key1":"value1","key2":"value2"}
   * }}}
   *
   * Example 3:
   * {{{
   *   session.setQueryTag("")
   *   session.updateQueryTag("""{"key1":"value1"}""")
   *   print(session.getQueryTag().get)
   *   {"key1":"value1"}
   * }}}
   *
   * @param queryTag A JSON encoded string that provides updates to the current query tag.
   * @throws SnowparkClientException If the provided query tag or the query tag of the current
   *                                 session are not valid JSON strings; or if it could not
   *                                 serialize the query tag into a JSON string.
   * @since 1.13.0
   */
  def updateQueryTag(queryTag: String): Unit = synchronized {
    val newQueryTagMap = parseJsonString(queryTag)
    if (newQueryTagMap.isEmpty) {
      throw ErrorMessage.MISC_INVALID_INPUT_QUERY_TAG()
    }

    var currentQueryTag = this.conn.getParameterValue("query_tag")
    currentQueryTag = if (currentQueryTag.isEmpty) "{}" else currentQueryTag

    val currentQueryTagMap = parseJsonString(currentQueryTag)
    if (currentQueryTagMap.isEmpty) {
      throw ErrorMessage.MISC_INVALID_CURRENT_QUERY_TAG(currentQueryTag)
    }

    val updatedQueryTagMap = currentQueryTagMap.get ++ newQueryTagMap.get
    val updatedQueryTagStr = toJsonString(updatedQueryTagMap)
    if (updatedQueryTagStr.isEmpty) {
      throw ErrorMessage.MISC_FAILED_TO_SERIALIZE_QUERY_TAG()
    }

    setQueryTag(updatedQueryTagStr.get)
  }

  /**
   * Attempts to parse a JSON-encoded string into a [[scala.collection.immutable.Map]].
   *
   * @param jsonString The JSON-encoded string to parse.
   * @return An `Option` containing the `Map` if the parsing of the JSON string was
   *         successful, or `None` otherwise.
   */
  private def parseJsonString(jsonString: String): Option[Map[String, Any]] = {
    Utils.jsonToMap(jsonString)
  }

  /**
   * Attempts to convert a [[scala.collection.immutable.Map]] into a JSON-encoded string.
   *
   * @param map The `Map` to convert.
   * @return An `Option` containing the JSON-encoded string if the conversion was successful,
   *         or `None` otherwise.
   */
  private def toJsonString(map: Map[String, Any]): Option[String] = {
    Utils.mapToJson(map)
  }

  /*
   * Checks that the latest version of all jar dependencies is
   * uploaded to a stage and returns the staged URLs
   */
  private[snowpark] def resolveJarDependencies(stageLocation: String): Seq[Future[String]] = {
    val stageFileList = listFilesInStage(stageLocation)
    classpathURIs.flatMap {
      // local file
      case (uri, Some(fileName)) =>
        val file = new File(uri)
        Some(Future {
          if (stageFileList.contains(fileName)) {
            logInfo(s"$fileName exists on $stageLocation, skipped")
          } else {
            doUpload(file.toURI, stageLocation)
          }
          s"$stageLocation/$fileName"
        }(getExecutionContext))
      // stage file
      case (uri, None) =>
        if (uri.getPath.startsWith(STAGE_PREFIX)) {
          Some(Future {
            uri.getPath
          }(getExecutionContext))
        } else {
          None
        }
    }.toSeq
  }

  // Package private for tests
  private[snowpark] def doUpload(uri: URI, stageLocation: String): Unit = {
    Utils.logTime(
      {
        val (targetPrefix, targetFileName) = classpathURIs(uri) match {
          case Some(fileName) if fileName.lastIndexOf("/") > -1 =>
            val fileNameParts = fileName.split("/")
            (fileNameParts.dropRight(1).mkString("/"), fileNameParts.last)
          case _ => ("", classpathURIs(uri).get)
        }
        Utils
          .withRetry(
            maxFileUploadRetryCount,
            s"Uploading jar file $targetPrefix $targetFileName $stageLocation $uri") {
            val file = new File(uri)
            conn
              .uploadStream(
                stageLocation,
                targetPrefix,
                new FileInputStream(file),
                targetFileName,
                compressData = false)
          }
      },
      s"Uploading file ${uri.toString} to stage $stageLocation")

  }

  /**
   * the format of file name on stage is
   * stage/prefix/file
   *
   * stage: case insensitive, no quote
   * for example:
   * stage -> stage
   * STAGE -> stage
   * "stage" -> stage
   * "STAGE" -> stage
   * "sta/ge" -> sta/ge
   *
   * prefix: case sensitive
   * file: case sensitive
   *
   */
  private[snowpark] def listFilesInStage(stageLocation: String): Set[String] = {
    val normalized = Utils.normalizeStageLocation(stageLocation)

    // Leverage server to check whether the stage location is valid
    val fileList = sql(s"ls $normalized").select(""""name"""").collect()

    val prefixLength = Utils.stageFilePrefixLength(normalized)

    fileList.map(_.getString(0).substring(prefixLength)).toSet
  }

  /**
   * Returns an Updatable that points to the specified table.
   *
   * {@code name} can be a fully qualified identifier and must conform to the
   * rules for a Snowflake identifier.
   *
   * @param name Table name that is either a fully qualified name
   *             or a name in the current database/schema.
   * @return A [[Updatable]]
   * @since 0.1.0
   */
  def table(name: String): Updatable = {
    Utils.validateObjectName(name)
    Updatable(name, this)
  }

  /**
   * Returns an Updatable that points to the specified table.
   *
   * @param multipartIdentifier A sequence of strings that specify the database name, schema name,
   *                            and table name (e.g.
   *                            {@code Seq("database_name", "schema_name", "table_name")}).
   * @return A [[Updatable]]
   * @since 0.1.0
   */
  // [[<database name>.]<schema name>.]<table name>
  def table(multipartIdentifier: Seq[String]): Updatable =
    table(multipartIdentifier.mkString("."))

  /**
   * Returns an Updatable that points to the specified table.
   *
   * @param multipartIdentifier A list of strings that specify the database name, schema name,
   *                            and table name.
   * @return A [[Updatable]]
   * @since 0.2.0
   */
  def table(multipartIdentifier: java.util.List[String]): Updatable =
    table(multipartIdentifier.asScala)

  /**
   * Returns an Updatable that points to the specified table.
   *
   * @param multipartIdentifier An array of strings that specify the database name, schema name,
   *                            and table name.
   * @since 0.7.0
   */
  def table(multipartIdentifier: Array[String]): Updatable = {
    table(multipartIdentifier.mkString("."))
  }

  /**
   * Returns a dataframe with only columns that are in the result of df.join but not the original df
   *
   * @param df The source DataFrame on which the join operation was called
   * @param result The resulting Dataframe of the join operation
   */
  private def tableFunctionResultOnly(df: DataFrame, result: DataFrame): DataFrame = {
    // Check if the leading result columns are from the source df to confirm positions
    if (df.schema.indices.exists(i => result.schema(i).name != df.schema(i).name)) {
      throw UDF_UNEXPECTED_COLUMN_ORDER()
    }
    // Select columns that are in df.join but not the source DF using position
    // (to uniquely identify columns with duplicate names)
    val outputColumns = (df.schema.length until result.schema.length).map { i =>
      col(s"$$${i + 1}").as(result.schema(i).name)
    }
    result.select(outputColumns)
  }

  /**
   * Creates a new DataFrame from the given table function and arguments.
   *
   * Example
   * {{{
   *    import com.snowflake.snowpark.functions._
   *    import com.snowflake.snowpark.tableFunctions._
   *
   *    session.tableFunction(
   *      split_to_table,
   *      lit("split by space"),
   *      lit(" ")
   *    )
   * }}}
   *
   * @since 0.4.0
   * @param func Table function object, can be created from TableFunction class or
   *              referred from the built-in list from tableFunctions.
   * @param firstArg the first function argument of the given table function.
   * @param remaining all remaining function arguments.
   */
  def tableFunction(func: TableFunction, firstArg: Column, remaining: Column*): DataFrame =
    tableFunction(func, firstArg +: remaining)

  /**
   * Creates a new DataFrame from the given table function and arguments.
   *
   * Example
   * {{{
   *    import com.snowflake.snowpark.functions._
   *    import com.snowflake.snowpark.tableFunctions._
   *
   *    session.tableFunction(
   *      split_to_table,
   *      Seq(lit("split by space"), lit(" "))
   *    )
   *    // Since 1.8.0, DataFrame columns are accepted as table function arguments:
   *    df = Seq(Seq("split by space", " ")).toDF(Seq("a", "b"))
   *    session.tableFunction((
   *      split_to_table,
   *      Seq(df("a"), df("b"))
   *    )
   * }}}
   *
   * @since 0.4.0
   * @param func Table function object, can be created from TableFunction class or
   *             referred from the built-in list from tableFunctions.
   * @param args function arguments of the given table function.
   */
  def tableFunction(func: TableFunction, args: Seq[Column]): DataFrame = {
    // Use df.join to apply function result if args contains a DF column
    val sourceDFs = args.flatMap(_.expr.sourceDFs)
    if (sourceDFs.isEmpty) {
      // explode function requires a special handling since it is a client side function.
      if (func.funcName.trim.toLowerCase() == "explode") {
        callExplode(args.head)
      } else DataFrame(this, TableFunctionRelation(func.call(args: _*)))
    } else if (sourceDFs.toSet.size > 1) {
      throw UDF_CANNOT_ACCEPT_MANY_DF_COLS()
    } else {
      val df = sourceDFs.head
      val result = df.join(func, args)
      tableFunctionResultOnly(df, result)
    }
  }

  /**
   * Creates a new DataFrame from the given table function and arguments.
   *
   * Example
   * {{{
   *    import com.snowflake.snowpark.functions._
   *    import com.snowflake.snowpark.tableFunctions._
   *
   *    session.tableFunction(
   *      flatten,
   *      Map("input" -> parse_json(lit("[1,2]")))
   *    )
   *    // Since 1.8.0, DataFrame columns are accepted as table function arguments:
   *    df = Seq("[1,2]").toDF("a")
   *    session.tableFunction((
   *      flatten,
   *      Map("input" -> parse_json(df("a")))
   *    )
   * }}}
   *
   * @since 0.4.0
   * @param func Table function object, can be created from TableFunction class or
   *             referred from the built-in list from tableFunctions.
   * @param args function arguments map of the given table function.
   *              Some functions, like flatten, have named parameters.
   *              use this map to assign values to the corresponding parameters.
   */
  def tableFunction(func: TableFunction, args: Map[String, Column]): DataFrame = {
    // Use df.join to apply function result if args contains a DF column
    val sourceDFs = args.values.flatMap(_.expr.sourceDFs)
    if (sourceDFs.isEmpty) {
      DataFrame(this, TableFunctionRelation(func.call(args)))
    } else if (sourceDFs.toSet.size > 1) {
      throw UDF_CANNOT_ACCEPT_MANY_DF_COLS()
    } else {
      val df = sourceDFs.head
      val result = df.join(func, args)
      tableFunctionResultOnly(df, result)
    }
  }

  // process explode function with literal values
  private def callExplode(input: Column): DataFrame = {
    import this.implicits._
    // to reuse the DataFrame.join function, the input column has to be converted to
    // a DataFrame column. The best the solution is to create an empty dataframe and
    // then append this column via withColumn function. However, Snowpark doesn't support
    // empty DataFrame, therefore creating a dummy dataframe instead.
    val dummyDF = Seq(1).toDF("a")
    val sourceDF = dummyDF.withColumn("b", input)
    sourceDF.select(tableFunctions.explode(sourceDF("b")))
  }

  /**
   * Creates a new DataFrame from the given table function.
   *
   * Example
   * {{{
   *    import com.snowflake.snowpark.functions._
   *    import com.snowflake.snowpark.tableFunctions._
   *
   *    session.tableFunction(
   *      flatten(parse_json(lit("[1,2]")))
   *    )
   * }}}
   *
   * @since 1.10.0
   * @param func Table function object, can be created from TableFunction class or
   *             referred from the built-in list from tableFunctions.
   */
  def tableFunction(func: Column): DataFrame = {
    func.expr match {
      case TFunction(funcName, args) =>
        tableFunction(TableFunction(funcName), args.map(Column(_)))
      case NamedArgumentsTableFunction(funcName, argMap) =>
        tableFunction(TableFunction(funcName), argMap.map {
          case (key, value) => key -> Column(value)
        })
      case _ => throw ErrorMessage.MISC_INVALID_TABLE_FUNCTION_INPUT()
    }
  }

  private def createFromStoredProc(spName: String, args: Seq[Any]): DataFrame =
    DataFrame(this, StoredProcedureRelation(spName, args.map(functions.lit).map(_.expr)))

  /**
   * Creates a new DataFrame from the given Stored Procedure and arguments.
   *
   * {{{
   *   session.storedProcedure(
   *     "sp_name", "arg1", "arg2"
   *   ).show()
   * }}}
   * @since 1.8.0
   * @param spName The name of stored procedures.
   * @param args The arguments of the given stored procedure
   */
  def storedProcedure(spName: String, args: Any*): DataFrame = {
    Utils.validateObjectName(spName)
    createFromStoredProc(spName, args)
  }

  /**
   * Creates a new DataFrame from the given Stored Procedure and arguments.
   *
   * {{{
   *   val sp = session.sproc.register(...)
   *   session.storedProcedure(
   *     sp, "arg1", "arg2"
   *   ).show()
   * }}}
   * @since 1.8.0
   * @param sp The stored procedures object, can be created by `Session.sproc.register` methods.
   * @param args The arguments of the given stored procedure
   */
  def storedProcedure(sp: StoredProcedure, args: Any*): DataFrame =
    createFromStoredProc(sp.name.get, args)

  /**
   * Creates a new DataFrame containing the specified values. Currently, you can use values of the
   * following types:
   *
   *  - '''Base types (Int, Short, String etc.).''' The resulting DataFrame has the column name
   *    "VALUE".
   *  - '''Tuples consisting of base types.''' The resulting DataFrame has the column names "_1",
   *    "_2", etc.
   *  - '''Case classes consisting of base types.''' The resulting DataFrame has column names that
   *    correspond to the case class constituents.
   *
   * If you want to create a DataFrame by calling the {@code toDF} method of a {@code Seq} object,
   * import `session.implicits._`, where `session` is an object of the `Session` class that you
   * created to connect to the Snowflake database. For example:
   *
   * {{{
   *   val session = Session.builder.configFile(..).create
   *   // Importing this allows you to call the toDF method on a Seq object.
   *   import session.implicits._
   *   // Create a DataFrame from a Seq object.
   *   val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("numCol", "varcharCol")
   *   df.show()
   * }}}
   *
   * @param data A sequence in which each element represents a row of values in the DataFrame.
   * @tparam T DataType
   * @return A [[DataFrame]]
   * @since 0.1.0
   */
  def createDataFrame[T: TypeTag](data: Seq[T]): DataFrame = {
    val schema = TypeToSchemaConverter.inferSchema[T]()

    // convert Seq[T] to Seq[Row]
    val rows: Seq[Row] = data.map {
      case None => Row.fromSeq(Seq(null))
      // case class and tuple
      case product: Product =>
        val values: Seq[Any] = (0 until product.productArity).map(product.productElement)
        Row.fromSeq(values)
      case other => Row(other)
    }

    createDataFrame(rows, schema)
  }

  /**
   * Creates a new DataFrame that uses the specified schema and contains the specified [[Row]]
   * objects.
   *
   * For example, the following code creates a DataFrame containing three columns of the types
   * `int`, `string`, and `variant` with a single row of data:
   * {{{
   *   import com.snowflake.snowpark.types._
   *   ...
   *   // Create a sequence of a single Row object containing data.
   *   val data = Seq(Row(1, "a", new Variant(1)))
   *   // Define the schema for the columns in the DataFrame.
   *   val schema = StructType(Seq(StructField("int", IntegerType),
   *     StructField("string", StringType),
   *     StructField("variant", VariantType)))
   *   // Create the DataFrame.
   *   val df = session.createDataFrame(data, schema)
   * }}}
   *
   * @param data A sequence of [[Row]] objects representing rows of data.
   * @param schema [[types.StructType StructType]] representing the schema for the DataFrame.
   * @return A [[DataFrame]]
   * @since 0.2.0
   */
  def createDataFrame(data: Seq[Row], schema: StructType): DataFrame = {
    val spAttrs = schema.map { field =>
      {
        val sfType = field.dataType match {
          case _ @(VariantType | _: ArrayType | _: MapType | GeographyType | GeometryType |
              TimeType | DateType | TimestampType) =>
            StringType
          case other => other
        }
        Attribute(quoteName(field.name), sfType, field.nullable)
      }
    }

    val dataTypes = schema.map(_.dataType)

    // Strip options out of the input values
    val dataNoOption = data.map { row =>
      Row.fromSeq(row.toSeq.zip(dataTypes).map {
        case (None, _) => null
        case (Some(value), _) => value
        case (value, _) => value
      })
    }

    // convert all variant/time/geography/array/map data to string
    val converted = dataNoOption.map { row =>
      Row.fromSeq(row.toSeq.zip(dataTypes).map {
        case (null, _) => null
        case (value: BigDecimal, DecimalType(p, s)) => value
        case (value: Time, TimeType) => value.toString
        case (value: Date, DateType) => value.toString
        case (value: Timestamp, TimestampType) => value.toString
        case (value, _: AtomicType) => value
        case (value: Variant, VariantType) => value.asJsonString()
        case (value: Geography, GeographyType) => value.asGeoJSON()
        case (value: Geometry, GeometryType) => value.toString
        case (value: Array[_], _: ArrayType) =>
          new Variant(value.toSeq).asJsonString()
        case (value: Map[_, _], _: MapType) => new Variant(value).asJsonString()
        case (value: JMap[_, _], _: MapType) => new Variant(value).asJsonString()
        case (value, dataType) =>
          throw ErrorMessage
            .MISC_CANNOT_CAST_VALUE(value.getClass.getName, s"$value", dataType.toString)
      })
    }

    // construct a project statement to convert string value back to variant
    val projectColumns = schema.map { field =>
      field.dataType match {
        case DecimalType(precision, scale) =>
          to_decimal(column(field.name), precision, scale).as(field.name)
        case TimeType => callUDF("to_time", column(field.name)).as(field.name)
        case DateType => callUDF("to_date", column(field.name)).as(field.name)
        case TimestampType => callUDF("to_timestamp", column(field.name)).as(field.name)
        case VariantType => to_variant(parse_json(column(field.name))).as(field.name)
        case GeographyType => callUDF("to_geography", column(field.name)).as(field.name)
        case GeometryType => callUDF("to_geometry", column(field.name)).as(field.name)
        case _: ArrayType => to_array(parse_json(column(field.name))).as(field.name)
        case _: MapType => to_object(parse_json(column(field.name))).as(field.name)
        case _ => column(field.name)
      }
    }

    DataFrame(this, SnowflakeValues(spAttrs, converted)).select(projectColumns)
  }

  /**
   * Creates a new DataFrame that uses the specified schema and contains the specified [[Row]]
   * objects.
   *
   * For example, the following code creates a DataFrame containing two columns of the types
   * `int` and `string` with two rows of data:
   *
   * For example
   *
   * {{{
   *   import com.snowflake.snowpark.types._
   *   ...
   *   // Create an array of Row objects containing data.
   *   val data = Array(Row(1, "a"), Row(2, "b"))
   *   // Define the schema for the columns in the DataFrame.
   *   val schema = StructType(Seq(StructField("num", IntegerType),
   *     StructField("str", StringType)))
   *   // Create the DataFrame.
   *   val df = session.createDataFrame(data, schema)
   * }}}
   *
   * @param data An array of [[Row]] objects representing rows of data.
   * @param schema [[types.StructType StructType]] representing the schema for the DataFrame.
   * @return A [[DataFrame]]
   * @since 0.7.0
   */
  def createDataFrame(data: Array[Row], schema: StructType): DataFrame =
    createDataFrame(data.toSeq, schema)

  /**
   * Creates a new DataFrame from a range of numbers.
   * The resulting DataFrame has the column name "ID" and a row for each number in the sequence.
   *
   * @param start Start of the range.
   * @param end End of the range.
   * @param step Step function for producing the numbers in the range.
   * @return A [[DataFrame]]
   * @since 0.1.0
   */
  def range(start: Long, end: Long, step: Long): DataFrame =
    DataFrame(this, Range(start, end, step))

  /**
   * Creates a new DataFrame from a range of numbers starting from 0.
   * The resulting DataFrame has the column name "ID" and a row for each number in the sequence.
   *
   * @param end End of the range.
   * @return A [[DataFrame]]
   * @since 0.1.0
   */
  def range(end: Long): DataFrame = range(0, end)

  /**
   * Creates a new DataFrame from a range of numbers.
   * The resulting DataFrame has the column name "ID" and a row for each number in the sequence.
   *
   * @param start Start of the range.
   * @param end End of the range.
   * @return A [[DataFrame]]
   * @since 0.1.0
   */
  def range(start: Long, end: Long): DataFrame = range(start, end, 1)

  /**
   * Returns a new DataFrame representing the results of a SQL query.
   *
   * You can use this method to execute an arbitrary SQL statement.
   *
   * @param query The SQL statement to execute.
   * @return A [[DataFrame]]
   * @since 0.1.0
   */
  def sql(query: String): DataFrame = {
    sql(query, Seq.empty)
  }

  /**
   * Returns a new DataFrame representing the results of a SQL query.
   *
   * You can use this method to execute an arbitrary SQL statement.
   *
   * @param query The SQL statement to execute.
   * @param params for bind variables in SQL statement.
   * @return A [[DataFrame]]
   * @since 1.15.0
   */
  def sql(query: String, params: Seq[Any]): DataFrame = {
    // PUT and GET command cannot be executed in async mode
    DataFrame(this, plans.query(query, None, !Utils.isPutOrGetCommand(query), params))
  }

  /**
   * Creates a new DataFrame via Generator function.
   *
   * For example:
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   session.generator(10, Seq(seq4(), uniform(lit(1), lit(5), random()))).show()
   * }}}
   *
   * @param rowCount The row count of the result DataFrame.
   * @param columns the column list of the result DataFrame
   * @return A [[DataFrame]]
   * @since 0.11.0
   */
  def generator(rowCount: Long, columns: Seq[Column]): DataFrame =
    DataFrame(this, Generator(columns.map(_.expr), rowCount))

  /**
   * Creates a new DataFrame via Generator function.
   *
   * For example:
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   session.generator(10, seq4(), uniform(lit(1), lit(5), random())).show()
   * }}}
   *
   * @param rowCount The row count of the result DataFrame.
   * @param col the column of the result DataFrame
   * @param cols A list of columns excepts the first column
   * @return A [[DataFrame]]
   * @since 0.11.0
   */
  def generator(rowCount: Long, col: Column, cols: Column*): DataFrame =
    generator(rowCount, col +: cols)

  /**
   * Returns a [[DataFrameReader]] that you can use to read data from various supported sources
   * (e.g. a file in a stage) as a DataFrame.
   *
   * @return A [[DataFrameReader]]
   * @since 0.1.0
   */
  def read: DataFrameReader = new DataFrameReader(this)

  // Run the query directly but don't need to retrieve the result
  private[snowpark] def runQuery(sql: String, isDDLOnTempObject: Boolean = false): Unit =
    conn.runQuery(sql, isDDLOnTempObject)

  /**
   * Returns the name of the default database configured for this session in [[Session.builder]].
   *
   * @return The name of the default database
   * @since 0.1.0
   */
  def getDefaultDatabase: Option[String] = conn.getDefaultDatabase

  /**
   * Returns the name of the default schema configured for this session in [[Session.builder]].
   *
   * @return The name of the default schema
   * @since 0.1.0
   */
  def getDefaultSchema: Option[String] = conn.getDefaultSchema

  /**
   * Returns the name of the current database for the JDBC session attached to this session.
   *
   * For example, if you change the current database by executing the following code:
   *
   * {{{
   *   session.sql("use database newDB").collect()
   * }}}
   *
   * the method returns `newDB`.
   *
   * @return The name of the current database for this session.
   * @since 0.1.0
   */
  def getCurrentDatabase: Option[String] = conn.getCurrentDatabase

  /**
   * Returns the name of the current schema for the JDBC session attached to this session.
   *
   * For example, if you change the current schema by executing the following code:
   *
   * {{{
   *   session.sql("use schema newSchema").collect()
   * }}}
   *
   * the method returns `newSchema`.
   *
   * @return Current schema in session.
   * @since 0.1.0
   */
  def getCurrentSchema: Option[String] = conn.getCurrentSchema

  /**
   * Returns the fully qualified name of the current schema for the session.
   *
   * @return The fully qualified name of the schema
   * @since 0.2.0
   */
  def getFullyQualifiedCurrentSchema: String =
    conn.getCurrentDatabase.get + "." + conn.getCurrentSchema.get

  private[snowpark] def getResultAttributes(sql: String): Seq[Attribute] =
    conn.getResultAttributes(sql)

  /**
   * Returns the name of the temporary stage created by the Snowpark library for uploading and
   * store temporary artifacts for this session. These artifacts include classes for UDFs that you
   * define in this session and dependencies that you add when calling [[addDependency]].
   *
   * @return The name of stage.
   * @since 0.1.0
   */
  def getSessionStage: String = synchronized {
    val qualifiedStageName = s"$getFullyQualifiedCurrentSchema.$sessionStage"
    if (!stageCreated) {
      val tempType: TempType = getTempType(isTemp = true, isNameGenerated = true)
      this.runQuery(s"CREATE $tempType STAGE IF NOT EXISTS $qualifiedStageName", true)
      this.recordTempObjectIfNecessary(TempObjectType.Stage, qualifiedStageName, tempType)
      stageCreated = true
    }
    "@" + qualifiedStageName
  }

  /**
   * Returns a [[UDFRegistration]] object that you can use to register UDFs.
   * For example:
   * {{{
   *   session.udf.registerTemporary("mydoubleudf", (x: Int) => 2 * x)
   *   session.sql(s"SELECT mydoubleudf(c) FROM table")
   * }}}
   * @since 0.1.0
   */
  lazy val udf = new UDFRegistration(this)

  /**
   * Returns a [[UDTFRegistration]] object that you can use to register UDTFs.
   * For example:
   * {{{
   *   class MyWordSplitter extends UDTF1[String] {
   *     override def process(input: String): Iterable[Row] = input.split(" ").map(Row(_))
   *     override def endPartition(): Iterable[Row] = Array.empty[Row]
   *     override def outputSchema(): StructType = StructType(StructField("word", StringType))
   *   }
   *   val tableFunction = session.udtf.registerTemporary(new MyWordSplitter)
   *   session.tableFunction(tableFunction, lit("My name is Snow Park")).show()
   * }}}
   * @since 1.2.0
   */
  lazy val udtf: UDTFRegistration = new UDTFRegistration(this)

  /**
   * Returns a [[SProcRegistration]] object that you can use to register Stored Procedures.
   * For example:
   * {{{
   *   val sp = session.sproc.registerTemporary((session: Session, num: Int) => num * 2)
   *   session.storedProcedure(sp, 100).show()
   * }}}
   * @since 1.8.0
   */
  @PublicPreview
  lazy val sproc: SProcRegistration = new SProcRegistration(this)

  /**
   * Returns a [[FileOperation]] object that you can use to perform file operations on stages.
   * For example:
   * {{{
   *   session.file.put("file:///tmp/file1.csv", "@myStage/prefix1")
   *   session.file.get("@myStage/prefix1", "file:///tmp")
   * }}}
   *
   * @since 0.4.0
   */
  lazy val file = new FileOperation(this)

  /**
   * Provides implicit methods for convert Scala objects to Snowpark DataFrame and Column objects.
   *
   * To use this, import {@code session.implicits._}:
   * {{{
   *   val session = Session.builder.configFile(..).create
   *   import session.implicits._
   * }}}
   *
   * After you import this, you can call the {@code toDF} method of a {@code Seq} to convert a
   * sequence to a DataFrame:
   * {{{
   *   // Create a DataFrame from a local sequence of integers.
   *   val df = (1 to 10).toDF("a")
   *   val df = Seq((1, "one"), (2, "two")).toDF("a", "b")
   * }}}
   *
   * You can also refer to columns in DataFrames by using `$"colName"` and `'colName`:
   * {{{
   *   // Refer to a column in a DataFrame by using $"colName".
   *   val df = session.table("T").filter($"a" > 1)
   *   // Refer to columns by using 'colName.
   *   val df = session.table("T").filter('a > 1)
   * }}}
   * @since 0.1.0
   */
  // scalastyle:off
  object implicits extends Implicits with Serializable {
    protected override def _session: Session = Session.this
  }
  // scalastyle:on

  /**
   * Creates a new DataFrame by flattening compound values into multiple rows.
   *
   * For example:
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   val df = session.flatten(parse_json(lit("""{"a":[1,2]}""")))
   * }}}
   *
   * @param input The expression that will be unseated into rows.
   *              The expression must be of data type VARIANT, OBJECT, or ARRAY.
   * @return A [[DataFrame]].
   * @since 0.2.0
   */
  def flatten(input: Column): DataFrame =
    flatten(input, "", outer = false, recursive = false, "BOTH")

  /**
   * Creates a new DataFrame by flattening compound values into multiple rows.
   *
   * for example:
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   val df = session.flatten(parse_json(lit("""{"a":[1,2]}""")), "a", false, false, "BOTH")
   * }}}
   *
   * @param input The expression that will be unseated into rows.
   *              The expression must be of data type VARIANT, OBJECT, or ARRAY.
   * @param path The path to the element within a VARIANT data structure which
   *             needs to be flattened. Can be a zero-length string
   *             (i.e. empty path) if the outermost element is to be flattened.
   * @param outer If {@code false}, any input rows that cannot be expanded,
   *              either because they cannot be accessed in the path or because
   *              they have zero fields or entries, are completely omitted from
   *              the output. Otherwise, exactly one row is generated for
   *              zero-row expansions (with NULL in the KEY, INDEX, and VALUE columns).
   * @param recursive If {@code false}, only the element referenced by PATH is expanded.
   *                  Otherwise, the expansion is performed for all sub-elements
   *                  recursively.
   * @param mode Specifies which types should be flattened ({@code "OBJECT"}, {@code "ARRAY"}, or
   *             {@code "BOTH"}).
   * @since 0.2.0
   */
  def flatten(
      input: Column,
      path: String,
      outer: Boolean,
      recursive: Boolean,
      mode: String): DataFrame = {
    // scalastyle:off
    val flattenMode = mode.toUpperCase() match {
      case m @ ("OBJECT" | "ARRAY" | "BOTH") => m
      case m =>
        throw ErrorMessage.DF_FLATTEN_UNSUPPORTED_INPUT_MODE(m)
    }
    // scalastyle:on

    DataFrame(
      this,
      TableFunctionRelation(FlattenFunction(input.expr, path, outer, recursive, flattenMode)))
  }

  private[snowpark] val closureCleanerMode: ClosureCleanerMode.Value = conn.closureCleanerMode

  private[snowpark] def useScopedTempObjects: Boolean = conn.useScopedTempObjects

  private[snowpark] def getTempType(isTemp: Boolean, isNameGenerated: Boolean): TempType = {
    if (isTemp) {
      if (useScopedTempObjects && isNameGenerated) {
        TempType.ScopedTemporary
      } else {
        TempType.Temporary
      }
    } else {
      TempType.Permanent
    }
  }

  private[snowpark] def getTempType(isTemp: Boolean, name: String): TempType = {
    getTempType(isTemp, name.matches(TempObjectNamePattern))
  }

  private val _isLazyAnalysis: ThreadLocal[Boolean] = new ThreadLocal[Boolean]() {
    override protected def initialValue: Boolean = conn.isLazyAnalysis
  }
  private[snowpark] def isLazyAnalysis = _isLazyAnalysis.get()

  // Execute the code segment of 'chunk' with the specified analysis mode.
  private[snowpark] def withAnalysisMode[T](isLazyMode: Boolean)(thunk: => T): T = {
    val originalAnalysisMode = _isLazyAnalysis.get()
    try {
      _isLazyAnalysis.set(isLazyMode)
      thunk
    } finally {
      _isLazyAnalysis.set(originalAnalysisMode)
    }
  }

  private[snowpark] lazy val requestTimeoutInSeconds = conn.requestTimeoutInSeconds
  private[snowpark] lazy val maxFileUploadRetryCount = conn.maxFileUploadRetryCount
  private[snowpark] lazy val maxFileDownloadRetryCount = conn.maxFileDownloadRetryCount

  private val tempObjectsMap = new ConcurrentHashMap[String, TempObjectType]().asScala

  // Key collision only happens when user create temp udf or view that has name collision with
  // existing temp udf/view. In that case, the creation should fail unless user manually dropped
  // The previous temp object. We always store the latest mapping.
  private[snowpark] def recordTempObjectIfNecessary(
      tempObjectType: TempObjectType,
      name: String,
      tempType: TempType): Unit = {
    // We only need to track and drop session scoped temp objects
    if (tempType == TempType.Temporary) {
      // Make the name fully qualified by prepending database and schema to the name.
      var multipartIdentifier = name.split("\\.").toSeq
      if (multipartIdentifier.length == 1) {
        multipartIdentifier = getCurrentSchema.get +: multipartIdentifier
      }
      if (multipartIdentifier.length == 2) {
        multipartIdentifier = getCurrentDatabase.get +: multipartIdentifier
      }
      tempObjectsMap.put(multipartIdentifier.mkString("."), tempObjectType)
    }
  }

  /**
   * This api is for Stored Procedure internal usage only.
   * Do not call this api.
   */
  private[snowpark] def dropAllTempObjects(): Unit = {
    tempObjectsMap.foreach(v => {
      this.runQuery(s"drop ${v._2} if exists ${v._1}", true)
    })
  }

  // For test
  private[snowpark] def getTempObjectMap
    : scala.collection.concurrent.Map[String, TempObjectType] = tempObjectsMap

  /**
   * Close this session.
   *
   * @since 0.7.0
   */
  def close(): Unit = synchronized {
    // The users should not close a session used by stored procedure.
    if (conn.isStoredProc) {
      throw ErrorMessage.MISC_CANNOT_CLOSE_STORED_PROC_SESSION()
    }
    // If this session is registered as active session, unregister it.
    if (Session.activeSession.get() == this) {
      Session.activeSession.remove()
    }

    // Cancel all queries and close the connection.
    try {
      if (conn.isClosed) {
        logWarning("This session has been closed.")
      } else {
        logInfo("Closing session: " + sessionInfo)
        cancelAll()
      }
    } catch {
      case t: Throwable =>
        throw ErrorMessage.MISC_FAILED_CLOSE_SESSION(t.getMessage)
    } finally {
      conn.close()
    }
  }

  // Don't expose this function to end user because this method has false-negatives and
  // should be used with caution. It is used in COPY INTO workflow that can handle false-negatives
  // because this check precedes a create the table sql.
  private[snowpark] def tableExists(tableName: String): Boolean = {
    Utils.validateObjectName(tableName)
    try {
      conn.runQuery(s"DESCRIBE TABLE $tableName")
      true
    } catch {
      case _: SnowflakeSQLException => false
    }
  }

  /**
   * Get the session information.
   *
   * @since 0.11.0
   */
  def getSessionInfo(): String = sessionInfo

  /**
   * Returns an [[AsyncJob]] object that you can use to track the status and get the results of
   * the asynchronous query specified by the query ID.
   *
   * For example, create an AsyncJob by specifying a valid `<query_id>`, check whether
   * the query is running or not, and get the result rows.
   * {{{
   *   val asyncJob = session.createAsyncJob(<query_id>)
   *   println(s"Is query \${asyncJob.getQueryId} running? \${asyncJob.isRunning()}")
   *   val rows = asyncJob.getRows()
   * }}}
   *
   * @since 0.11.0
   * @param queryID A valid query ID
   * @return An [[AsyncJob]] object
   */
  def createAsyncJob(queryID: String): AsyncJob = new AsyncJob(queryID, this, None)

}

/**
 * Companion object to [[Session! Session]] that you use to build and create a session.
 * @since 0.1.0
 */
object Session extends Logging {
  Utils.checkScalaVersionCompatibility()

  // SNOW-150601
  // Some libraries use reflective acess that is deemed illegal in Java 9+
  // and throws a scary warning. Redirecting STDERR disables this warning.
  // A user can set DISABLE_REDIRECT_STDERR to not redirect stderr.
  if (!sys.env.get("DISABLE_REDIRECT_STDERR").isDefined) {
    disableStderr()
  }

  /**
   * This api is for Stored Procedure internal usage only.
   * Do not create a Session with this api.
   *
   * @return [[Session]]
   */
  private[snowpark] def apply(connection: SnowflakeConnectionV1): Session = {
    Session.builder.createInternal(Some(connection))
  }

  private[snowpark] def loadConfFromFile(configFile: String): Map[String, String] = {
    val prop = new Properties()
    var options: Map[String, String] = Map()
    prop.load(new FileInputStream(configFile))
    prop
      .entrySet()
      .forEach(entry => {
        options = options + (entry.getKey.toString -> entry.getValue.toString)
      })
    options
  }

  /**
   * Returns a builder you can use to set configuration properties and create a [[Session]] object.
   * @return [[SessionBuilder]]
   * @since 0.1.0
   */
  def builder: SessionBuilder = new SessionBuilder

  private val activeSession: InheritableThreadLocal[Session] =
    new InheritableThreadLocal[Session]()

  // In Stored Proc InheritableThreadLocal will drop the active session when passed to user.
  // So we need this global variable to store the active session for Stored Proc
  private var globalStoredProcSession: Option[Session] = None

  private def setActiveSession(session: Session): Session = {
    logInfo("Snowpark Session information: " + session.sessionInfo)
    if (!session.conn.isStoredProc) {
      if (activeSession.get() != null) {
        logInfo("Overwriting an already active session in this thread")
      }
      activeSession.set(session)
    } else {
      if (globalStoredProcSession.isDefined) {
        throw ErrorMessage.MISC_SP_ACTIVE_SESSION_RESET()
      }
      globalStoredProcSession = Some(session)
    }
    session
  }

  // test use only
  private[snowpark] def resetGlobalStoredProcSession(): Unit = {
    this.globalStoredProcSession = None
    logInfo(s"reset global stored proc session")
  }

  /**
   * Returns the active session in this thread, if any.
   *
   * @return [[Session]]
   * @since 0.1.0
   */
  private[snowpark] def getActiveSession: Option[Session] = {
    if (globalStoredProcSession.isDefined) {
      logInfo(s"global stored proc session is defined, returned it instead of the active session")
      globalStoredProcSession
    } else {
      Option(activeSession.get())
    }
  }

  private def disableStderr(): Unit = {
    logInfo("Closing stderr and redirecting to stdout")
    System.err.close()
    System.setErr(System.out)
    logInfo("Done closing stderr and redirecting to stdout")
  }

  /**
   * Provides methods to set configuration properties and create a [[Session]].
   * @since 0.1.0
   */
  class SessionBuilder {

    private var options: Map[String, String] = Map()

    private var isScalaAPI: Boolean = true
    private var appName: Option[String] = None

    // used by Java API only
    private[snowpark] def setJavaAPI(): SessionBuilder = {
      this.isScalaAPI = false
      this
    }

    // Used only for tests
    private[snowpark] def removeConfig(key: String): SessionBuilder = {
      options -= key
      this
    }

    /**
     * Adds the app name to set in the query_tag after session creation.
     *
     * Since version 1.13.0, the app name is set to the query tag in JSON format. For example:
     * {{{
     *   val session = Session.builder.appName("myApp").configFile(myConfigFile).create
     *   print(session.getQueryTag().get)
     *   {"APPNAME":"myApp"}
     * }}}
     *
     * In previous versions it is set using a key=value format. For example:
     * {{{
     *   val session = Session.builder.appName("myApp").configFile(myConfigFile).create
     *   print(session.getQueryTag().get)
     *   APPNAME=myApp
     * }}}
     *
     * @param appName Name of the app.
     * @return A [[SessionBuilder]]
     * @since 1.12.0
     */
    def appName(appName: String): SessionBuilder = {
      this.appName = Some(appName)
      this
    }

    /**
     * Adds the configuration properties in the specified file to the SessionBuilder configuration.
     *
     * @param file Path to the file containing the configuration properties.
     * @return A [[SessionBuilder]]
     * @since 0.1.0
     */
    def configFile(file: String): SessionBuilder = {
      configs(loadConfFromFile(file))
    }

    /**
     * Adds the specified configuration property and value to the SessionBuilder configuration.
     *
     * @param key Name of the configuration property.
     * @param value Value of the configuration property.
     * @return A [[SessionBuilder]]
     * @since 0.1.0
     */
    def config(key: String, value: String): SessionBuilder = synchronized {
      options = options + (key -> value)
      this
    }

    /**
     * Adds the specified {@code Map} of configuration properties to the SessionBuilder
     * configuration.
     *
     * Note that calling this method overwrites any existing configuration properties that you have
     * already set in the SessionBuilder.
     *
     * @param configs Map of the names and values of configuration properties.
     * @return A [[SessionBuilder]]
     * @since 0.1.0
     */
    def configs(configs: Map[String, String]): SessionBuilder = synchronized {
      options = options ++ configs
      this
    }

    /**
     * Adds the specified Java {@code Map} of configuration properties to the SessionBuilder
     * configuration.
     *
     * Note that calling this method overwrites any existing configuration properties that you have
     * already set in the SessionBuilder.
     *
     * @since 0.2.0
     */
    def configs(javaMap: java.util.Map[String, String]): SessionBuilder = {
      configs(javaMap.asScala.toMap)
    }

    /**
     * Creates a new Session.
     *
     * @return A [[Session]]
     * @since 0.1.0
     */
    def create: Session = {
      val session = createInternal(None)
      val appName = this.appName
      if (appName.isDefined) {
        val appNameTag = s"""{"APPNAME":"${appName.get}"}"""
        session.updateQueryTag(appNameTag)
      }
      session
    }

    /**
     * Returns the existing session if already exists or create it if not.
     *
     * @return A [[Session]]
     * @since 1.10.0
     */
    def getOrCreate: Session = {
      Session.getActiveSession.getOrElse(create)
    }

    private[snowpark] def createInternal(conn: Option[SnowflakeConnectionV1]): Session = {
      conn match {
        case Some(_) =>
          setActiveSession(new Session(new ServerConnection(Map.empty, isScalaAPI, conn)))
        case None => setActiveSession(new Session(ServerConnection(options, isScalaAPI)))
      }
    }
  }
}
