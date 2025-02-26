package com.snowflake.snowpark.internal

import net.snowflake.client.jdbc.SnowflakeConnectionV1

private[snowpark] object ServerConnection {

  // JDBC Connection
  private val defaultConnectionBuilder: (Map[String, String], Boolean) => ServerConnection =
    (configs, isScalaAPI) => new JDBCServerConnection(configs, isScalaAPI)

  // external connection builder, used by test and stored procedure
  private var connectionBuilder: Option[(Map[String, String], Boolean) => ServerConnection] = None

  /**
   * Replace default connection builder by the given one.
   * @param builder
   *   A server connection builder
   */
  def setBuilder(builder: (Map[String, String], Boolean) => ServerConnection): Unit = {
    connectionBuilder = Some(builder)
  }

  /**
   * Create a server connection.
   * @param configs
   *   A Map of configurations
   * @param isScalaAPI
   *   Whether the connection is used by a Scala Snowpark client or not
   * @return
   *   A Server Connection
   */
  def apply(configs: Map[String, String], isScalaAPI: Boolean): ServerConnection =
    connectionBuilder match {
      case Some(builder) => builder(configs, isScalaAPI)
      case _ => defaultConnectionBuilder(configs, isScalaAPI)
    }
}

private[snowpark] case class QueryResult()

trait ServerConnection {
  protected val configs: Map[String, String]
  val isScalaAPI: Boolean
  val isStoredProc: Boolean
  val connection: SnowflakeConnectionV1
  def close(): Unit
  def setQueryTag(queryTag: String): Unit
  def getQueryTag: Option[String]
  def unsetQueryTag(): Unit
  def isQueryTagSetInSession: Boolean

  protected val QueryTagName: String = "QUERY_TAG"

  /**
   * Generate a list of statement parameters. The result contains the parameters from context and
   * the given additional statement parameters.
   * @param isDdlOnTempObject
   *   Whether is a DDL on temporary object or not.
   * @param additionalParameters
   *   A list of additional statement parameters being set.
   * @return
   */
  def generateStatementParameters(
      isDdlOnTempObject: Boolean = false,
      additionalParameters: Map[String, Any] = Map.empty): Map[String, Any]

  /**
   * List all available java/scala packages on the Snowflake server.
   * @return
   *   A Set of package names
   */
  def listServerPackages: Set[String]

  /**
   * Run sql query and return the queryID when the caller doesn't need the result set.
   * @param query
   *   Sql text
   * @param isDdlOnTempObject
   *   Whether is a DDL on temporary object or not.
   * @param statementParameters
   *   A map of statement parameters.
   * @param params
   *   A list of bind variable.
   * @return
   */
  def runQuery(
      query: String,
      isDdlOnTempObject: Boolean = false,
      statementParameters: Map[String, Any] = Map.empty,
      params: Seq[Any] = Seq.empty): String

  /**
   * Execute the given query and fetch the query result.
   *
   * If the caller needs to get Iterator[Row], the internal JDBC ResultSet and Statement will NOT be
   * closed.
   *
   * If the caller needs to get Array[Row], the internal JDBC ResultSet and Statement will be
   * closed.
   *
   * If the caller doesn't need Array[Row] and Iterator[Row], the internal JDBC ResultSet and
   * Statement will be closed.
   *
   * @param query
   *   A Sql text.
   * @param returnRowArray
   *   Whether the result contains Array of Rows or not.
   * @param returnRowIterator
   *   Whether the result contains Iterator of Rows or not.
   * @param statementParameters
   *   A list of statement parameters.
   * @param params
   *   A list of SQL Bind variables
   * @return
   *   The query result.
   */
  def runQueryAndGetResult(
      query: String,
      returnRowArray: Boolean,
      returnRowIterator: Boolean,
      statementParameters: Map[String, Any],
      params: Seq[Any]): QueryResult

  /**
   * Fetch the value of the given session parameter. There are three ways to get a parameter and
   * this function will perform these in order:
   *   1. Try to read from JDBC.getOtherParameter 2. If no result and if skipActiveRead == false,
   *      try to issue a `show parameters like ...` to read the value 3. If skipActiveRead == true
   *      or the active read failed, try to return the provided default value
   *
   * @param parameterName
   *   The session parameter's name.
   * @param skipActiveRead
   *   Whether a SQL query to retrieve session parameter should be issued or not.
   * @param defaultValue
   *   Default value of parameter in case of the client can't read parameter from server.
   * @return
   *   The value of parameter.
   */
  def getParameterValue(
      parameterName: String,
      skipActiveRead: Boolean = false,
      defaultValue: Option[String] = None): String
}

/**
 * Connect to Snowflake server with Snowflake JDBC. It is the default implementation of server
 * connection.
 * @param configs
 *   The connection configurations
 * @param isScalaAPI
 *   Whether the connection is used by a Scala Snowpark client or not.
 */
class JDBCServerConnection(
    override protected val configs: Map[String, String],
    override val isScalaAPI: Boolean)
    extends ServerConnection {

  override val isStoredProc: Boolean = false

  override val connection: SnowflakeConnectionV1 = {
    null
  }

  override def close(): Unit = {
    if (connection != null) connection.close()
  }

  private var queryTag: Option[String] = None
  override def setQueryTag(queryTag: String): Unit = {
    runQuery(s"alter session set $QueryTagName = '$queryTag'")
    this.queryTag = Some(queryTag)
  }

  override def getQueryTag: Option[String] = queryTag

  override def unsetQueryTag(): Unit = {
    runQuery(s"alter session unset $QueryTagName")
    queryTag = None
  }

  override def isQueryTagSetInSession: Boolean =
    queryTag.isDefined | isQueryTagSet

  lazy private val isQueryTagSet: Boolean = {
    try {
      getParameterValue(QueryTagName).nonEmpty
    } catch {
      // Any error in reading QUERY_TAG session param should result
      // in snowpark skipping the logic to set QUERY_TAG
      case _: Exception => true
    }
  }

  override def generateStatementParameters(
      isDdlOnTempObject: Boolean,
      additionalParameters: Map[String, Any]): Map[String, Any] =
    // Only set queryTag if in client mode and if it is not already set
    (if (isStoredProc || isQueryTagSetInSession) Map()
     else Map(QueryTagName -> queryTag.getOrElse(Utils.getUserCodeMeta))) ++
      // Use SNOWPARK_SKIP_TXN_COMMIT_IN_DDL to avoid the DDL command to commit the open transaction
      (if (isDdlOnTempObject) Map("SNOWPARK_SKIP_TXN_COMMIT_IN_DDL" -> true)
       else Map()) ++ additionalParameters

  override def listServerPackages: Set[String] = Set.empty

  override def runQuery(
      query: String,
      isDdlOnTempObject: Boolean,
      statementParameters: Map[String, Any],
      params: Seq[Any]): String = { "" }

  override def getParameterValue(
      parameterName: String,
      skipActiveRead: Boolean,
      defaultValue: Option[String]): String = { "" }

  override def runQueryAndGetResult(
      query: String,
      returnRowArray: Boolean,
      returnRowIterator: Boolean,
      statementParameters: Map[String, Any],
      params: Seq[Any]): QueryResult = QueryResult()

}
