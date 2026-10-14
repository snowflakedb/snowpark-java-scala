package com.snowflake.snowpark.internal.jdbc4

import java.io.InputStream
import java.lang.reflect.{InvocationTargetException, Method}
import java.sql.{
  Connection,
  PreparedStatement,
  ResultSet,
  ResultSetMetaData,
  SQLFeatureNotSupportedException,
  Statement
}
import java.util.Properties

import com.snowflake.snowpark.internal.jdbc.{
  FieldInfo,
  Jdbc4SupportApi,
  JdbcBridge,
  QueryState,
  StructuredResultReader
}
import com.snowflake.snowpark.internal.{ErrorMessage, Utils}
import net.snowflake.client.api.connection.{
  DownloadStreamConfig,
  SnowflakeConnection,
  UploadStreamConfig
}
import net.snowflake.client.api.resultset.{
  FieldMetadata,
  SnowflakeResultSet,
  SnowflakeResultSetMetaData
}
import net.snowflake.client.api.statement.{SnowflakePreparedStatement, SnowflakeStatement}
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl
import net.snowflake.client.internal.api.implementation.resultset.SnowflakeBaseResultSet
import net.snowflake.client.internal.core.arrow.StructObjectWrapper
import net.snowflake.client.internal.core.{
  ArrowSqlInput,
  ColumnTypeHelper,
  SFArrowResultSet,
  SFBaseResultSet
}
import net.snowflake.client.internal.jdbc.{
  DefaultSFConnectionHandler,
  SnowflakeConnectString,
  SnowflakeUtil
}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.internal.snowflake.common.core.LoginInfoDTO

import scala.collection.JavaConverters._

/**
 * Loaded reflectively by [[JdbcBridge]] when the connection is a JDBC 4.x connection, for example
 * the stored-procedure connection created by the exact-runtime JDBC adapter.
 */
object Jdbc4Support extends Jdbc4SupportApi {
  override def bridge(conn: Connection): JdbcBridge = new Jdbc4Bridge(conn)

  override def createClientConnection(connURL: String, connParam: Properties): Connection = {
    val connStr = SnowflakeConnectString.parse(connURL, connParam)
    if (!connStr.isValid) {
      throw ErrorMessage.MISC_INVALID_CONNECTION_STRING(s"$connStr")
    }
    // Identify the client type as Snowpark instead of JDBC.
    new SnowflakeConnectionImpl(new Jdbc4SnowparkConnectionHandler(connStr), connURL, connParam)
  }
}

private class Jdbc4SnowparkConnectionHandler(conStr: SnowflakeConnectString)
    extends DefaultSFConnectionHandler(conStr) {

  // Version format copied from GS ClientVersionUtils.java
  private val VersionFormatPattern = "[0-9]+\\.[0-9]+\\.[0-9]+(?:\\.[0-9]+)?".r

  override def initializeConnection(url: String, info: Properties): Unit = {
    val connStr = SnowflakeConnectString.parse(url, info)
    val version = VersionFormatPattern
      .findFirstIn(Utils.Version)
      .getOrElse(throw ErrorMessage.MISC_INVALID_CLIENT_VERSION(Utils.Version))
    super.initialize(connStr, LoginInfoDTO.SF_SNOWPARK_APP_ID, version)
  }
}

private class Jdbc4Bridge(connection: Connection) extends JdbcBridge {
  private val sfConn: SnowflakeConnection = connection.unwrap(classOf[SnowflakeConnection])

  override def sessionId: String = sfConn.getSessionID

  override def database: String = sfConn.getDatabase

  override def schema: String = connection.getSchema

  override def queryState(queryId: String): QueryState = {
    val status = sfConn.getQueryStatus(queryId)
    QueryState(status.isStillRunning, String.valueOf(status))
  }

  override def childQueryIds(queryId: String): Array[String] = sfConn.getChildQueryIds(queryId)

  override def sessionParameter(name: String): String =
    Jdbc4Bridge.getSessionParameterMethod match {
      case Some(m) =>
        try {
          Jdbc4Bridge.invoke(m, sfConn, name).asInstanceOf[String]
        } catch {
          case _: SQLFeatureNotSupportedException => null
        }
      case None => null
    }

  override def submitTelemetry(message: ObjectNode): Unit =
    Jdbc4Bridge.submitTelemetryMethod.foreach(Jdbc4Bridge.invoke(_, sfConn, message.toString))

  override def uploadStream(
      stageName: String,
      destPrefix: String,
      inputStream: InputStream,
      destFileName: String,
      compressData: Boolean): Unit =
    sfConn.uploadStream(
      stageName,
      destFileName,
      inputStream,
      UploadStreamConfig
        .builder()
        .setDestPrefix(destPrefix)
        .setCompressData(compressData)
        .build())

  override def downloadStream(
      stageName: String,
      sourceFileName: String,
      decompress: Boolean): InputStream =
    sfConn.downloadStream(
      stageName,
      sourceFileName,
      DownloadStreamConfig.builder().setDecompress(decompress).build())

  override def setStatementParameter(statement: Statement, name: String, value: Any): Unit =
    statement.asInstanceOf[SnowflakeStatement].setParameter(name, value)

  override def statementQueryId(statement: Statement): String =
    statement.asInstanceOf[SnowflakeStatement].getQueryID

  override def resultSetQueryId(resultSet: ResultSet): String =
    resultSet.asInstanceOf[SnowflakeResultSet].getQueryID

  override def executeAsyncQuery(statement: PreparedStatement): ResultSet =
    statement.asInstanceOf[SnowflakePreparedStatement].executeAsyncQuery()

  override def columnFields(meta: ResultSetMetaData, index: Int): List[FieldInfo] =
    Jdbc4Bridge.toFieldInfos(meta.asInstanceOf[SnowflakeResultSetMetaData].getColumnFields(index))

  override def structuredResult(resultSet: ResultSet): StructuredResultReader =
    new Jdbc4StructuredResultReader(resultSet)
}

private object Jdbc4Bridge {
  // TODO: Call these directly once Snowpark compiles against a JDBC release that includes
  // snowflake-jdbc#19 (4.4.1+). Older JDBC 4.x releases do not declare them.
  lazy val getSessionParameterMethod: Option[Method] = method("getSessionParameter")
  lazy val submitTelemetryMethod: Option[Method] = method("submitTelemetry")

  private def method(name: String): Option[Method] =
    try {
      Some(classOf[SnowflakeConnection].getMethod(name, classOf[String]))
    } catch {
      case _: NoSuchMethodException => None
    }

  def invoke(m: Method, target: AnyRef, arg: String): AnyRef =
    try {
      m.invoke(target, arg)
    } catch {
      case e: InvocationTargetException if e.getCause != null => throw e.getCause
    }

  def toFieldInfos(fields: java.util.List[FieldMetadata]): List[FieldInfo] =
    if (fields == null) {
      List.empty
    } else {
      fields.asScala.toList.map(toFieldInfo)
    }

  private def toFieldInfo(f: FieldMetadata): FieldInfo =
    FieldInfo(
      f.getName,
      f.getTypeName,
      f.getType,
      f.getPrecision,
      f.getScale,
      f.isNullable,
      toFieldInfos(f.getFields))
}

private class Jdbc4StructuredResultReader(data: ResultSet) extends StructuredResultReader {
  // the baseResultSet is not always SFArrowResultSet
  private lazy val baseResultSet: SFBaseResultSet = {
    val sfResultSet = data.asInstanceOf[SnowflakeBaseResultSet]
    val baseResultSetField =
      classOf[SnowflakeBaseResultSet].getDeclaredField("sfBaseResultSet")
    baseResultSetField.setAccessible(true)
    baseResultSetField.get(sfResultSet).asInstanceOf[SFBaseResultSet]
  }

  private lazy val arrowResultSet: SFArrowResultSet =
    baseResultSet.asInstanceOf[SFArrowResultSet]

  override def rawObject(index: Int): Any =
    SnowflakeUtil.mapSFExceptionToSQLException(() => baseResultSet.getObject(index))

  override def unwrapStructuredObject(value: Any): Option[Any] = value match {
    case w: StructObjectWrapper => Some(w.getObject)
    case _ => None
  }

  override def unwrapSqlInput(value: Any): Option[Any] = value match {
    case input: ArrowSqlInput => Some(input.getInput)
    case _ => None
  }

  override def toDate(value: Any): Any = arrowResultSet.convertToDate(value, null)

  override def toTime(value: Any, scale: Int): Any = arrowResultSet.convertToTime(value, scale)

  override def toTimestamp(value: Any, sqlType: Int, scale: Int): Any = {
    val columnType = ColumnTypeHelper.getColumnType(sqlType, arrowResultSet.getSession)
    arrowResultSet.convertToTimestamp(value, columnType, sqlType, null, scale)
  }
}
