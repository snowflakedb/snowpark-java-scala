package com.snowflake.snowpark.internal.jdbc

import java.io.InputStream
import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties

import com.snowflake.snowpark.internal.{ErrorMessage, SnowparkSFConnectionHandler}
import net.snowflake.client.core.arrow.StructObjectWrapper
import net.snowflake.client.core.{
  ArrowSqlInput,
  ColumnTypeHelper,
  QueryStatus,
  SFArrowResultSet,
  SFBaseResultSet
}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.telemetry.TelemetryUtil
import net.snowflake.client.jdbc.{
  FieldMetadata,
  SnowflakeBaseResultSet,
  SnowflakeConnectString,
  SnowflakeConnectionV1,
  SnowflakePreparedStatement,
  SnowflakeResultSet,
  SnowflakeResultSetMetaData,
  SnowflakeStatement,
  SnowflakeUtil
}

import scala.collection.JavaConverters._

/**
 * [[JdbcBridge]] for JDBC 3.x (`SnowflakeConnectionV1`). Only loaded when JDBC 3.x is on the
 * classpath: the default client dependency and the legacy stored-procedure runtime.
 */
private[snowpark] class Jdbc3Bridge private (connection: SnowflakeConnectionV1) extends JdbcBridge {

  private lazy val telemetryClient = connection.getSFBaseSession.getTelemetryClient

  override def sessionId: String = connection.getSessionID

  override def database: String = connection.getSFBaseSession.getDatabase

  override def schema: String = connection.getSFBaseSession.getSchema

  override def queryState(queryId: String): QueryState = {
    val status = connection.getSFBaseSession.getQueryStatus(queryId)
    QueryState(QueryStatus.isStillRunning(status), String.valueOf(status))
  }

  override def childQueryIds(queryId: String): Array[String] =
    connection.getChildQueryIds(queryId)

  override def sessionParameter(name: String): String = {
    val param = connection.getSFBaseSession.getOtherParameter(name)
    if (param == null) null else param.toString
  }

  override def submitTelemetry(message: ObjectNode): Unit = {
    telemetryClient.addLogToBatch(TelemetryUtil.buildJobData(message))
    telemetryClient.sendBatchAsync()
  }

  override def uploadStream(
      stageName: String,
      destPrefix: String,
      inputStream: InputStream,
      destFileName: String,
      compressData: Boolean): Unit =
    connection.uploadStream(stageName, destPrefix, inputStream, destFileName, compressData)

  override def downloadStream(
      stageName: String,
      sourceFileName: String,
      decompress: Boolean): InputStream =
    connection.downloadStream(stageName, sourceFileName, decompress)

  override def setStatementParameter(statement: Statement, name: String, value: Any): Unit =
    statement.asInstanceOf[SnowflakeStatement].setParameter(name, value)

  override def statementQueryId(statement: Statement): String =
    statement.asInstanceOf[SnowflakeStatement].getQueryID

  override def resultSetQueryId(resultSet: ResultSet): String =
    resultSet.asInstanceOf[SnowflakeResultSet].getQueryID

  override def executeAsyncQuery(statement: PreparedStatement): ResultSet =
    statement.asInstanceOf[SnowflakePreparedStatement].executeAsyncQuery()

  override def columnFields(meta: ResultSetMetaData, index: Int): List[FieldInfo] =
    Jdbc3Bridge.toFieldInfos(meta.asInstanceOf[SnowflakeResultSetMetaData].getColumnFields(index))

  override def structuredResult(resultSet: ResultSet): StructuredResultReader =
    new Jdbc3StructuredResultReader(resultSet)
}

private[snowpark] object Jdbc3Bridge {
  def apply(conn: Connection): Jdbc3Bridge =
    new Jdbc3Bridge(conn.unwrap(classOf[SnowflakeConnectionV1]))

  def createClientConnection(connURL: String, connParam: Properties): Connection = {
    val connStr = SnowflakeConnectString.parse(connURL, connParam)
    if (!connStr.isValid) {
      throw ErrorMessage.MISC_INVALID_CONNECTION_STRING(s"$connStr")
    }
    // Identify the client type as Snowpark instead of JDBC.
    new SnowflakeConnectionV1(new SnowparkSFConnectionHandler(connStr), connURL, connParam)
  }

  private[jdbc] def toFieldInfos(fields: java.util.List[FieldMetadata]): List[FieldInfo] =
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

private class Jdbc3StructuredResultReader(data: ResultSet) extends StructuredResultReader {
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
