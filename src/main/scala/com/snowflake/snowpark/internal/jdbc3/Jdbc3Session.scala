package com.snowflake.snowpark.internal.jdbc3

import java.io.InputStream
import java.sql.{
  Connection,
  PreparedStatement,
  ResultSet,
  ResultSetMetaData,
  SQLException,
  Statement
}
import java.util.{Properties, List => JList}

import com.snowflake.session.api.{FieldMetadata, QueryStatus, SnowflakeSession}
import com.snowflake.snowpark.internal.ErrorMessage
import net.snowflake.client.core.arrow.StructObjectWrapper
import net.snowflake.client.core.{
  ArrowSqlInput,
  ColumnTypeHelper,
  SFArrowResultSet,
  SFBaseResultSet,
  SFException,
  QueryStatus => SFQueryStatus
}
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.{
  JsonStringArrayList,
  JsonStringHashMap
}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
import net.snowflake.client.jdbc.telemetry.TelemetryUtil
import net.snowflake.client.jdbc.{
  SnowflakeBaseResultSet,
  SnowflakeConnectString,
  SnowflakeConnectionV1,
  SnowflakePreparedStatement,
  SnowflakeReauthenticationRequest,
  SnowflakeResultSet,
  SnowflakeResultSetMetaData,
  SnowflakeSQLException,
  SnowflakeStatement,
  SnowflakeUtil,
  FieldMetadata => SFFieldMetadata
}

import scala.collection.JavaConverters._

/**
 * [[SnowflakeSession]] on a JDBC 3.x connection (`SnowflakeConnectionV1`): JDBC 3.x clients, and
 * the legacy stored-procedure runtime.
 */
private[snowpark] class Jdbc3Session private (
    connection: SnowflakeConnectionV1,
    storedProcedure: Boolean)
    extends SnowflakeSession {

  private lazy val telemetryClient = connection.getSFBaseSession.getTelemetryClient

  override def getConnection: Connection = connection

  override def isStoredProcedure: Boolean = storedProcedure

  override def getSessionId: String = connection.getSessionID

  override def getDatabase: String = connection.getSFBaseSession.getDatabase

  override def getSchema: String = connection.getSFBaseSession.getSchema

  override def getSessionParameter(name: String): String = {
    val param = connection.getSFBaseSession.getOtherParameter(name)
    if (param == null) null else param.toString
  }

  override def getQueryStatus(queryId: String): QueryStatus = {
    val status = connection.getSFBaseSession.getQueryStatus(queryId)
    QueryStatus
      .builder(String.valueOf(status))
      .stillRunning(SFQueryStatus.isStillRunning(status))
      .build()
  }

  override def getChildQueryIds(queryId: String): JList[String] =
    java.util.Arrays.asList(connection.getChildQueryIds(queryId): _*)

  override def executeAsyncQuery(statement: PreparedStatement): ResultSet =
    statement.asInstanceOf[SnowflakePreparedStatement].executeAsyncQuery()

  override def setStatementParameter(statement: Statement, name: String, value: Any): Unit =
    statement.asInstanceOf[SnowflakeStatement].setParameter(name, value)

  override def getQueryId(statement: Statement): String =
    statement.asInstanceOf[SnowflakeStatement].getQueryID

  override def getQueryId(resultSet: ResultSet): String =
    resultSet.asInstanceOf[SnowflakeResultSet].getQueryID

  override def uploadStream(
      stageName: String,
      destPrefix: String,
      data: InputStream,
      destFileName: String,
      compress: Boolean): Unit =
    connection.uploadStream(stageName, destPrefix, data, destFileName, compress)

  override def downloadStream(
      stageName: String,
      sourceFileName: String,
      decompress: Boolean): InputStream =
    connection.downloadStream(stageName, sourceFileName, decompress)

  override def submitTelemetry(eventJson: String): Unit = {
    val event = Jdbc3Session.Mapper.readTree(eventJson).asInstanceOf[ObjectNode]
    telemetryClient.addLogToBatch(TelemetryUtil.buildJobData(event))
    telemetryClient.sendBatchAsync()
  }

  override def getColumnFields(metadata: ResultSetMetaData, column: Int): JList[FieldMetadata] =
    Jdbc3Session.toFieldMetadata(
      metadata.asInstanceOf[SnowflakeResultSetMetaData].getColumnFields(column))

  override def getStructuredObject(resultSet: ResultSet, column: Int): AnyRef = {
    val meta = resultSet.getMetaData
    val field = FieldMetadata
      .builder(meta.getColumnTypeName(column), meta.getColumnType(column))
      .name(meta.getColumnName(column))
      .fields(getColumnFields(meta, column))
      .build()
    new Jdbc3StructuredDecoder(resultSet).decode(column, field)
  }

  override def isSnowflakeError(error: Throwable): Boolean = Jdbc3Session.isSnowflakeError(error)

  override def isReauthenticationRequest(error: Throwable): Boolean =
    error.isInstanceOf[SnowflakeReauthenticationRequest]

  override def isDriverInternalError(error: Throwable): Boolean = error match {
    case e: SnowflakeSQLException =>
      e.getSQLState == SqlState.INTERNAL_ERROR &&
      (e.getQueryId == null || e.getQueryId.isEmpty || e.getQueryId == "unknown")
    case e: SFException => e.getSqlState == SqlState.INTERNAL_ERROR
    case _ => false
  }
}

private[snowpark] object Jdbc3Session {
  private val Mapper = new ObjectMapper()

  /** The session of a client connection opened with the Snowpark client app id. */
  def createClientSession(connURL: String, connParam: Properties): SnowflakeSession = {
    val connStr = SnowflakeConnectString.parse(connURL, connParam)
    if (!connStr.isValid) {
      throw ErrorMessage.MISC_INVALID_CONNECTION_STRING(s"$connStr")
    }
    new Jdbc3Session(
      new SnowflakeConnectionV1(new SnowparkSFConnectionHandler(connStr), connURL, connParam),
      storedProcedure = false)
  }

  /** The session of a legacy stored-procedure call, on the connection the runtime created. */
  def forStoredProcedure(connection: SnowflakeConnectionV1): SnowflakeSession =
    new Jdbc3Session(connection, storedProcedure = true)

  def isSnowflakeError(error: Throwable): Boolean = error.isInstanceOf[SnowflakeSQLException]

  private def toFieldMetadata(fields: JList[SFFieldMetadata]): JList[FieldMetadata] =
    if (fields == null) {
      java.util.Collections.emptyList()
    } else {
      fields.asScala
        .map(f =>
          FieldMetadata
            .builder(f.getTypeName, f.getType)
            .name(f.getName)
            .precision(f.getPrecision)
            .scale(f.getScale)
            .nullable(f.isNullable)
            .fields(toFieldMetadata(f.getFields))
            .build())
        .asJava
    }
}

/** Decodes the driver's representation of structured values into JDK types. */
private class Jdbc3StructuredDecoder(data: ResultSet) {
  // the baseResultSet is not always SFArrowResultSet
  private lazy val baseResultSet: SFBaseResultSet = {
    val field = classOf[SnowflakeBaseResultSet].getDeclaredField("sfBaseResultSet")
    field.setAccessible(true)
    field.get(data.asInstanceOf[SnowflakeBaseResultSet]).asInstanceOf[SFBaseResultSet]
  }

  private lazy val arrowResultSet: SFArrowResultSet =
    baseResultSet.asInstanceOf[SFArrowResultSet]

  def decode(column: Int, field: FieldMetadata): AnyRef =
    decode(SnowflakeUtil.mapSFExceptionToSQLException(() => baseResultSet.getObject(column)), field)

  private def decode(value: Any, field: FieldMetadata): AnyRef = {
    lazy val fields = field.getFields.asScala
    if (value == null) {
      null
    } else {
      field.getTypeName match {
        case "ARRAY" | "OBJECT" if fields.isEmpty => value.toString
        case "ARRAY" =>
          val elements = value match {
            case arr: java.util.ArrayList[_] => arr.asScala // nested, or JDBC older than 3.21.0
            case w: StructObjectWrapper => w.getObject.asInstanceOf[JsonStringArrayList[_]].asScala
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported Structured Array Type: ${value.getClass.getSimpleName}")
          }
          new java.util.ArrayList[AnyRef](elements.map(decode(_, fields.head)).asJava)
        // structured map, Map type has two fields, and both field names are empty
        case "OBJECT" if fields.size == 2 && fields.head.getName.isEmpty =>
          val entries: Iterable[(Any, Any)] = value match {
            // nested structured maps are JsonStringArrayValues
            case subMap: JsonStringArrayList[_] =>
              subMap.asScala.map { case entry: JsonStringHashMap[_, _] =>
                entry.get("key") -> entry.get("value")
              }
            case map: java.util.HashMap[_, _] => map.asScala.toSeq
            case w: StructObjectWrapper =>
              w.getObject.asInstanceOf[java.util.HashMap[_, _]].asScala.toSeq
          }
          val result = new java.util.LinkedHashMap[AnyRef, AnyRef]()
          entries.foreach { case (k, v) =>
            result.put(decode(k, fields.head), decode(v, fields(1)))
          }
          result
        case "OBJECT" =>
          value match {
            case w: StructObjectWrapper => decode(w.getObject, field)
            case input: ArrowSqlInput => decode(input.getInput, field)
            case map: java.util.Map[_, _] =>
              val result = new java.util.LinkedHashMap[String, AnyRef]()
              map.asScala.toList.zip(fields).foreach { case ((k, v), f) =>
                result.put(k.toString, decode(v, f))
              }
              result
          }
        case "VARCHAR" | "VARIANT" => value.toString
        case "DATE" => arrowResultSet.convertToDate(value, null)
        case "TIME" => arrowResultSet.convertToTime(value, field.getScale)
        case _
            if field.getSqlType == java.sql.Types.TIMESTAMP ||
              field.getSqlType == java.sql.Types.TIMESTAMP_WITH_TIMEZONE =>
          val columnType =
            ColumnTypeHelper.getColumnType(field.getSqlType, arrowResultSet.getSession)
          arrowResultSet.convertToTimestamp(
            value,
            columnType,
            field.getSqlType,
            null,
            field.getScale)
        case _ => value.asInstanceOf[AnyRef]
      }
    }
  }
}
