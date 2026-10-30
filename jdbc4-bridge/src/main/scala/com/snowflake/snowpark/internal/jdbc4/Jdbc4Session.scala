package com.snowflake.snowpark.internal.jdbc4

import java.io.InputStream
import java.lang.reflect.{InvocationTargetException, Method}
import java.sql.{
  Connection,
  PreparedStatement,
  ResultSet,
  ResultSetMetaData,
  SQLException,
  SQLFeatureNotSupportedException,
  Statement
}
import java.util.{Properties, List => JList}

import com.snowflake.session.api.{
  FieldMetadata,
  QueryStatus,
  SnowflakeClientSessionFactory,
  SnowflakeSession
}
import net.snowflake.client.api.connection.{
  DownloadStreamConfig,
  SnowflakeConnection,
  UploadStreamConfig
}
import net.snowflake.client.api.exception.SnowflakeSQLException
import net.snowflake.client.api.resultset.{
  SnowflakeResultSet,
  SnowflakeResultSetMetaData,
  FieldMetadata => SFFieldMetadata
}
import net.snowflake.client.api.statement.{SnowflakePreparedStatement, SnowflakeStatement}
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl
import net.snowflake.client.internal.api.implementation.resultset.SnowflakeBaseResultSet
import net.snowflake.client.internal.core.arrow.StructObjectWrapper
import net.snowflake.client.internal.core.{
  ArrowSqlInput,
  ColumnTypeHelper,
  SFArrowResultSet,
  SFBaseResultSet,
  SFException
}
import net.snowflake.client.internal.jdbc.{
  DefaultSFConnectionHandler,
  SnowflakeConnectString,
  SnowflakeReauthenticationRequest,
  SnowflakeUtil
}
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.{
  JsonStringArrayList,
  JsonStringHashMap
}
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState

import scala.collection.JavaConverters._

/**
 * Reference JDBC 4.x implementation of [[SnowflakeClientSessionFactory]], standing in for the one
 * the JDBC 4.x driver will ship.
 */
class Jdbc4ClientSessionFactory extends SnowflakeClientSessionFactory {
  override def createSession(
      url: String,
      properties: Properties,
      clientAppId: String,
      clientAppVersion: String): SnowflakeSession = {
    val connStr = SnowflakeConnectString.parse(url, properties)
    if (!connStr.isValid) {
      throw new SQLException(s"Invalid connection string: $connStr")
    }
    val handler = new ClientAppConnectionHandler(connStr, clientAppId, clientAppVersion)
    new Jdbc4Session(new SnowflakeConnectionImpl(handler, url, properties), storedProcedure = false)
  }
}

private class ClientAppConnectionHandler(
    conStr: SnowflakeConnectString,
    clientAppId: String,
    clientAppVersion: String)
    extends DefaultSFConnectionHandler(conStr) {

  override def initializeConnection(url: String, info: Properties): Unit =
    super.initialize(SnowflakeConnectString.parse(url, info), clientAppId, clientAppVersion)
}

/**
 * [[SnowflakeSession]] on a JDBC 4.x connection. The stored-procedure runtime adapter reuses it on
 * the procedure's connection with `storedProcedure = true`.
 */
class Jdbc4Session(connection: Connection, storedProcedure: Boolean) extends SnowflakeSession {
  private val sfConn: SnowflakeConnection = connection.unwrap(classOf[SnowflakeConnection])

  override def getConnection: Connection = connection

  override def isStoredProcedure: Boolean = storedProcedure

  override def getSessionId: String = sfConn.getSessionID

  override def getDatabase: String = sfConn.getDatabase

  override def getSchema: String = connection.getSchema

  override def getSessionParameter(name: String): String =
    Jdbc4Session.getSessionParameterMethod match {
      case Some(m) =>
        try {
          Jdbc4Session.invoke(m, sfConn, name).asInstanceOf[String]
        } catch {
          case _: SQLFeatureNotSupportedException => null
        }
      case None => null
    }

  override def getQueryStatus(queryId: String): QueryStatus = {
    val status = sfConn.getQueryStatus(queryId)
    new QueryStatus(String.valueOf(status.getName), status.isStillRunning)
  }

  override def getChildQueryIds(queryId: String): JList[String] =
    java.util.Arrays.asList(sfConn.getChildQueryIds(queryId): _*)

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
    sfConn.uploadStream(
      stageName,
      destFileName,
      data,
      UploadStreamConfig.builder().setDestPrefix(destPrefix).setCompressData(compress).build())

  override def downloadStream(
      stageName: String,
      sourceFileName: String,
      decompress: Boolean): InputStream =
    sfConn.downloadStream(
      stageName,
      sourceFileName,
      DownloadStreamConfig.builder().setDecompress(decompress).build())

  override def submitTelemetry(eventJson: String): Unit =
    Jdbc4Session.submitTelemetryMethod.foreach(Jdbc4Session.invoke(_, sfConn, eventJson))

  override def getColumnFields(metadata: ResultSetMetaData, column: Int): JList[FieldMetadata] =
    Jdbc4Session.toFieldMetadata(
      metadata.asInstanceOf[SnowflakeResultSetMetaData].getColumnFields(column))

  override def getStructuredObject(resultSet: ResultSet, column: Int): AnyRef = {
    val meta = resultSet.getMetaData
    val field = new FieldMetadata(
      meta.getColumnName(column),
      meta.getColumnTypeName(column),
      meta.getColumnType(column),
      0,
      0,
      true,
      getColumnFields(meta, column))
    new Jdbc4StructuredDecoder(resultSet).decode(column, field)
  }

  override def isSnowflakeError(error: Throwable): Boolean =
    error.isInstanceOf[SnowflakeSQLException]

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

private object Jdbc4Session {
  // TODO: Call these directly once this compiles against a JDBC release that includes
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

  def toFieldMetadata(fields: JList[SFFieldMetadata]): JList[FieldMetadata] =
    if (fields == null) {
      java.util.Collections.emptyList()
    } else {
      fields.asScala
        .map(f =>
          new FieldMetadata(
            f.getName,
            f.getTypeName,
            f.getType,
            f.getPrecision,
            f.getScale,
            f.isNullable,
            toFieldMetadata(f.getFields)))
        .asJava
    }
}

/** Decodes the driver's representation of structured values into JDK types. */
private class Jdbc4StructuredDecoder(data: ResultSet) {
  // the baseResultSet is not always SFArrowResultSet
  //
  // `SnowflakeBaseResultSet` does not expose the field; once this code lives in the driver it can
  // read it directly.
  private lazy val baseResultSet: SFBaseResultSet = {
    val field =
      try {
        val f = classOf[SnowflakeBaseResultSet].getDeclaredField("sfBaseResultSet")
        f.setAccessible(true)
        f
      } catch {
        case e: NoSuchFieldException =>
          throw new IllegalStateException(
            "Jdbc4Session: could not locate 'sfBaseResultSet' on " +
              classOf[SnowflakeBaseResultSet].getName +
              ". The bundled JDBC driver may have renamed or removed this internal field.",
            e)
      }
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
            case arr: java.util.ArrayList[_] => arr.asScala
            case w: StructObjectWrapper => w.getObject.asInstanceOf[JsonStringArrayList[_]].asScala
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported Structured Array Type: ${value.getClass.getSimpleName}")
          }
          new java.util.ArrayList[AnyRef](elements.map(decode(_, fields.head)).asJava)
        // structured map, Map type has two fields, and both field names are empty
        case "OBJECT" if fields.size == 2 && fields.head.getName.isEmpty =>
          val entries: Seq[(Any, Any)] = value match {
            // nested structured maps are JsonStringArrayValues
            case subMap: JsonStringArrayList[_] =>
              subMap.asScala.toSeq.map { case entry: JsonStringHashMap[_, _] =>
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
