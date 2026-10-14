package com.snowflake.snowpark.internal.jdbc

import java.io.InputStream
import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode

/**
 * Snowflake-specific JDBC operations used by Snowpark, independent of the JDBC major version.
 *
 * JDBC 3.x and 4.x expose these operations through different classes (for example
 * `net.snowflake.client.jdbc.SnowflakeConnectionV1` and
 * `net.snowflake.client.api.connection.SnowflakeConnection`). Shared Snowpark code must only use
 * this trait; [[JdbcBridge.forConnection]] picks the implementation that matches the driver on the
 * classpath, so the same Snowpark jar runs on either driver.
 */
private[snowpark] trait JdbcBridge {
  def sessionId: String
  def database: String
  def schema: String
  def queryState(queryId: String): QueryState
  def childQueryIds(queryId: String): Array[String]

  /** Returns the value of a session parameter known to the driver, or null when absent. */
  def sessionParameter(name: String): String
  def submitTelemetry(message: ObjectNode): Unit

  def uploadStream(
      stageName: String,
      destPrefix: String,
      inputStream: InputStream,
      destFileName: String,
      compressData: Boolean): Unit
  def downloadStream(stageName: String, sourceFileName: String, decompress: Boolean): InputStream

  def setStatementParameter(statement: Statement, name: String, value: Any): Unit
  def statementQueryId(statement: Statement): String
  def resultSetQueryId(resultSet: ResultSet): String
  def executeAsyncQuery(statement: PreparedStatement): ResultSet

  def columnFields(meta: ResultSetMetaData, index: Int): List[FieldInfo]
  def structuredResult(resultSet: ResultSet): StructuredResultReader
}

/** Driver-specific access to structured (ARRAY / OBJECT / MAP) values of a result set. */
private[snowpark] trait StructuredResultReader {
  def rawObject(index: Int): Any

  /** The wrapped value when `value` is the driver's structured-object wrapper. */
  def unwrapStructuredObject(value: Any): Option[Any]

  /** The underlying input when `value` is the driver's Arrow SQL input. */
  def unwrapSqlInput(value: Any): Option[Any]

  def toDate(value: Any): Any
  def toTime(value: Any, scale: Int): Any
  def toTimestamp(value: Any, sqlType: Int, scale: Int): Any
}

private[snowpark] case class FieldInfo(
    name: String,
    typeName: String,
    sqlType: Int,
    precision: Int,
    scale: Int,
    nullable: Boolean,
    fields: List[FieldInfo])

private[snowpark] case class QueryState(isStillRunning: Boolean, description: String) {
  override def toString: String = description
}

private[snowpark] object JdbcBridge {
  private val Jdbc3ConnectionClass = "net.snowflake.client.jdbc.SnowflakeConnectionV1"
  private val Jdbc4ConnectionInterface = "net.snowflake.client.api.connection.SnowflakeConnection"
  private val Jdbc4Support = "com.snowflake.snowpark.internal.jdbc4.Jdbc4Support"

  private[snowpark] def loadClass(name: String): Option[Class[_]] =
    try {
      Some(Class.forName(name, false, getClass.getClassLoader))
    } catch {
      case _: ClassNotFoundException | _: LinkageError => None
    }

  private[snowpark] lazy val isJdbc3Available: Boolean = loadClass(Jdbc3ConnectionClass).isDefined

  private[snowpark] def isJdbc4Connection(conn: Connection): Boolean =
    loadClass(Jdbc4ConnectionInterface).exists(conn.isWrapperFor(_))

  def forConnection(conn: Connection): JdbcBridge =
    loadClass(Jdbc3ConnectionClass) match {
      case Some(c) if conn.isWrapperFor(c) => Jdbc3Bridge(conn)
      case _ if isJdbc4Connection(conn) => jdbc4Support.bridge(conn)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported JDBC connection: ${conn.getClass.getName}. Expected a Snowflake JDBC " +
            s"connection ($Jdbc3ConnectionClass or $Jdbc4ConnectionInterface).")
    }

  def createClientConnection(connURL: String, connParam: Properties): Connection =
    if (isJdbc3Available) {
      Jdbc3Bridge.createClientConnection(connURL, connParam)
    } else {
      jdbc4Support.createClientConnection(connURL, connParam)
    }

  private lazy val jdbc4Support: Jdbc4SupportApi =
    loadClass(Jdbc4Support + "$") match {
      case Some(c) => c.getField("MODULE$").get(null).asInstanceOf[Jdbc4SupportApi]
      case None =>
        throw new IllegalStateException(
          s"No supported Snowflake JDBC driver found on the classpath ($Jdbc4Support is missing).")
    }
}

/** Implemented by the JDBC 4.x module, which is compiled separately against JDBC 4.x. */
private[snowpark] trait Jdbc4SupportApi {
  def bridge(conn: Connection): JdbcBridge
  def createClientConnection(connURL: String, connParam: Properties): Connection
}

/** Driver-independent checks for driver exception types. */
private[snowpark] object JdbcExceptions {
  private val SnowflakeSQLExceptionClasses = Set(
    "net.snowflake.client.jdbc.SnowflakeSQLException",
    "net.snowflake.client.api.exception.SnowflakeSQLException")
  private val ReauthenticationRequestClasses = Set(
    "net.snowflake.client.jdbc.SnowflakeReauthenticationRequest",
    "net.snowflake.client.internal.jdbc.SnowflakeReauthenticationRequest")

  private def isInstanceOfAny(t: Throwable, classNames: Set[String]): Boolean = {
    var c: Class[_] = t.getClass
    while (c != null) {
      if (classNames.contains(c.getName)) return true
      c = c.getSuperclass
    }
    false
  }

  def isReauthenticationRequest(t: Throwable): Boolean =
    isInstanceOfAny(t, ReauthenticationRequestClasses)

  /** Matches a Snowflake driver `SnowflakeSQLException` from either JDBC major version. */
  object SnowflakeSQLException {
    def unapply(t: Throwable): Option[java.sql.SQLException] = t match {
      case e: java.sql.SQLException if isInstanceOfAny(e, SnowflakeSQLExceptionClasses) => Some(e)
      case _ => None
    }
  }
}

/** Driver-independent access to static driver utilities. */
private[snowpark] object JdbcRuntime {
  private lazy val driverClass: Option[Class[_]] =
    JdbcBridge
      .loadClass("net.snowflake.client.jdbc.SnowflakeDriver")
      .orElse(JdbcBridge.loadClass("net.snowflake.client.api.driver.SnowflakeDriver"))

  /** The driver class, used to locate the JDBC jar. */
  def snowflakeDriverClass: Option[Class[_]] = driverClass

  lazy val driverVersion: String =
    driverClass
      .flatMap { c =>
        def static(name: String): Option[Any] =
          try {
            Some(c.getMethod(name).invoke(null))
          } catch { case _: ReflectiveOperationException => None }
        def field(name: String): Option[Any] =
          try {
            Some(c.getField(name).get(null))
          } catch { case _: ReflectiveOperationException => None }
        field("implementVersion").orElse(static("getImplementationVersion"))
      }
      .map(String.valueOf)
      .getOrElse("unknown")

  private lazy val maskSecretsMethod: Option[java.lang.reflect.Method] =
    JdbcBridge
      .loadClass("net.snowflake.client.util.SecretDetector")
      .orElse(JdbcBridge.loadClass("net.snowflake.client.internal.util.SecretDetector"))
      .flatMap { c =>
        try {
          Some(c.getMethod("maskSecrets", classOf[String]))
        } catch { case _: NoSuchMethodException => None }
      }

  def maskSecrets(msg: String): String =
    maskSecretsMethod match {
      case Some(m) => m.invoke(null, msg).asInstanceOf[String]
      case None => msg
    }
}
