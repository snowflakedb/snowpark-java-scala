package com.snowflake.snowpark.internal.jdbc4

import java.sql.Connection

import com.snowflake.snowpark.internal.jdbc.{JdbcBridge, JdbcExceptions, JdbcRuntime}
import com.snowflake.snowpark.internal.sproc.StoredProcSessionFactory
import net.snowflake.client.api.connection.SnowflakeConnection
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

// Runs with only JDBC 4.x on the classpath, like the exact-runtime stored-procedure bundle.
class Jdbc4BridgeSuite extends AnyFunSuite {

  private def jdbc4Connection(): (Connection, SnowflakeConnection) = {
    val sfConn = Mockito.mock(classOf[SnowflakeConnection])
    val conn = Mockito.mock(classOf[Connection])
    Mockito.when(conn.isWrapperFor(classOf[SnowflakeConnection])).thenReturn(true)
    Mockito.when(conn.unwrap(classOf[SnowflakeConnection])).thenReturn(sfConn)
    Mockito.when(conn.isClosed).thenReturn(false)
    (conn, sfConn)
  }

  private val getSessionParameter =
    scala.util.Try(classOf[SnowflakeConnection].getMethod("getSessionParameter", classOf[String]))
  private val submitTelemetry =
    scala.util.Try(classOf[SnowflakeConnection].getMethod("submitTelemetry", classOf[String]))

  test("JDBC 3.x is not on the classpath") {
    assert(!JdbcBridge.isJdbc3Available)
  }

  test("JDBC 4.x connection uses the JDBC 4.x bridge") {
    val (conn, _) = jdbc4Connection()
    assert(JdbcBridge.isJdbc4Connection(conn))
    assert(JdbcBridge.forConnection(conn).getClass.getName.endsWith("Jdbc4Bridge"))
  }

  test("session parameter is read through SnowflakeConnection.getSessionParameter") {
    val (conn, sfConn) = jdbc4Connection()
    getSessionParameter.toOption match {
      case Some(m) =>
        m.invoke(
          Mockito.doReturn("true", Seq.empty[Object]: _*).when(sfConn),
          "SNOWPARK_LAZY_ANALYSIS")
        assert(JdbcBridge.forConnection(conn).sessionParameter("SNOWPARK_LAZY_ANALYSIS") == "true")
      case None =>
        // JDBC 4.x releases before snowflake-jdbc#19: fall back to SHOW PARAMETERS.
        assert(JdbcBridge.forConnection(conn).sessionParameter("SNOWPARK_LAZY_ANALYSIS") == null)
    }
  }

  test("telemetry is sent through SnowflakeConnection.submitTelemetry") {
    val (conn, sfConn) = jdbc4Connection()
    val msg = new ObjectMapper().createObjectNode().put("type", "test")
    JdbcBridge.forConnection(conn).submitTelemetry(msg)
    submitTelemetry.toOption.foreach { m =>
      m.invoke(Mockito.verify(sfConn), msg.toString)
    }
  }

  test("driver version and exceptions resolve against JDBC 4.x") {
    assert(JdbcRuntime.snowflakeDriverClass.isDefined)
    assert(JdbcRuntime.driverVersion != "unknown")
    val ex = new net.snowflake.client.api.exception.SnowflakeSQLException(
      new RuntimeException("invalid"),
      "42000")
    assert(JdbcExceptions.SnowflakeSQLException.unapply(ex).contains(ex))
  }

  test("StoredProcSessionFactory rejects null and non-JDBC-4.x connections") {
    intercept[IllegalArgumentException](StoredProcSessionFactory.fromJdbcConnection(null))
    val other = Mockito.mock(classOf[Connection])
    intercept[IllegalArgumentException](StoredProcSessionFactory.fromJdbcConnection(other))
  }

  test("StoredProcSessionFactory creates a session from a JDBC 4.x connection") {
    val (conn, _) = jdbc4Connection()
    assert(StoredProcSessionFactory.fromJdbcConnection(conn) != null)
  }
}
