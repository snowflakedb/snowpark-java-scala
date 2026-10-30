package com.snowflake.snowpark.internal.jdbc4

import java.sql.Connection

import com.snowflake.session.api.SnowflakeSessionLoader
import com.snowflake.snowpark.internal.sproc.StoredProcSessionFactory
import net.snowflake.client.api.connection.SnowflakeConnection
import net.snowflake.client.api.exception.{ErrorCode, SnowflakeSQLException}
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

// Runs with only JDBC 4.x on the classpath, like a JDBC 4.x client or the exact-runtime
// stored-procedure bundle.
class Jdbc4SessionSuite extends AnyFunSuite {

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
    intercept[ClassNotFoundException](
      Class.forName("net.snowflake.client.jdbc.SnowflakeConnectionV1"))
  }

  test("Snowpark discovers the JDBC 4.x client session factory") {
    val factory = SnowflakeSessionLoader.loadClientSessionFactory(getClass.getClassLoader)
    assert(factory.get.isInstanceOf[Jdbc4ClientSessionFactory])
  }

  test("session parameter is read through SnowflakeConnection.getSessionParameter") {
    val (conn, sfConn) = jdbc4Connection()
    val session = new Jdbc4Session(conn, storedProcedure = false)
    getSessionParameter.toOption match {
      case Some(m) =>
        m.invoke(
          Mockito.doReturn("true", Seq.empty[Object]: _*).when(sfConn),
          "SNOWPARK_LAZY_ANALYSIS")
        assert(session.getSessionParameter("SNOWPARK_LAZY_ANALYSIS") == "true")
      case None =>
        // JDBC 4.x releases before snowflake-jdbc#19: fall back to SHOW PARAMETERS.
        assert(session.getSessionParameter("SNOWPARK_LAZY_ANALYSIS") == null)
    }
  }

  test("telemetry is sent through SnowflakeConnection.submitTelemetry") {
    val (conn, sfConn) = jdbc4Connection()
    val event = """{"type":"test"}"""
    new Jdbc4Session(conn, storedProcedure = false).submitTelemetry(event)
    submitTelemetry.toOption.foreach { m =>
      m.invoke(Mockito.verify(sfConn), event)
    }
  }

  test("errors are classified against JDBC 4.x exception types") {
    val (conn, _) = jdbc4Connection()
    val session = new Jdbc4Session(conn, storedProcedure = false)
    val userError = new SnowflakeSQLException("qid", "syntax error", "42000")
    val internalError = new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR, "bug")
    val serverError = new SnowflakeSQLException("qid", "server error", "XX000")
    assert(session.isSnowflakeError(userError))
    assert(!session.isSnowflakeError(new RuntimeException("other")))
    assert(!session.isDriverInternalError(userError))
    assert(session.isDriverInternalError(internalError))
    assert(!session.isDriverInternalError(serverError))
  }

  test("StoredProcSessionFactory rejects null and client sessions") {
    intercept[IllegalArgumentException](StoredProcSessionFactory.fromSnowflakeSession(null))
    val (conn, _) = jdbc4Connection()
    intercept[IllegalArgumentException](
      StoredProcSessionFactory.fromSnowflakeSession(
        new Jdbc4Session(conn, storedProcedure = false)))
  }

  test("StoredProcSessionFactory creates a stored-procedure Snowpark session on JDBC 4.x") {
    val (conn, _) = jdbc4Connection()
    val session =
      StoredProcSessionFactory.fromSnowflakeSession(new Jdbc4Session(conn, storedProcedure = true))
    assert(session.conn.isStoredProc)
    assert(session.conn.connection eq conn)
  }
}
