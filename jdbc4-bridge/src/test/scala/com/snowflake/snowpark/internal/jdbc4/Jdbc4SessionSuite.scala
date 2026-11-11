package com.snowflake.snowpark.internal.jdbc4

import java.sql.Connection
import java.util.Properties

import com.snowflake.session.api.{FieldMetadata, SnowflakeSessionLoader}
import com.snowflake.snowpark.SnowparkClientException
import com.snowflake.snowpark.internal.jdbc.SnowflakeSessions
import com.snowflake.snowpark.internal.sproc.StoredProcSessionFactory
import net.snowflake.client.api.connection.SnowflakeConnection
import net.snowflake.client.api.exception.{ErrorCode, SnowflakeSQLException}
import net.snowflake.client.internal.api.implementation.resultset.SnowflakeBaseResultSet
import net.snowflake.client.internal.core.SFBaseResultSet
import net.snowflake.client.internal.core.arrow.StructObjectWrapper
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.JsonStringArrayList
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

  test("an invalid connect string is an IllegalArgumentException from the factory") {
    val factory = SnowflakeSessionLoader.loadClientSessionFactory(getClass.getClassLoader).get
    val ex = intercept[IllegalArgumentException](
      factory.createSession("not-a-snowflake-url", new Properties(), "Snowpark", "1.0.0"))
    assert(ex.getMessage != null && ex.getMessage.nonEmpty)
  }

  test("an invalid connect string is a SnowparkClientException from createClientSession") {
    val factory = SnowflakeSessionLoader.loadClientSessionFactory(getClass.getClassLoader).get
    val description = intercept[IllegalArgumentException](
      factory
        .createSession("not-a-snowflake-url", new Properties(), "Snowpark", "1.0.0")).getMessage
    val ex = intercept[SnowparkClientException](
      SnowflakeSessions.createClientSession("not-a-snowflake-url", new Properties()))
    assert(ex.errorCode == "0405")
    assert(ex.message.contains("Invalid connection string"))
    assert(ex.message.contains(description))
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

  private def decodeArray(raw: AnyRef): AnyRef = {
    val sfBase = Mockito.mock(classOf[SFBaseResultSet])
    Mockito.when(sfBase.getObject(1)).thenReturn(raw, Nil: _*)
    val rs = Mockito.mock(classOf[SnowflakeBaseResultSet])
    val base = classOf[SnowflakeBaseResultSet].getDeclaredField("sfBaseResultSet")
    base.setAccessible(true)
    base.set(rs, sfBase)
    val number =
      FieldMetadata.builder("NUMBER", java.sql.Types.INTEGER).name("").precision(10).build()
    val array = FieldMetadata
      .builder("ARRAY", java.sql.Types.ARRAY)
      .name("c")
      .fields(java.util.Arrays.asList(number))
      .build()
    new Jdbc4StructuredDecoder(rs).decode(1, array)
  }

  test("decoder returns a plain list for nested raw JsonStringArrayList values") {
    val raw = new JsonStringArrayList[AnyRef]()
    raw.add(java.math.BigDecimal.ONE)
    raw.add(null)
    val decoded = decodeArray(raw)
    assert(decoded.getClass == classOf[java.util.ArrayList[_]])
    assert(decoded == java.util.Arrays.asList(java.math.BigDecimal.ONE, null))
  }

  test("decoder returns a plain list for the StructObjectWrapper path") {
    val inner = new JsonStringArrayList[AnyRef]()
    inner.add(java.math.BigDecimal.ONE)
    val decoded = decodeArray(new StructObjectWrapper(null, inner))
    assert(decoded.getClass == classOf[java.util.ArrayList[_]])
    assert(decoded == java.util.Arrays.asList(java.math.BigDecimal.ONE))
  }
}
