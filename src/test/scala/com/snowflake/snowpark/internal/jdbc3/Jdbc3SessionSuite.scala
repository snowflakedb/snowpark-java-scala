package com.snowflake.snowpark.internal.jdbc3

import java.sql.SQLException

import com.snowflake.session.api.SnowflakeSessionLoader
import com.snowflake.snowpark.internal.sproc.StoredProcSessionFactory
import net.snowflake.client.core.{SFException, SFSessionProperty}
import net.snowflake.client.jdbc.{
  ErrorCode,
  SnowflakeConnectionV1,
  SnowflakeReauthenticationRequest,
  SnowflakeSQLException
}
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

// Runs with the default JDBC 3.x dependency, i.e. the client and legacy stored-procedure path.
class Jdbc3SessionSuite extends AnyFunSuite {

  private def storedProcSession() = {
    val conn = Mockito.mock(classOf[SnowflakeConnectionV1])
    (Jdbc3Session.forStoredProcedure(conn), conn)
  }

  test("JDBC 3.x provides no client session factory, so Snowpark uses its own") {
    assert(!SnowflakeSessionLoader.loadClientSessionFactory(getClass.getClassLoader).isPresent)
  }

  test("legacy stored-procedure session reports itself as a stored procedure") {
    val (session, conn) = storedProcSession()
    assert(session.isStoredProcedure)
    assert(session.getConnection eq conn)
  }

  test("child query IDs come from the connection") {
    val (session, conn) = storedProcSession()
    Mockito.when(conn.getChildQueryIds("q")).thenReturn(Array("q1", "q2"))
    assert(session.getChildQueryIds("q") == java.util.Arrays.asList("q1", "q2"))
  }

  test("Snowflake errors are the driver's SnowflakeSQLException only") {
    val (session, _) = storedProcSession()
    assert(session.isSnowflakeError(new SnowflakeSQLException("invalid identifier 'A'", "42000")))
    assert(!session.isSnowflakeError(new SQLException("plain")))
    assert(!session.isSnowflakeError(new RuntimeException("other")))
  }

  test("reauthentication requests are recognized") {
    val (session, _) = storedProcSession()
    assert(
      session.isReauthenticationRequest(
        new SnowflakeReauthenticationRequest("qid", "expired", "08001", 390112)))
    assert(!session.isReauthenticationRequest(new SnowflakeSQLException("x", "08001")))
  }

  test("driver-internal errors have the internal SQL state and no query") {
    val (session, _) = storedProcSession()
    assert(session.isDriverInternalError(new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR, "x")))
    assert(
      !session.isDriverInternalError(new SnowflakeSQLException("qid", "server error", "XX000")))
    assert(!session.isDriverInternalError(new SnowflakeSQLException("syntax error", "42000")))
    assert(session.isDriverInternalError(new SFException(ErrorCode.INTERNAL_ERROR, "x")))
    assert(!session.isDriverInternalError(new RuntimeException("user")))
  }

  test("stored-procedure entry point rejects null and client sessions") {
    intercept[IllegalArgumentException](StoredProcSessionFactory.fromSnowflakeSession(null))
    val client = Mockito.mock(classOf[com.snowflake.session.api.SnowflakeSession])
    Mockito.when(client.isStoredProcedure).thenReturn(false)
    val ex =
      intercept[IllegalArgumentException](StoredProcSessionFactory.fromSnowflakeSession(client))
    assert(ex.getMessage.contains("is not a stored-procedure session"))
  }

  test("client info property key agrees with JDBC 3.x") {
    assert(SFSessionProperty.CLIENT_INFO.getPropertyKey == "snowflakeClientInfo")
  }
}
