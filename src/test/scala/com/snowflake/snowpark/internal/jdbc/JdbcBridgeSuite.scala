package com.snowflake.snowpark.internal.jdbc

import java.sql.{Connection, SQLException}

import com.snowflake.snowpark.internal.sproc.StoredProcSessionFactory
import net.snowflake.client.core.SFSessionProperty
import net.snowflake.client.jdbc.{SnowflakeConnectionV1, SnowflakeDriver, SnowflakeSQLException}
import net.snowflake.client.util.SecretDetector
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

// Runs with the default JDBC 3.x dependency, i.e. the client and legacy stored-procedure path.
class JdbcBridgeSuite extends AnyFunSuite {

  test("JDBC 3.x connection uses the JDBC 3.x bridge") {
    val conn = Mockito.mock(classOf[SnowflakeConnectionV1])
    Mockito.when(conn.isWrapperFor(classOf[SnowflakeConnectionV1])).thenReturn(true)
    Mockito.when(conn.unwrap(classOf[SnowflakeConnectionV1])).thenReturn(conn)

    assert(JdbcBridge.isJdbc3Available)
    assert(JdbcBridge.forConnection(conn).isInstanceOf[Jdbc3Bridge])
  }

  test("non-Snowflake connection is rejected") {
    val conn = Mockito.mock(classOf[Connection])
    val ex = intercept[IllegalArgumentException](JdbcBridge.forConnection(conn))
    assert(ex.getMessage.contains("Unsupported JDBC connection"))
    assert(ex.getMessage.contains(classOf[SnowflakeConnectionV1].getName))
    assert(ex.getMessage.contains("net.snowflake.client.api.connection.SnowflakeConnection"))
  }

  test("JDBC 3.x connection is not treated as a JDBC 4.x connection") {
    assert(!JdbcBridge.isJdbc4Connection(Mockito.mock(classOf[SnowflakeConnectionV1])))
  }

  test("JDBC 4.x stored-procedure factory rejects JDBC 3.x on the JDBC 3.x classpath") {
    val conn = Mockito.mock(classOf[SnowflakeConnectionV1])
    val ex =
      intercept[IllegalArgumentException](StoredProcSessionFactory.fromJdbcConnection(conn))
    assert(ex.getMessage.contains("cannot be unwrapped"))
  }

  test("client info property key agrees with JDBC 3.x") {
    assert(SFSessionProperty.CLIENT_INFO.getPropertyKey == "snowflakeClientInfo")
  }

  test("SnowflakeSQLException matcher accepts the driver exception only") {
    val sfException = new SnowflakeSQLException("invalid identifier 'A'", "42000")
    assert(JdbcExceptions.SnowflakeSQLException.unapply(sfException).contains(sfException))
    assert(JdbcExceptions.SnowflakeSQLException.unapply(new SQLException("plain")).isEmpty)
    assert(JdbcExceptions.SnowflakeSQLException.unapply(new RuntimeException("other")).isEmpty)
  }

  test("reauthentication check matches the driver exception only") {
    val reauth =
      new net.snowflake.client.jdbc.SnowflakeReauthenticationRequest(
        "qid",
        "expired",
        "08001",
        390112)
    assert(JdbcExceptions.isReauthenticationRequest(reauth))
    assert(!JdbcExceptions.isReauthenticationRequest(new SnowflakeSQLException("x", "08001")))
  }

  test("driver version and secret masking come from the JDBC 3.x driver") {
    assert(JdbcRuntime.driverVersion == SnowflakeDriver.implementVersion)
    assert(JdbcRuntime.snowflakeDriverClass.contains(classOf[SnowflakeDriver]))
    val msg = "password=abc123 token=xyz"
    assert(JdbcRuntime.maskSecrets(msg) == SecretDetector.maskSecrets(msg))
  }
}
