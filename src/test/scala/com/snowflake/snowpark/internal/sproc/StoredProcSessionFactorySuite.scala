package com.snowflake.snowpark.internal.sproc

import com.snowflake.snowpark.Session
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl
import net.snowflake.client.internal.core.SFBaseSession
import net.snowflake.client.internal.jdbc.telemetry.NoOpTelemetryClient
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

/**
 * Offline unit tests for [[StoredProcSessionFactory]].
 *
 * These tests do NOT require a live Snowflake connection. The happy-path test stubs just enough
 * JDBC internals (getTelemetryClient) so that Session construction completes without network I/O.
 */
class StoredProcSessionFactorySuite extends AnyFunSuite {

  test("fromJdbcConnection(null) throws IllegalArgumentException") {
    val ex = intercept[IllegalArgumentException] {
      StoredProcSessionFactory.fromJdbcConnection(null)
    }
    assert(ex.getMessage.contains("conn must not be null"))
  }

  test("fromJdbcConnection(non-SnowflakeConnectionImpl) throws IllegalArgumentException") {
    val wrongConn = Mockito.mock(classOf[java.sql.Connection])
    val ex = intercept[IllegalArgumentException] {
      StoredProcSessionFactory.fromJdbcConnection(wrongConn)
    }
    assert(ex.getMessage.contains(
      "expected net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl"))
  }

  /**
   * Happy-path: fromJdbcConnection(SnowflakeConnectionImpl) must dispatch correctly (not throw
   * IllegalArgumentException) and must return a non-null Session.
   *
   * The mock stubs isClosed → false and getSFBaseSession → a stub that returns NoOpTelemetryClient
   * so that Session construction succeeds without any network round-trip.
   */
  test("fromJdbcConnection(SnowflakeConnectionImpl) returns non-null Session (offline stub)") {
    val mockImpl = Mockito.mock(classOf[SnowflakeConnectionImpl])
    val mockSfSession = Mockito.mock(classOf[SFBaseSession])
    val noopTelemetry = new NoOpTelemetryClient()

    Mockito.when(mockImpl.isClosed).thenReturn(false)
    Mockito.when(mockImpl.getSFBaseSession).thenReturn(mockSfSession)
    Mockito.when(mockSfSession.getTelemetryClient).thenReturn(noopTelemetry)

    val session: Session = StoredProcSessionFactory.fromJdbcConnection(mockImpl)
    assert(session != null, "fromJdbcConnection must return a non-null Session")
  }
}
