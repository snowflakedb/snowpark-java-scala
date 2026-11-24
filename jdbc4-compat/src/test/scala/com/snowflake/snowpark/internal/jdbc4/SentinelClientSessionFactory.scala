package com.snowflake.snowpark.internal.jdbc4

import java.util.Properties

import com.snowflake.session.api.{SnowflakeClientSessionFactory, SnowflakeSession}

/** Raised by [[SentinelClientSessionFactory]] so tests can tell that the provider was called. */
class SentinelProviderCalled extends RuntimeException("test provider was called")

/**
 * Test-only stand-in for the client session factory that a JDBC 4.x driver registers. It is
 * registered in this project's test resources and never connects.
 */
class SentinelClientSessionFactory extends SnowflakeClientSessionFactory {
  override def createSession(
      url: String,
      properties: Properties,
      clientAppId: String,
      clientAppVersion: String): SnowflakeSession = throw new SentinelProviderCalled
}
