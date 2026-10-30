package com.snowflake.snowpark.internal.jdbc

import java.sql.SQLException
import java.util.Properties

import com.snowflake.session.api.{SnowflakeSession, SnowflakeSessionLoader}
import com.snowflake.snowpark.internal.{ErrorMessage, Utils}
import com.snowflake.snowpark.internal.jdbc3.Jdbc3Session

private[snowpark] object SnowflakeSessions {
  val ClientAppId = "Snowpark"

  /**
   * Logs in through the driver's session factory, or through the JDBC 3.x implementation when the
   * driver on the classpath does not provide one.
   */
  def createClientSession(connURL: String, connParam: Properties): SnowflakeSession = {
    val factory = SnowflakeSessionLoader.loadClientSessionFactory(getClass.getClassLoader)
    if (factory.isPresent) {
      try {
        factory.get.createSession(
          connURL,
          connParam,
          ClientAppId,
          Utils.extractValidVersionNumber(Utils.Version))
      } catch {
        // the factory contract: IllegalArgumentException when url/properties are not a valid
        // Snowflake connect string, with the connect string description as the message
        case e: IllegalArgumentException =>
          throw ErrorMessage.MISC_INVALID_CONNECTION_STRING(e.getMessage)
      }
    } else {
      Jdbc3Session.createClientSession(connURL, connParam)
    }
  }
}

/** Matches the errors that `session` reports as raised by Snowflake. */
private[snowpark] class SnowflakeErrorExtractor(session: SnowflakeSession) {
  def unapply(t: Throwable): Option[SQLException] = t match {
    case e: SQLException if session.isSnowflakeError(e) => Some(e)
    case _ => None
  }
}
