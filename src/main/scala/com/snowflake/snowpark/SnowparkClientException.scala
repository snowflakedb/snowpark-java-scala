package com.snowflake.snowpark

/**
 * Represents a Snowpark client side exception.
 *
 * @since 0.1.0
 */
class SnowparkClientException private[snowpark] (
    val message: String,
    val errorCode: String,
    val telemetryMessage: String)
    extends RuntimeException(message) {

  // log error message via telemetry
  Session.getActiveSession.foreach(_.conn.telemetry.reportErrorMessage(this))
}
