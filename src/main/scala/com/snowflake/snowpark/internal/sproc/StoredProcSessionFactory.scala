package com.snowflake.snowpark.internal.sproc

import com.snowflake.session.api.SnowflakeSession
import com.snowflake.snowpark.Session

/**
 * Stored-procedure entry point for runtimes that open the call's [[SnowflakeSession]] themselves.
 *
 * The runtime calls it through JNI as
 * `(Lcom/snowflake/session/api/SnowflakeSession;)Lcom/snowflake/snowpark/Session;`, so the
 * signature must not change.
 */
private[snowpark] object StoredProcSessionFactory {

  /**
   * Creates a Snowpark [[Session]] on a stored-procedure session.
   *
   * @throws IllegalArgumentException
   *   if `session` is null or is not a stored-procedure session.
   */
  def fromSnowflakeSession(session: SnowflakeSession): Session = {
    require(session != null, "session must not be null")
    require(
      session.isStoredProcedure,
      s"StoredProcSessionFactory.fromSnowflakeSession: ${session.getClass.getName} is not a " +
        "stored-procedure session.")
    Session.fromSnowflakeSession(session)
  }
}
