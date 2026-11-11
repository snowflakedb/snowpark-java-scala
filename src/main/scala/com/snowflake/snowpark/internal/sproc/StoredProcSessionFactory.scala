package com.snowflake.snowpark.internal.sproc

import com.snowflake.session.api.SnowflakeSession
import com.snowflake.snowpark.Session

/**
 * Stored-procedure entry point for the exact-runtime-bundle path, where the runtime opens the
 * call's [[SnowflakeSession]] through the stored-procedure session factory.
 *
 * Its JNI descriptor
 * `(Lcom/snowflake/session/api/SnowflakeSession;)Lcom/snowflake/snowpark/Session;` does not depend
 * on any JDBC class. The legacy runtime keeps calling `Session.apply(SnowflakeConnectionV1)`; both
 * entry points live in the same Snowpark jar.
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
