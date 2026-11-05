package com.snowflake.session.api;

import java.sql.SQLException;
import java.util.Map;

/**
 * Opens and tears down the {@link SnowflakeSession} of a stored-procedure call. Implemented by the
 * stored-procedure JDBC runtime adapter and called by the native stored-procedure runtime, which
 * locates it through {@link SnowflakeSessionLoader#loadStoredProcSessionFactory}.
 */
public interface SnowflakeStoredProcSessionFactory {

  /**
   * Opens the session of the procedure call identified by {@code rsoId}. The returned session
   * reports {@link SnowflakeSession#isStoredProcedure()} as {@code true}.
   */
  SnowflakeSession openSession(int rsoId) throws SQLException;

  /**
   * Drops the temporary objects created during the call, then closes the session's connection.
   * {@code tempObjects} maps each fully qualified object name to its SQL object type, for example
   * {@code TABLE} or {@code FILE FORMAT}.
   */
  void closeSession(SnowflakeSession session, Map<String, String> tempObjects) throws SQLException;
}
