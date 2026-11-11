package com.snowflake.session.api;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Creates client {@link SnowflakeSession}s. Implemented by the Snowflake JDBC 4.x driver and
 * discovered by Snowpark through {@link SnowflakeSessionLoader#loadClientSessionFactory}. When no
 * implementation is present, Snowpark falls back to its own JDBC 3.x implementation.
 */
public interface SnowflakeClientSessionFactory {

  /**
   * Opens a connection to {@code url} with {@code properties} and returns a session on it. The
   * connection reports {@code clientAppId} and {@code clientAppVersion} as the calling application,
   * for example Snowpark and its version. The caller closes the connection.
   *
   * @throws IllegalArgumentException if {@code url} and {@code properties} do not form a valid
   *     Snowflake connect string; the message describes the connect string
   */
  SnowflakeSession createSession(
      String url, Properties properties, String clientAppId, String clientAppVersion)
      throws SQLException;
}
