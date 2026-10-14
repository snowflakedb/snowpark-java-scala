package com.snowflake.snowpark.internal.sproc

import com.snowflake.snowpark.Session
import com.snowflake.snowpark.internal.jdbc.JdbcBridge

/**
 * Stored-procedure session factory for the exact-runtime-bundle path, where XP supplies a JDBC 4.x
 * connection created by the JDBC adapter.
 *
 * The entry point only accepts [[java.sql.Connection]], so its JNI descriptor
 * `(Ljava/sql/Connection;)Lcom/snowflake/snowpark/Session;` does not depend on any JDBC class. The
 * legacy runtime keeps calling `Session.apply(SnowflakeConnectionV1)`; both entry points live in
 * the same Snowpark jar.
 */
private[snowpark] object StoredProcSessionFactory {

  /**
   * Creates a Snowpark [[Session]] from a JDBC 4.x stored-procedure connection.
   *
   * @throws IllegalArgumentException
   *   if `conn` is null or is not a JDBC 4.x Snowflake connection.
   */
  def fromJdbcConnection(conn: java.sql.Connection): Session = {
    require(conn != null, "conn must not be null")
    if (!JdbcBridge.isJdbc4Connection(conn)) {
      throw new IllegalArgumentException(
        s"StoredProcSessionFactory.fromJdbcConnection: conn (${conn.getClass.getName}) cannot be " +
          "unwrapped to net.snowflake.client.api.connection.SnowflakeConnection.")
    }
    Session.fromJdbcConnection(conn)
  }
}
