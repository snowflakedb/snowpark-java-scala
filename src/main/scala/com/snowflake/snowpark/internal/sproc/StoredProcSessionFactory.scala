package com.snowflake.snowpark.internal.sproc

import com.snowflake.snowpark.Session
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl

/**
 * <b>JDBC 4.x POC / INTERNAL-ONLY – unstable API, subject to removal without notice.</b>
 *
 * <p>Minimal stored-procedure session factory whose public entry point accepts
 * [[java.sql.Connection]] rather than any concrete JDBC implementation class, producing a JNI
 * descriptor that is stable across JDBC major versions:
 * {{{(Ljava/sql/Connection;)Lcom/snowflake/snowpark/Session;}}}
 *
 * <h3>Why this exists</h3> <p>The legacy entry point [[Session$.apply(SnowflakeConnectionImpl)*]]
 * encodes the concrete JDBC class name directly in its JNI method descriptor. This factory provides
 * a descriptor-neutral seam that hides the concrete JDBC type, enabling the native layer to invoke
 * Snowpark without hard-coding any JDBC implementation class name.
 *
 * <h3>JDBC 4.x support</h3> <p>This artifact is compiled against JDBC 4.0.1 and therefore requires
 * [[SnowflakeConnectionImpl]] (the JDBC 4.x successor to the removed SnowflakeConnectionV1). The
 * connection must be created via
 * [[net.snowflake.client.internal.jdbc.sproc.StoredProcConnectionFactory.fromHandler]] from a
 * pre-initialized [[net.snowflake.client.internal.jdbc.SFConnectionHandler]].
 *
 * <h3>API boundary</h3> <p>The public method [[fromJdbcConnection]] accepts only
 * [[java.sql.Connection]] so that the JNI descriptor visible to [[JavaMethodExecutor.cpp]] does not
 * reference any JDBC class. Callers must not cast the return type of
 * [[net.snowflake.client.internal.jdbc.sproc.StoredProcConnectionFactory.fromHandler]] to any
 * concrete type before passing it here.
 *
 * @since 1.22.0-SNAPSHOT
 *   (POC, JDBC 4.x artifact)
 */
private[snowpark] object StoredProcSessionFactory {

  /**
   * Creates a Snowpark [[Session]] from a [[java.sql.Connection]] produced by the JDBC 4.x
   * stored-procedure connection factory.
   *
   * <p>The JNI descriptor is <code>(Ljava/sql/Connection;)Lcom/snowflake/snowpark/Session;</code>,
   * which does not expose any concrete JDBC type to the native layer.
   *
   * <p>Internally this method casts <code>conn</code> to [[SnowflakeConnectionImpl]], which is the
   * only concrete type returned by
   * [[net.snowflake.client.internal.jdbc.sproc.StoredProcConnectionFactory.fromHandler]]. The cast
   * is kept inside this adapter package so that no other Snowpark package depends on the concrete
   * JDBC 4.x implementation class.
   *
   * @param conn
   *   Non-null [[java.sql.Connection]] from
   *   [[net.snowflake.client.internal.jdbc.sproc.StoredProcConnectionFactory.fromHandler]].
   * @return
   *   A fully initialised stored-procedure [[Session]].
   * @throws IllegalArgumentException
   *   if <code>conn</code> is null or not a [[SnowflakeConnectionImpl]].
   */
  def fromJdbcConnection(conn: java.sql.Connection): Session = {
    require(conn != null, "conn must not be null")
    conn match {
      case impl: SnowflakeConnectionImpl =>
        Session(impl)
      case other =>
        throw new IllegalArgumentException(
          s"[POC JDBC4] StoredProcSessionFactory.fromJdbcConnection: " +
            s"expected net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl " +
            s"(returned by StoredProcConnectionFactory.fromHandler), " +
            s"got ${other.getClass.getName}")
    }
  }
}
