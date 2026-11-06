package com.snowflake.session.api;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * The Snowflake-specific session functions Snowpark needs beyond {@code java.sql}, so that the
 * Snowpark runtime does not depend on the classes of any particular Snowflake JDBC driver.
 *
 * <p>The same interface serves client and stored-procedure sessions:
 *
 * <ul>
 *   <li>a client implementation, shipped with the Snowflake JDBC 4.x driver;
 *   <li>a stored-procedure implementation, shipped with the stored-procedure JDBC runtime adapter,
 *       which reuses the driver's implementation on the procedure's connection;
 *   <li>a JDBC 3.x implementation, shipped with Snowpark, for JDBC 3.x clients and the legacy
 *       stored-procedure path.
 * </ul>
 *
 * <p>Sessions are created by {@link SnowflakeClientSessionFactory} or {@link
 * SnowflakeStoredProcSessionFactory}; creation and teardown are not part of this interface.
 *
 * <p>Signatures use JDK types only. Methods added after the first release must be {@code default}
 * methods so that an older implementation keeps working with a newer Snowpark. The value classes
 * {@link FieldMetadata} and {@link QueryStatus} are created through builders for the same reason.
 */
public interface SnowflakeSession {

  /** The JDBC connection that SQL is executed on. Owned by the session's creator. */
  Connection getConnection();

  /**
   * Whether this session runs inside a stored procedure. Snowpark keys its stored-procedure
   * behavior off this, for example forbidding {@code close()} and skipping the query tag.
   */
  boolean isStoredProcedure();

  String getSessionId() throws SQLException;

  String getDatabase() throws SQLException;

  String getSchema() throws SQLException;

  /** Returns the value of the named session parameter, or {@code null} if it is not set. */
  String getSessionParameter(String name) throws SQLException;

  QueryStatus getQueryStatus(String queryId) throws SQLException;

  /** Returns the IDs of the queries run by a multi-statement query, in execution order. */
  List<String> getChildQueryIds(String queryId) throws SQLException;

  /**
   * Submits {@code statement} for asynchronous execution and returns immediately. The query ID is
   * available through {@link #getQueryId(ResultSet)}.
   */
  ResultSet executeAsyncQuery(PreparedStatement statement) throws SQLException;

  /** Sets a Snowflake statement-level parameter, such as {@code QUERY_TAG}. */
  void setStatementParameter(Statement statement, String name, Object value) throws SQLException;

  String getQueryId(Statement statement) throws SQLException;

  String getQueryId(ResultSet resultSet) throws SQLException;

  /**
   * Uploads {@code data} to {@code stageName/destPrefix/destFileName}, compressing it first when
   * {@code compress} is set.
   */
  void uploadStream(
      String stageName, String destPrefix, InputStream data, String destFileName, boolean compress)
      throws SQLException;

  InputStream downloadStream(String stageName, String sourceFileName, boolean decompress)
      throws SQLException;

  /** Submits one client telemetry event, given as a JSON object. */
  void submitTelemetry(String eventJson) throws SQLException;

  /**
   * Describes column {@code column} (1-based) of {@code metadata}, including the nested fields of
   * structured ARRAY, MAP and OBJECT columns. Returns one element for a scalar column.
   */
  List<FieldMetadata> getColumnFields(ResultSetMetaData metadata, int column) throws SQLException;

  /**
   * Returns the value of structured column {@code column} (1-based) of the current row as JDK
   * types: ARRAY as {@link List}, MAP as {@link java.util.Map}, and OBJECT as {@link java.util.Map}
   * from field name to value in field order, at every nesting level. Nested DATE, TIME and
   * TIMESTAMP values are converted to {@link java.sql.Date}, {@link java.sql.Time} and {@link
   * java.sql.Timestamp}. Returns {@code null} for SQL NULL.
   */
  Object getStructuredObject(ResultSet resultSet, int column) throws SQLException;

  /** Whether {@code error} was raised by Snowflake, as opposed to by the client or the network. */
  boolean isSnowflakeError(Throwable error);

  /** Whether {@code error} asks the client to authenticate again, for example an expired token. */
  boolean isReauthenticationRequest(Throwable error);

  /**
   * Whether {@code error} is an internal failure of the driver itself rather than an error in user
   * code or in a query it ran. The native stored-procedure runtime reports such errors from a
   * procedure as internal errors instead of user errors.
   */
  boolean isDriverInternalError(Throwable error);
}
