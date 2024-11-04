package com.snowflake.snowpark_java;

import com.snowflake.snowpark.PublicPreview;
import com.snowflake.snowpark.SnowparkClientException;
import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.types.InternalUtils;
import com.snowflake.snowpark_java.types.StructType;
import java.net.URI;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Establishes a connection with a Snowflake database and provides methods for creating DataFrames
 * and accessing objects for working with files in stages.
 *
 * <p>When you create a {@code Session} object, you provide configuration settings to establish a
 * connection with a Snowflake database (e.g. the URL for the account, a user name, etc.). You can
 * specify these settings in a configuration file or in a Map that associates configuration setting
 * names with values.
 *
 * <p>To create a Session from a file:
 *
 * <pre>{@code
 * Session session = Session.builder().configFile("/path/to/file.properties").create();
 * }</pre>
 *
 * Session contains functions to construct {@code DataFrame}s like {@code Session.table}, {@code
 * Session.sql}, and {@code Session.read}
 *
 * @see com.snowflake.snowpark_java.DataFrame DataFrame
 * @since 0.8.0
 */
public class Session {
  private final com.snowflake.snowpark.Session session;
  private UDFRegistration udf = null;
  private UDTFRegistration udtf = null;

  private SProcRegistration sproc = null;

  Session(com.snowflake.snowpark.Session session) {
    this.session = session;
  }

  /**
   * Returns a builder you can use to set configuration properties and create a {@code Session}
   * object.
   *
   * @return A new {@code SessionBuilder} object.
   * @since 0.8.0
   */
  public static SessionBuilder builder() {
    return new SessionBuilder();
  }

  /**
   * Returns a new {@code DataFrame} representing the results of a SQL query.
   *
   * <p>You can use this method to execute an arbitrary SQL statement.
   *
   * @param query The SQL statement to execute.
   * @return A {@code DataFrame} object
   * @since 0.8.0
   */
  public DataFrame sql(String query) {
    return new DataFrame(session.sql(query, JavaUtils.objectArrayToSeq(new Object[0])));
  }

  /**
   * Returns a new {@code DataFrame} representing the results of a SQL query.
   *
   * <p>You can use this method to execute an arbitrary SQL statement.
   *
   * @param query The SQL statement to execute.
   * @param params The binding parameters for SQL statement (optional)
   * @return A {@code DataFrame} object
   * @since 1.15.0
   */
  public DataFrame sql(String query, Object... params) {
    return new DataFrame(session.sql(query, JavaUtils.objectArrayToSeq(params)));
  }

  /**
   * Returns a Updatable that points to the specified table.
   *
   * <p>{@code name} can be a fully qualified identifier and must conform to the rules for a
   * Snowflake identifier.
   *
   * @param name Table name that is either a fully qualified name or a name in the current
   *     database/schema.
   * @return A Updatable
   * @since 0.12.0
   */
  public Updatable table(String name) {
    return new Updatable(session.table(name));
  }

  /**
   * Returns an Updatable that points to the specified table.
   *
   * <p>{@code name} can be a fully qualified identifier and must conform to the rules for a
   * Snowflake identifier.
   *
   * @param multipartIdentifier An array of strings that specify the database name, schema name, and
   *     table name.
   * @return A Updatable
   * @since 0.12.0
   */
  public Updatable table(String[] multipartIdentifier) {
    return new Updatable(session.table(multipartIdentifier));
  }

  /**
   * Creates a new DataFrame from a range of numbers starting from 0. The resulting DataFrame has
   * the column name "ID" and a row for each number in the sequence.
   *
   * @param end End of the range.
   * @return A DataFrame
   * @since 0.12.0
   */
  public DataFrame range(long end) {
    return new DataFrame(session.range(end));
  }

  /**
   * Creates a new DataFrame from a range of numbers. The resulting DataFrame has the column name
   * "ID" and a row for each number in the sequence.
   *
   * @param start Start of the range.
   * @param end End of the range.
   * @param step Step function for producing the numbers in the range.
   * @return A DataFrame
   * @since 0.12.0
   */
  public DataFrame range(long start, long end, long step) {
    return new DataFrame(session.range(start, end, step));
  }

  /**
   * Creates a new DataFrame from a range of numbers. The resulting DataFrame has the column name
   * "ID" and a row for each number in the sequence.
   *
   * @param start Start of the range.
   * @param end End of the range.
   * @return A DataFrame
   * @since 0.12.0
   */
  public DataFrame range(long start, long end) {
    return new DataFrame(session.range(start, end));
  }

  /**
   * Creates a new DataFrame that uses the specified schema and contains the specified Row objects.
   *
   * <p>For example, the following code creates a DataFrame containing two columns of the types
   * `int` and `string` with two rows of data:
   *
   * <p>For example
   *
   * <pre>{@code
   * Row[] data = {Row.create(1, "a"), Row.create(2, "b")};
   * StructType schema = StructType.create(
   *   new StructField("num", DataTypes.IntegerType),
   *   new StructField("str", DataTypes.StringType));
   * DataFrame df = getSession().createDataFrame(data, schema);
   * }</pre>
   *
   * @param data An array of Row objects representing rows of data.
   * @param schema A StructType representing the schema for the DataFrame.
   * @return A DataFrame
   * @since 0.12.0
   */
  public DataFrame createDataFrame(Row[] data, StructType schema) {
    com.snowflake.snowpark.Row[] scalaRows = new com.snowflake.snowpark.Row[data.length];
    for (int i = 0; i < data.length; i++) {
      scalaRows[i] = data[i].getScalaRow();
    }
    return new DataFrame(
        session.createDataFrame(scalaRows, InternalUtils.toScalaStructType(schema)));
  }

  /**
   * Returns a UDFRegistration object that you can use to register UDFs.
   *
   * @since 0.12.0
   * @return A UDF utility class, which contains UDF registration functions.
   */
  public UDFRegistration udf() {
    if (udf == null) {
      this.udf = new UDFRegistration(session.udf());
    }
    return udf;
  }

  /**
   * Returns a UDTFRegistration object that you can use to register UDTFs.
   *
   * @since 1.4.0
   * @return A UDTFRegistration object that provides methods for registering UDTFs
   */
  public UDTFRegistration udtf() {
    if (udtf == null) {
      this.udtf = new UDTFRegistration(session.udtf());
    }
    return udtf;
  }

  /**
   * Removes a path from the set of dependencies.
   *
   * @since 0.12.0
   * @param path Path to a local directory, local file, or file in a stage.
   */
  public void removeDependency(String path) {
    session.removeDependency(path);
  }

  /**
   * Registers a file in stage or a local file as a dependency of a user-defined function (UDF).
   *
   * <p>The local file can be a JAR file, a directory, or any other file resource. If you pass the
   * path to a local file to {@code addDependency}, the Snowpark library uploads the file to a
   * temporary stage and imports the file when executing a UDF.
   *
   * <p>If you pass the path to a file in a stage to {@code addDependency}, the file is included in
   * the imports when executing a UDF.
   *
   * <p>Note that in most cases, you don't need to add the Snowpark JAR file and the JAR file (or
   * directory) of the currently running application as dependencies. The Snowpark library
   * automatically attempts to detect and upload these JAR files. However, if this automatic
   * detection fails, the Snowpark library reports this in an error message, and you must add these
   * JAR files explicitly by calling {@code addDependency}.
   *
   * <p>The following example demonstrates how to add dependencies on local files and files in a
   * stage:
   *
   * <pre>{@code
   * session.addDependency("@my_stage/http-commons.jar")
   * session.addDependency("/home/username/lib/language-detector.jar")
   * session.addDependency("./resource-dir/")
   * session.addDependency("./resource.xml")
   * }</pre>
   *
   * @since 0.12.0
   * @param path Path to a local directory, local file, or file in a stage.
   */
  public void addDependency(String path) {
    session.addDependency(path);
  }

  /**
   * Returns the list of URLs for all the dependencies that were added for user-defined functions
   * (UDFs). This list includes any JAR files that were added automatically by the library.
   *
   * @return A set of URI
   * @since 0.12.0
   */
  public Set<URI> getDependencies() {
    return session.getDependenciesAsJavaSet();
  }

  /**
   * Cancel all action methods that are running currently. This does not affect on any action
   * methods called in the future.
   *
   * @since 0.12.0
   */
  public void cancelAll() {
    // test in APIInternalSuite
    session.cancelAll();
  }

  /**
   * Returns the JDBC <a
   * href="https://docs.snowflake.com/en/user-guide/jdbc-api.html#object-connection">Connection</a>
   * object used for the connection to the Snowflake database.
   *
   * @return JDBC Connection object
   * @since 0.12.0
   */
  public Connection jdbcConnection() {
    return session.jdbcConnection();
  }

  /**
   * Sets a query tag for this session. You can use the query tag to find all queries run for this
   * session.
   *
   * <p>If not set, the default value of query tag is the Snowpark library call and the class and
   * method in your code that invoked the query (e.g. `com.snowflake.snowpark.DataFrame.collect
   * Main$.main(Main.scala:18)`).
   *
   * @param queryTag String to use as the query tag for this session.
   * @since 0.12.0
   */
  public void setQueryTag(String queryTag) {
    session.setQueryTag(queryTag);
  }

  /**
   * Unset query_tag parameter for this session.
   *
   * <p>If not set, the default value of query tag is the Snowpark library call and the class and
   * method in your code that invoked the query (e.g. `com.snowflake.snowpark.DataFrame.collect
   * Main$.main(Main.scala:18)`).
   *
   * @since 0.12.0
   */
  public void unsetQueryTag() {
    session.unsetQueryTag();
  }

  /**
   * Updates the query tag that is a JSON encoded string for the current session.
   *
   * <p>Keep in mind that assigning a value via {@link Session#setQueryTag(String)} will remove any
   * current query tag state.
   *
   * <p>Example 1:
   *
   * <pre>{@code
   * session.setQueryTag("{\"key1\":\"value1\"}");
   * session.updateQueryTag("{\"key2\":\"value2\"}");
   * System.out.println(session.getQueryTag().get());
   * {"key1":"value1","key2":"value2"}
   * }</pre>
   *
   * <p>Example 2:
   *
   * <pre>{@code
   * session.sql("ALTER SESSION SET QUERY_TAG = '{\"key1\":\"value1\"}'").collect();
   * session.updateQueryTag("{\"key2\":\"value2\"}");
   * System.out.println(session.getQueryTag().get());
   * {"key1":"value1","key2":"value2"}
   * }</pre>
   *
   * <p>Example 3:
   *
   * <pre>{@code
   * session.setQueryTag("");
   * session.updateQueryTag("{\"key1\":\"value1\"}");
   * System.out.println(session.getQueryTag().get());
   * {"key1":"value1"}
   * }</pre>
   *
   * @param queryTag A JSON encoded string that provides updates to the current query tag.
   * @throws SnowparkClientException If the provided query tag or the query tag of the current
   *     session are not valid JSON strings; or if it could not serialize the query tag into a JSON
   *     string.
   * @since 1.13.0
   */
  public void updateQueryTag(String queryTag) throws SnowparkClientException {
    session.updateQueryTag(queryTag);
  }

  /**
   * Creates a new DataFrame via Generator function.
   *
   * <p>For example:
   *
   * <pre>{@code
   * import com.snowflake.snowpark_java.Functions;
   * DataFrame df = session.generator(10, Functions.seq4(),
   *   Functions.uniform(Functions.lit(1), Functions.lit(4), Functions.random()));
   * }</pre>
   *
   * @param rowCount The row count of the result DataFrame.
   * @param columns the column list of the result DataFrame
   * @return A DataFrame
   * @since 0.12.0
   */
  public DataFrame generator(long rowCount, Column... columns) {
    return new DataFrame(
        session.generator(
            rowCount, JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(columns))));
  }

  /**
   * Returns the name of the default database configured for this session in {@code
   * Session.builder}.
   *
   * @return The name of the default database
   * @since 0.12.0
   */
  public Optional<String> getDefaultDatabase() {
    scala.Option<String> result = session.getDefaultDatabase();
    if (result.isDefined()) {
      return Optional.of(result.get());
    }
    return Optional.empty();
  }

  /**
   * Returns the name of the default schema configured for this session in {@code Session.builder}.
   *
   * @return The name of the default schema
   * @since 0.12.0
   */
  public Optional<String> getDefaultSchema() {
    scala.Option<String> result = session.getDefaultSchema();
    if (result.isDefined()) {
      return Optional.of(result.get());
    }
    return Optional.empty();
  }

  /**
   * Returns the name of the current database for the JDBC session attached to this session.
   *
   * <p>For example, if you change the current database by executing the following code:
   *
   * <p>{@code session.sql("use database newDB").collect();}
   *
   * <p>the method returns `newDB`.
   *
   * @return The name of the current database for this session.
   * @since 0.12.0
   */
  public Optional<String> getCurrentDatabase() {
    scala.Option<String> result = session.getCurrentDatabase();
    if (result.isDefined()) {
      return Optional.of(result.get());
    }
    return Optional.empty();
  }

  /**
   * Returns the name of the current schema for the JDBC session attached to this session.
   *
   * <p>For example, if you change the current schema by executing the following code:
   *
   * <p>{@code session.sql("use schema newSchema").collect();}
   *
   * <p>the method returns `newSchema`.
   *
   * @return Current schema in session.
   * @since 0.12.0
   */
  public Optional<String> getCurrentSchema() {
    scala.Option<String> result = session.getCurrentSchema();
    if (result.isDefined()) {
      return Optional.of(result.get());
    }
    return Optional.empty();
  }

  /**
   * Returns the fully qualified name of the current schema for the session.
   *
   * @return The fully qualified name of the schema
   * @since 0.12.0
   */
  public String getFullyQualifiedCurrentSchema() {
    return session.getFullyQualifiedCurrentSchema();
  }

  /**
   * Returns the query tag that you set by calling {@code setQueryTag}.
   *
   * @since 0.12.0
   * @return The current query tag
   */
  public Optional<String> getQueryTag() {
    scala.Option<String> result = session.getQueryTag();
    if (result.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(result.get());
  }

  /**
   * Returns the name of the temporary stage created by the Snowpark library for uploading and store
   * temporary artifacts for this session. These artifacts include classes for UDFs that you define
   * in this session and dependencies that you add when calling {@code addDependency}.
   *
   * @return The name of stage.
   * @since 0.12.0
   */
  public String getSessionStage() {
    return session.getSessionStage();
  }

  /**
   * Creates a new DataFrame by flattening compound values into multiple rows.
   *
   * <p>For example:
   *
   * <pre>{@code
   * import com.snowflake.snowpark_java.Functions;
   * DataFrame df = session.flatten(Functions.parse_json(Functions.lit("{\"a\":[1,2]}")));
   * }</pre>
   *
   * @param input The expression that will be unseated into rows. The expression must be of data
   *     type VARIANT, OBJECT, or ARRAY.
   * @return A DataFrame.
   * @since 0.12.0
   */
  public DataFrame flatten(Column input) {
    return new DataFrame(session.flatten(input.toScalaColumn()));
  }

  /**
   * Creates a new DataFrame by flattening compound values into multiple rows.
   *
   * <p>for example:
   *
   * <pre>{@code
   * import com.snowflake.snowpark_java.Functions;
   * DataFrame df = session.flatten(Functions.parse_json(Functions.lit("{\"a\":[1,2]}")),
   *   "a", false. false, "BOTH");
   * }</pre>
   *
   * @param input The expression that will be unseated into rows. The expression must be of data
   *     type VARIANT, OBJECT, or ARRAY.
   * @param path The path to the element within a VARIANT data structure which needs to be
   *     flattened. Can be a zero-length string (i.e. empty path) if the outermost element is to be
   *     flattened.
   * @param outer If {@code false}, any input rows that cannot be expanded, either because they
   *     cannot be accessed in the path or because they have zero fields or entries, are completely
   *     omitted from the output. Otherwise, exactly one row is generated for zero-row expansions
   *     (with NULL in the KEY, INDEX, and VALUE columns).
   * @param recursive If {@code false}, only the element referenced by PATH is expanded. Otherwise,
   *     the expansion is performed for all sub-elements recursively.
   * @param mode Specifies which types should be flattened ({@code "OBJECT"}, {@code "ARRAY"}, or
   *     {@code "BOTH"}).
   * @since 0.12.0
   * @return A DataFrame.
   */
  public DataFrame flatten(
      Column input, String path, boolean outer, boolean recursive, String mode) {
    return new DataFrame(session.flatten(input.toScalaColumn(), path, outer, recursive, mode));
  }

  /**
   * Close this session.
   *
   * @since 0.12.0
   */
  public void close() {
    session.close();
  }

  /**
   * Get the session information.
   *
   * @since 0.12.0
   * @return Session info
   */
  public String getSessionInfo() {
    return session.getSessionInfo();
  }

  /**
   * Returns a DataFrameReader that you can use to read data from various supported sources (e.g. a
   * file in a stage) as a DataFrame.
   *
   * @return A DataFrameReader
   * @since 1.1.0
   */
  public DataFrameReader read() {
    return new DataFrameReader(this.session.read());
  }

  /**
   * Returns a FileOperation object that you can use to perform file operations on stages.
   *
   * @return A FileOperation object
   * @since 1.2.0
   */
  public FileOperation file() {
    return new FileOperation(this.session.file());
  }

  /**
   * Creates a new DataFrame from the given table function and arguments.
   *
   * <p>Example
   *
   * <pre>{@code
   * session.tableFunction(TableFunctions.split_to_table(),
   *   Functions.lit("split by space"), Functions.lit(" "));
   * }</pre>
   *
   * @since 1.2.0
   * @param func Table function object, can be created from TableFunction class or referred from the
   *     built-in list from tableFunctions.
   * @param args The arguments of the given table function.
   * @return The result DataFrame
   */
  public DataFrame tableFunction(TableFunction func, Column... args) {
    return new DataFrame(
        session.tableFunction(
            func.getScalaTableFunction(),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(args))));
  }

  /**
   * Creates a new DataFrame from the given table function and arguments.
   *
   * <p>Example
   *
   * <pre>{@code
   * Map<String, Column> args = new HashMap<>();
   * args.put("input", Functions.parse_json(Functions.lit("[1,2]")));
   * session.tableFunction(TableFunctions.flatten(), args);
   * }</pre>
   *
   * @since 1.2.0
   * @param func Table function object, can be created from TableFunction class or referred from the
   *     built-in list from tableFunctions.
   * @param args function arguments map of the given table function. Some functions, like flatten,
   *     have named parameters. use this map to assign values to the corresponding parameters.
   * @return The result DataFrame
   */
  public DataFrame tableFunction(TableFunction func, Map<String, Column> args) {
    Map<String, com.snowflake.snowpark.Column> scalaArgs = new HashMap<>();
    for (Map.Entry<String, Column> entry : args.entrySet()) {
      scalaArgs.put(entry.getKey(), entry.getValue().toScalaColumn());
    }
    return new DataFrame(
        session.tableFunction(
            func.getScalaTableFunction(), JavaUtils.javaStringColumnMapToScala(scalaArgs)));
  }

  /**
   * Creates a new DataFrame from the given table function and arguments.
   *
   * <p>Example
   *
   * <pre>{@code
   * session.tableFunction(TableFunctions.flatten(
   *   Functions.parse_json(df.col("col")),
   *   "path", true, true, "both"
   * ));
   * }</pre>
   *
   * @since 1.10.0
   * @param func Column object, which can be one of the values in the TableFunctions class or an
   *     object that you create from the `new TableFunction("name").call()`.
   * @return The result DataFrame
   */
  public DataFrame tableFunction(Column func) {
    return new DataFrame(session.tableFunction(func.toScalaColumn()));
  }

  /**
   * Returns a SProcRegistration object that you can use to register Stored Procedures.
   *
   * @since 1.8.0
   * @return A SProcRegistration object that provides methods for registering SProcs.
   */
  @PublicPreview
  public SProcRegistration sproc() {
    if (sproc == null) {
      sproc = new SProcRegistration(this, session.sproc());
    }
    return sproc;
  }
  /**
   * Creates a new DataFrame from the given Stored Procedure and arguments.
   *
   * <p>Example
   *
   * <pre>{@code
   * session.storedProcedure("sp_name", "arg1", "arg2").show()
   * }</pre>
   *
   * @since 1.8.0
   * @param spName The name of stored procedures.
   * @param args The arguments of the given stored procedure
   * @return The result DataFrame
   */
  @PublicPreview
  public DataFrame storedProcedure(String spName, Object... args) {
    return new DataFrame(session.storedProcedure(spName, JavaUtils.objectArrayToSeq(args)));
  }

  /**
   * Creates a new DataFrame from the given Stored Procedure and arguments.
   *
   * <p>todo: add example in snow-683653
   *
   * @since 1.8.0
   * @param sp The stored procedure object, can be created by {@code Session.sproc().register}
   *     methods.
   * @param args The arguments of the given stored procedure
   * @return The result DataFrame
   */
  @PublicPreview
  public DataFrame storedProcedure(StoredProcedure sp, Object... args) {
    return new DataFrame(session.storedProcedure(sp.sp, JavaUtils.objectArrayToSeq(args)));
  }

  /**
   * Returns an AsyncJob object that you can use to track the status and get the results of the
   * asynchronous query specified by the query ID.
   *
   * <p>For example, create an AsyncJob by specifying a valid `query_id`, check whether the query is
   * running or not, and get the result rows.
   *
   * <pre>{@code
   * AsyncJob job = session.createAsyncJob(id);
   * Row[] result = job.getRows();
   * }</pre>
   *
   * @since 1.2.0
   * @param queryID A valid query ID
   * @return An AsyncJob object
   */
  public AsyncJob createAsyncJob(String queryID) {
    return new AsyncJob(this.session.createAsyncJob(queryID), this.session);
  }

  com.snowflake.snowpark.Session getScalaSession() {
    return session;
  }
}
