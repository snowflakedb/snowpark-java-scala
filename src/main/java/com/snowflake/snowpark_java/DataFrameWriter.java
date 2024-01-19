package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * Provides methods for writing data from a DataFrame to supported output destinations. You can
 * write data to the following locations:
 *
 * <ul>
 *   <li>A Snowflake table
 *   <li>A file on a stage
 * </ul>
 *
 * <h2>Saving Data to a Table</h2>
 *
 * To use this object to write into a table:
 *
 * <ul>
 *   <li>Access an instance of a DataFrameWriter by calling the {@code DataFrame.write} method.
 *   <li>Specify the save mode to use (overwrite or append) by calling the {@code mode()} method.
 *       This method returns a DataFrameWriter that is configured to save data using the specified
 *       mode. The default {@code SaveMode} is {@code SaveMode.Append}.
 *   <li>(Optional) If you need to set some options for the save operation (e.g. columnOrder), call
 *       the {@code options} or {@code option} method.
 *   <li>Call a {@code saveAsTable} method to save the data to the specified destination.
 * </ul>
 *
 * <h2>Saving Data to a File on a Stage</h2>
 *
 * To use this object to write into a file:
 *
 * <ul>
 *   <li>Access an instance of a DataFrameWriter by calling the {@code DataFrame.write} method.
 *   <li>Specify the save mode to use (Overwrite or ErrorIfExists) by calling the {@code
 *       DataFrame.mode} method. This method returns a DataFrameWriter that is configured to save
 *       data using the specified mode. The default {@code SaveMode} is {@code
 *       SaveMode.ErrorIfExists} for this case.
 *   <li>(Optional) If you need to set some options for the save operation (e.g. file format
 *       options), call the {@code options} or {@code option} method.
 *   <li>Call the method named after a file format to save the data in the specified format:
 *       <ul>
 *         <li>To save the data in CSV format, call the {@code csv} method.
 *         <li>To save the data in JSON format, call the {@code json} method.
 *         <li>To save the data in PARQUET format, call the {@code parquet} method.
 *       </ul>
 * </ul>
 *
 * @since 1.1.0
 */
public class DataFrameWriter {
  private final com.snowflake.snowpark.DataFrameWriter writer;
  private final com.snowflake.snowpark.Session session;

  DataFrameWriter(
      com.snowflake.snowpark.DataFrameWriter writer, com.snowflake.snowpark.Session session) {
    this.writer = writer;
    this.session = session;
  }

  /**
   * Sets the specified option in the DataFrameWriter.
   *
   * <p><b>Sets the specified option for saving data to a table</b>
   *
   * <p>Use this method to configure options:
   *
   * <ul>
   *   <li>columnOrder: save data into a table with table's column name order if saveMode is Append
   *       and the target table exists.
   * </ul>
   *
   * <b>Sets the specified option for saving data to a file on a stage</b>
   *
   * <p>Use this method to configure options:
   *
   * <ul>
   *   <li><a
   *       href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions">format-specific
   *       options</a>
   *   <li><a
   *       href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions">copy
   *       options</a>
   *   <li><a
   *       href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#optional-parameters">PARTITION
   *       BY or HEADER</a>
   * </ul>
   *
   * <p>Note that you cannot use the {@code options} or {@code option} methods to set the following
   * options:
   *
   * <ul>
   *   <li>The TYPE format type option.
   *   <li>The OVERWRITE copy option. To set this option, use the {@code mode} method instead.
   *       <ul>
   *         <li>To set OVERWRITE to TRUE, use {@code SaveMode.Overwrite}.
   *         <li>To set OVERWRITE to FALSE, use {@code SaveMode.ErrorIfExists}.
   *       </ul>
   * </ul>
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.write().option("compression", "none").csv("@myStage/prefix");
   * }</pre>
   *
   * @since 1.4.0
   * @param key Name of the option.
   * @param value Value of the option.
   * @return A {@code DataFrameWriter}
   */
  public DataFrameWriter option(String key, Object value) {
    this.writer.option(key, value);
    return this;
  }

  /**
   * Sets multiple specified options in the DataFrameWriter.
   *
   * <p><b>Sets the specified option for saving data to a table</b>
   *
   * <p>Use this method to configure options:
   *
   * <ul>
   *   <li>columnOrder: save data into a table with table's column name order if saveMode is Append
   *       and the target table exists.
   * </ul>
   *
   * <b>Sets the specified option for saving data to a file on a stage</b>
   *
   * <p>Use this method to configure options:
   *
   * <ul>
   *   <li><a
   *       href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions">format-specific
   *       options</a>
   *   <li><a
   *       href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions">copy
   *       options</a>
   *   <li><a
   *       href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#optional-parameters">PARTITION
   *       BY or HEADER</a>
   * </ul>
   *
   * <p>Note that you cannot use the {@code options} or {@code option} methods to set the following
   * options:
   *
   * <ul>
   *   <li>The TYPE format type option.
   *   <li>The OVERWRITE copy option. To set this option, use the {@code mode} method instead.
   *       <ul>
   *         <li>To set OVERWRITE to TRUE, use {@code SaveMode.Overwrite}.
   *         <li>To set OVERWRITE to FALSE, use {@code SaveMode.ErrorIfExists}.
   *       </ul>
   * </ul>
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, Object> configs = new HashMap<>();
   * configs.put("compression", "none");
   * df.write().options(configs).csv("@myStage/prefix");
   * }</pre>
   *
   * @since 1.5.0
   * @param configs Map of the names of options (e.g. {@code compression}, etc.) and their
   *     corresponding values.
   * @return A {@code DataFrameWriter}
   */
  public DataFrameWriter options(Map<String, Object> configs) {
    this.writer.options(JavaUtils.javaStringAnyMapToScala(configs));
    return this;
  }

  /**
   * Saves the contents of the DataFrame to a CSV file on a stage.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.write().option("compression", "none").csv("@myStage/prefix");
   * }</pre>
   *
   * @param path The path (including the stage name) to the CSV file.
   * @return A {@code WriteFileResult}
   * @since 1.5.0
   */
  public WriteFileResult csv(String path) {
    return new WriteFileResult(this.writer.csv(path));
  }

  private Row[] scalaRowArrayToJava(com.snowflake.snowpark.Row[] rows) {
    Row[] result = new Row[rows.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = new Row(rows[i]);
    }
    return result;
  }

  /**
   * Saves the contents of the DataFrame to a JSON file on a stage.
   *
   * <p>NOTE: You can call this method only on a DataFrame that contains a column of the type
   * Variant, Array, or Map. If the DataFrame does not contain a column of one of these types, you
   * must call the {@code to_variant}, {@code array_construct}, or {@code object_construct} to
   * return a DataFrame that contains a column of one of these types.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.write().option("compression", "none").json("@myStage/prefix");
   * }</pre>
   *
   * @param path The path (including the stage name) to the JSON file.
   * @return A {@code WriteFileResult}
   * @since 1.5.0
   */
  public WriteFileResult json(String path) {
    return new WriteFileResult(this.writer.json(path));
  }

  /**
   * Saves the contents of the DataFrame to a Parquet file on a stage.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.write().option("compression", "lzo").parquet("@myStage/prefix");
   * }</pre>
   *
   * @param path The path (including the stage name) to the Parquet file.
   * @return A {@code WriteFileResult}
   * @since 1.5.0
   */
  public WriteFileResult parquet(String path) {
    return new WriteFileResult(this.writer.parquet(path));
  }

  /**
   * Writes the data to the specified table in a Snowflake database. {@code tableName} can be a
   * fully-qualified object identifier.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.write().saveAsTable("db1.public_schema.table1");
   * }</pre>
   *
   * @param tableName Name of the table where the data should be saved.
   * @since 1.1.0
   */
  public void saveAsTable(String tableName) {
    this.writer.saveAsTable(tableName);
  }

  /**
   * Writes the data to the specified table in a Snowflake database.
   *
   * <p>For example:
   *
   * <pre>{@code
   * df.write().saveAsTable(new String[]{"db_name", "schema_name", "table_name"})
   * }</pre>
   *
   * @param multipartIdentifier A sequence of strings that specify the database name, schema name,
   *     and table name.
   * @since 1.1.0
   */
  public void saveAsTable(String[] multipartIdentifier) {
    this.writer.saveAsTable(JavaUtils.stringArrayToStringSeq(multipartIdentifier));
  }

  /**
   * Returns a new DataFrameWriter with the specified save mode configuration.
   *
   * @param saveMode One of the following strings: `"APPEND"`, `"OVERWRITE"`, `"ERRORIFEXISTS"`, or
   *     `"IGNORE"`
   * @since 1.1.0
   * @return This DataFrameWriter
   */
  public DataFrameWriter mode(String saveMode) {
    this.writer.mode(saveMode);
    return this;
  }

  /**
   * Returns a new DataFrameWriter with the specified save mode configuration.
   *
   * @param saveMode One of the following save modes: {@code SaveMode.Append}, {@code
   *     SaveMode.Overwrite}, {@code SaveMode.ErrorIfExists}, {@code SaveMode.Ignore}
   * @since 1.1.0
   * @return This DataFrameWriter
   */
  public DataFrameWriter mode(SaveMode saveMode) {
    this.writer.mode(JavaUtils.javaSaveModeToScala(saveMode));
    return this;
  }

  /**
   * Returns a DataFrameWriterAsyncActor object that can be used to execute DataFrameWriter actions
   * asynchronously.
   *
   * @since 1.2.0
   * @return A DataFrameWriterAsyncActor object
   */
  public DataFrameWriterAsyncActor async() {
    return new DataFrameWriterAsyncActor(this.writer.async(), session);
  }
}
