package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import com.snowflake.snowpark_java.types.StructType;
import java.util.Map;

/**
 * Provides methods to load data in various supported formats from a Snowflake stage to a DataFrame.
 * The paths provided to the DataFrameReader must refer to Snowflake stages.
 *
 * @since 1.1.0
 */
public class DataFrameReader {

  private final com.snowflake.snowpark.DataFrameReader reader;

  DataFrameReader(com.snowflake.snowpark.DataFrameReader reader) {
    this.reader = reader;
  }

  /**
   * Returns a DataFrame that is set up to load data from the specified table.
   *
   * <p>For the {@code name} argument, you can specify an unqualified name (if the table is in the
   * current database and schema) or a fully qualified name (`db.schema.name`).
   *
   * <p>Note that the data is not loaded in the DataFrame until you call a method that performs an
   * action (e.g. {@code DataFrame.collect}, {@code DataFrame.count}, etc.).
   *
   * @since 1.1.0
   * @param name Name of the table to use.
   * @return A new DataFrame
   */
  public DataFrame table(String name) {
    return new DataFrame(reader.table(name));
  }

  /**
   * Returns a DataFrameReader instance with the specified schema configuration for the data to be
   * read.
   *
   * <p>To define the schema for the data that you want to read, use a {@code types.StructType}
   * object.
   *
   * @since 1.1.0
   * @param schema Schema configuration for the data to be read.
   * @return A reference of this DataFrameReader
   */
  public DataFrameReader schema(StructType schema) {
    this.reader.schema(com.snowflake.snowpark_java.types.InternalUtils.toScalaStructType(schema));
    return this;
  }

  /**
   * Returns a CopyableDataFrame that is set up to load data from the specified CSV file.
   *
   * <p>This method only supports reading data from files in Snowflake stages.
   *
   * <p>Note that the data is not loaded in the DataFrame until you call a method that performs an
   * action (e.g. {@code DataFrame.collect}, {@code DataFrame.count}, etc.).
   *
   * <p>For example:
   *
   * <pre>{@code
   * String filePath = "@myStage/myFile.csv";
   * DataFrame df = session.read().schema(userSchema).csv(filePath);
   * }</pre>
   *
   * If you want to use the `COPY INTO 'table_name'` command to load data from staged files to a
   * specified table, call the `copyInto()` method (e.g. {@code CopyableDataFrame.copyInto}).
   *
   * <p>For example: The following example loads the CSV files in the stage location specified by
   * `path` to the table `T1`.
   *
   * <pre>{@code
   * session.read().schema(userSchema).csv(path).copyInto("T1")
   * }</pre>
   *
   * @since 1.1.0
   * @param path The path to the CSV file (including the stage name).
   * @return A CopyableDataFrame
   */
  public CopyableDataFrame csv(String path) {
    return new CopyableDataFrame(reader.csv(path));
  }

  /**
   * Returns a CopyableDataFrame that is set up to load data from the specified JSON file.
   *
   * <p>This method only supports reading data from files in Snowflake stages.
   *
   * <p>Note that the data is not loaded in the DataFrame until you call a method that performs an
   * action (e.g. {@code DataFrame.collect}, {@code DataFrame.count}, etc.).
   *
   * <p>For example:
   *
   * <pre>{@code
   * DataFrame df = session.read().json(path);
   * }</pre>
   *
   * If you want to use the `COPY INTO 'table_name'` command to load data from staged files to a
   * specified table, call the `copyInto()` method (e.g. {@code CopyableDataFrame.copyInto}).
   *
   * <p>For example: The following example loads the json files in the stage location specified by
   * `path` to the table `T1`.
   *
   * <pre>{@code
   * session.read().json(path).copyInto("T1")
   * }</pre>
   *
   * @since 1.1.0
   * @param path The path to the JSON file (including the stage name).
   * @return A new DataFrame
   */
  public CopyableDataFrame json(String path) {
    return new CopyableDataFrame(reader.json(path));
  }

  /**
   * Returns a CopyableDataFrame that is set up to load data from the specified Avro file.
   *
   * <p>This method only supports reading data from files in Snowflake stages.
   *
   * <p>Note that the data is not loaded in the DataFrame until you call a method that performs an
   * action (e.g. {@code DataFrame.collect}, {@code DataFrame.count}, etc.).
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.read().avro(path).where(Functions.sqlExpr("$1:col").gt(Functions.lit(1)));
   * }</pre>
   *
   * If you want to use the `COPY INTO 'table_name'` command to load data from staged files to a
   * specified table, call the `copyInto()` method (e.g. {@code CopyableDataFrame.copyInto}).
   *
   * <p>For example: The following example loads the avro files in the stage location specified by
   * `path` to the table `T1`.
   *
   * <pre>{@code
   * session.read().avro(path).copyInto("T1")
   * }</pre>
   *
   * @since 1.1.0
   * @param path The path to the Avro file (including the stage name).
   * @return A new DataFrame
   */
  public CopyableDataFrame avro(String path) {
    return new CopyableDataFrame(reader.avro(path));
  }

  /**
   * Returns a CopyableDataFrame that is set up to load data from the specified Parquet file.
   *
   * <p>This method only supports reading data from files in Snowflake stages.
   *
   * <p>Note that the data is not loaded in the DataFrame until you call a method that performs an
   * action (e.g. {@code DataFrame.collect}, {@code DataFrame.count}, etc.).
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.read().parquet(path).where(Functions.sqlExpr("$1:col").gt(Functions.lit(1)));
   * }</pre>
   *
   * If you want to use the `COPY INTO 'table_name'` command to load data from staged files to a
   * specified table, call the `copyInto()` method (e.g. {@code CopyableDataFrame.copyInto}).
   *
   * <p>For example: The following example loads the parquet files in the stage location specified
   * by `path` to the table `T1`.
   *
   * <pre>{@code
   * session.read().parquet(path).copyInto("T1")
   * }</pre>
   *
   * @since 1.1.0
   * @param path The path to the Parquet file (including the stage name).
   * @return A DataFrame
   */
  public CopyableDataFrame parquet(String path) {
    return new CopyableDataFrame(reader.parquet(path));
  }

  /**
   * Returns a CopyableDataFrame that is set up to load data from the specified ORC file.
   *
   * <p>This method only supports reading data from files in Snowflake stages.
   *
   * <p>Note that the data is not loaded in the DataFrame until you call a method that performs an
   * action (e.g. {@code DataFrame.collect}, {@code DataFrame.count}, etc.).
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.read().orc(path).where(Functions.sqlExpr("$1:col").gt(Functions.lit(1)));
   * }</pre>
   *
   * If you want to use the `COPY INTO 'table_name'` command to load data from staged files to a
   * specified table, call the `copyInto()` method (e.g. {@code CopyableDataFrame.copyInto}).
   *
   * <p>For example: The following example loads the ORC files in the stage location specified by
   * `path` to the table `T1`.
   *
   * <pre>{@code
   * session.read().orc(path).copyInto("T1")
   * }</pre>
   *
   * @since 1.1.0
   * @param path The path to the ORC file (including the stage name).
   * @return A DataFrame
   */
  public CopyableDataFrame orc(String path) {
    return new CopyableDataFrame(reader.orc(path));
  }

  /**
   * Returns a CopyableDataFrame that is set up to load data from the specified XML file.
   *
   * <p>This method only supports reading data from files in Snowflake stages.
   *
   * <p>Note that the data is not loaded in the DataFrame until you call a method that performs an
   * action (e.g. {@code DataFrame.collect}, {@code DataFrame.count}, etc.).
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.read().parquet(path).where(Functions
   *   .sqlExpr("xmlget($1, 'num', 0):\"$\"").gt(Functions.lit(1)));
   * }</pre>
   *
   * If you want to use the `COPY INTO 'table_name'` command to load data from staged files to a
   * specified table, call the `copyInto()` method (e.g. {@code CopyableDataFrame.copyInto}).
   *
   * <p>For example: The following example loads the XML files in the stage location specified by
   * `path` to the table `T1`.
   *
   * <pre>{@code
   * session.read().xml(path).copyInto("T1")
   * }</pre>
   *
   * @since 1.1.0
   * @param path The path to the XML file (including the stage name).
   * @return A DataFrame
   */
  public CopyableDataFrame xml(String path) {
    return new CopyableDataFrame(reader.xml(path));
  }

  /**
   * Sets the specified option in the DataFrameReader.
   *
   * <p>Use this method to configure any <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions">format-specific
   * options</a> and <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions">copy
   * options</a> (Note that although specifying copy options can make error handling more robust
   * during the reading process, it may have an effect on performance.)
   *
   * <p>In addition, if you want to load only a subset of files from the stage, you can use the <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching">pattern</a>
   * option to specify a regular expression that matches the files that you want to load.
   *
   * <pre>{@code
   * session.read().option("field_delimiter", ";").option("skip_header", 1)
   *   .schema(schema).csv(path);
   * }</pre>
   *
   * @since 1.1.0
   * @param key Name of the option (e.g. {@code compression}, {@code skip_header}, etc.).
   * @param value Value of the option.
   * @return A reference of this DataFrameReader
   */
  public DataFrameReader option(String key, Object value) {
    this.reader.option(key, value);
    return this;
  }

  /**
   * Sets multiple specified options in the DataFrameReader.
   *
   * <p>Use this method to configure any <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions">format-specific
   * options</a> and <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions">copy
   * options</a> (Note that although specifying copy options can make error handling more robust
   * during the reading process, it may have an effect on performance.)
   *
   * <p>In addition, if you want to load only a subset of files from the stage, you can use the <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching">pattern</a>
   * option to specify a regular expression that matches the files that you want to load.
   *
   * <pre>{@code
   * Map<String, Object> configs = new HashMap<>();
   * configs.put("field_delimiter", ";");
   * configs.put("skip_header", 1);
   * session.read().options(configs).schema(schema).csv(path);
   * }</pre>
   *
   * @since 1.1.0
   * @param configs Map of the names of options (e.g. {@code compression}, {@code skip_header},
   *     etc.) and their corresponding values.
   * @return A reference of this DataFrameReader
   */
  public DataFrameReader options(Map<String, Object> configs) {
    this.reader.options(JavaUtils.javaStringAnyMapToScala(configs));
    return this;
  }
}
