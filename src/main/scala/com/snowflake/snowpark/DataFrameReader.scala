package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer.StagedFileReader
import com.snowflake.snowpark.types.StructType

// scalastyle:off
/**
 * Provides methods to load data in various supported formats from a Snowflake stage to a DataFrame.
 * The paths provided to the DataFrameReader must refer to Snowflake stages.
 *
 * To use this object:
 *
 *  1. Access an instance of a DataFrameReader by calling the [[Session.read]] method.
 *  1. Specify any
 *     [[https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions format-specific options]]
 *     and
 *     [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions copy options]]
 *     by calling the [[option]] or [[options]] method. These methods return a DataFrameReader that
 *     is configured with these options. (Note that although specifying copy options can make error
 *     handling more robust during the reading process, it may have an effect on performance.)
 *  1. Specify the schema of the data that you plan to load by constructing a [[types.StructType]]
 *     object and passing it to the [[schema]] method. This method returns a DataFrameReader that
 *     is configured to read data that uses the specified schema.
 *  1. Specify the format of the data by calling the method named after the format (e.g. [[csv]],
 *     [[json]], etc.). These methods return a [[DataFrame]] that is configured to load data in the
 *     specified format.
 *  1. Call a [[DataFrame]] method that performs an action.
 *     - For example, to load the data from the file, call [[DataFrame.collect]].
 *     - As another example, to save the data from the file to a table, call [[CopyableDataFrame.copyInto(tableName:String)*]].
 *       This uses the COPY INTO `<table_name>` command.
 *
 * The following examples demonstrate how to use a DataFrameReader.
 *
 * '''Example 1:''' Loading the first two columns of a CSV file and skipping the first header line.
 * {{{
 *   // Import the package for StructType.
 *   import com.snowflake.snowpark.types._
 *   val filePath = "@mystage1"
 *   // Define the schema for the data in the CSV file.
 *   val userSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
 *   // Create a DataFrame that is configured to load data from the CSV file.
 *   val csvDF = session.read.option("skip_header", 1).schema(userSchema).csv(filePath)
 *   // Load the data into the DataFrame and return an Array of Rows containing the results.
 *   val results = csvDF.collect()
 * }}}
 *
 * '''Example 2:''' Loading a gzip compressed json file.
 * {{{
 *   val filePath = "@mystage2/data.json.gz"
 *   // Create a DataFrame that is configured to load data from the gzipped JSON file.
 *   val jsonDF = session.read.option("compression", "gzip").json(filePath)
 *   // Load the data into the DataFrame and return an Array of Rows containing the results.
 *   val results = jsonDF.collect()
 * }}}
 *
 * If you want to load only a subset of files from the stage, you can use the
 * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching pattern]]
 * option to specify a regular expression that matches the files that you want to load.
 *
 * '''Example 3:''' Loading only the CSV files from a stage location.
 * {{{
 *   import com.snowflake.snowpark.types._
 *   // Define the schema for the data in the CSV files.
 *   val userSchema: StructType = StructType(Seq(StructField("a", IntegerType),StructField("b", StringType)))
 *   // Create a DataFrame that is configured to load data from the CSV files in the stage.
 *   val csvDF = session.read.option("pattern", ".*[.]csv").schema(userSchema).csv("@stage_location")
 *   // Load the data into the DataFrame and return an Array of Rows containing the results.
 *   val results = csvDF.collect()
 * }}}
 *
 * In addition, if you want to load the files from the stage into a specified table with COPY INTO
 * `<table_name>` command, you can use a `copyInto()` method e.g.
 * [[CopyableDataFrame.copyInto(tableName:String)*]].
 *
 * '''Example 4:''' Loading data from a JSON file in a stage to a table by using COPY INTO `<table_name>`.
 * {{{
 *   val filePath = "@mystage1"
 *   // Create a DataFrame that is configured to load data from the JSON file.
 *   val jsonDF = session.read.json(filePath)
 *   // Load the data into the specified table `T1`.
 *   // The table "T1" should exist before calling copyInto().
 *   jsonDF.copyInto("T1")
 * }}}
 *
 * @param session Snowflake [[Session]]
 * @since 0.1.0
 */
// scalastyle:on
class DataFrameReader(session: Session) {

  private val stagedFileReader = new StagedFileReader(session)

  /**
   * Returns a [[DataFrame]] that is set up to load data from the specified table.
   *
   * For the {@code name} argument, you can specify an unqualified name (if the table is in the
   * current database and schema) or a fully qualified name (`db.schema.name`).
   *
   * Note that the data is not loaded in the DataFrame until you call a method that performs
   * an action (e.g. [[DataFrame.collect]], [[DataFrame.count]], etc.).
   *
   * @since 0.1.0
   * @param name Name of the table to use.
   * @return A [[DataFrame]]
   */
  def table(name: String): DataFrame = session.table(name)

  /**
   * Returns a DataFrameReader instance with the specified schema configuration for the data to be
   * read.
   *
   * To define the schema for the data that you want to read, use a [[types.StructType]] object.
   *
   * @since 0.1.0
   * @param schema Schema configuration for the data to be read.
   * @return A [[DataFrameReader]]
   */
  def schema(schema: StructType): DataFrameReader = {
    stagedFileReader.userSchema(schema)
    this
  }

  /**
   * Returns a [[CopyableDataFrame]] that is set up to load data from the specified CSV file.
   *
   * This method only supports reading data from files in Snowflake stages.
   *
   * Note that the data is not loaded in the DataFrame until you call a method that performs
   * an action (e.g. [[DataFrame.collect]], [[DataFrame.count]], etc.).
   *
   * For example:
   * {{{
   *   val filePath = "@mystage1/myfile.csv"
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.schema(userSchema).csv(fileInAStage).filter(col("a") < 2)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * If you want to use the `COPY INTO <table_name>` command to load data from staged files to
   * a specified table, call the `copyInto()` method (e.g.
   * [[CopyableDataFrame.copyInto(tableName:String)*]]).
   *
   * For example: The following example loads the CSV files in the stage location specified by
   * `path` to the table `T1`.
   * {{{
   *   // The table "T1" should exist before calling copyInto().
   *   session.read.schema(userSchema).csv(path).copyInto("T1")
   * }}}
   *
   * @since 0.1.0
   * @param path The path to the CSV file (including the stage name).
   * @return A [[CopyableDataFrame]]
   */
  def csv(path: String): CopyableDataFrame = {
    stagedFileReader
      .path(path)
      .format("csv")
      .databaseSchema(session.getFullyQualifiedCurrentSchema)
    new CopyableDataFrame(session, stagedFileReader.createSnowflakePlan(), stagedFileReader)
  }

  /**
   * Returns a [[DataFrame]] that is set up to load data from the specified JSON file.
   *
   * This method only supports reading data from files in Snowflake stages.
   *
   * Note that the data is not loaded in the DataFrame until you call a method that performs
   * an action (e.g. [[DataFrame.collect]], [[DataFrame.count]], etc.).
   *
   * For example:
   * {{{
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.json(path).where(col("\$1:num") > 1)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * If you want to use the `COPY INTO <table_name>` command to load data from staged files to
   * a specified table, call the `copyInto()` method (e.g.
   * [[CopyableDataFrame.copyInto(tableName:String)*]]).
   *
   * For example: The following example loads the JSON files in the stage location specified by
   * `path` to the table `T1`.
   * {{{
   *   // The table "T1" should exist before calling copyInto().
   *   session.read.json(path).copyInto("T1")
   * }}}
   *
   * @since 0.1.0
   * @param path The path to the JSON file (including the stage name).
   * @return A [[CopyableDataFrame]]
   */
  def json(path: String): CopyableDataFrame = readSemiStructuredFile(path, "JSON")

  /**
   * Returns a [[DataFrame]] that is set up to load data from the specified Avro file.
   *
   * This method only supports reading data from files in Snowflake stages.
   *
   * Note that the data is not loaded in the DataFrame until you call a method that performs
   * an action (e.g. [[DataFrame.collect]], [[DataFrame.count]], etc.).
   *
   * For example:
   * {{{
   *   session.read.avro(path).where(col("\$1:num") > 1)
   * }}}
   *
   * If you want to use the `COPY INTO <table_name>` command to load data from staged files to
   * a specified table, call the `copyInto()` method (e.g.
   * [[CopyableDataFrame.copyInto(tableName:String)*]]).
   *
   * For example: The following example loads the Avro files in the stage location specified by
   * `path` to the table `T1`.
   * {{{
   *   // The table "T1" should exist before calling copyInto().
   *   session.read.avro(path).copyInto("T1")
   * }}}
   *
   * @since 0.1.0
   * @param path The path to the Avro file (including the stage name).
   * @return A [[CopyableDataFrame]]
   */
  def avro(path: String): CopyableDataFrame = readSemiStructuredFile(path, "AVRO")

  /**
   * Returns a [[DataFrame]] that is set up to load data from the specified Parquet file.
   *
   * This method only supports reading data from files in Snowflake stages.
   *
   * Note that the data is not loaded in the DataFrame until you call a method that performs
   * an action (e.g. [[DataFrame.collect]], [[DataFrame.count]], etc.).
   *
   * For example:
   * {{{
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.parquet(path).where(col("\$1:num") > 1)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * If you want to use the `COPY INTO <table_name>` command to load data from staged files to
   * a specified table, call the `copyInto()` method (e.g.
   * [[CopyableDataFrame.copyInto(tableName:String)*]]).
   *
   * For example: The following example loads the Parquet files in the stage location specified by
   * `path` to the table `T1`.
   * {{{
   *   // The table "T1" should exist before calling copyInto().
   *   session.read.parquet(path).copyInto("T1")
   * }}}
   *
   * @since 0.1.0
   * @param path The path to the Parquet file (including the stage name).
   * @return A [[CopyableDataFrame]]
   */
  def parquet(path: String): CopyableDataFrame = readSemiStructuredFile(path, "PARQUET")

  /**
   * Returns a [[DataFrame]] that is set up to load data from the specified ORC file.
   *
   * This method only supports reading data from files in Snowflake stages.
   *
   * Note that the data is not loaded in the DataFrame until you call a method that performs
   * an action (e.g. [[DataFrame.collect]], [[DataFrame.count]], etc.).
   *
   * For example:
   * {{{
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.orc(path).where(col("\$1:num") > 1)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * If you want to use the `COPY INTO <table_name>` command to load data from staged files to
   * a specified table, call the `copyInto()` method (e.g.
   * [[CopyableDataFrame.copyInto(tableName:String)*]]).
   *
   * For example: The following example loads the ORC files in the stage location specified by
   * `path` to the table `T1`.
   * {{{
   *   // The table "T1" should exist before calling copyInto().
   *   session.read.orc(path).copyInto("T1")
   * }}}
   *
   * @since 0.1.0
   * @param path The path to the ORC file (including the stage name).
   * @return A [[CopyableDataFrame]]
   */
  def orc(path: String): CopyableDataFrame = readSemiStructuredFile(path, "ORC")

  /**
   * Returns a [[DataFrame]] that is set up to load data from the specified XML file.
   *
   * This method only supports reading data from files in Snowflake stages.
   *
   * Note that the data is not loaded in the DataFrame until you call a method that performs
   * an action (e.g. [[DataFrame.collect]], [[DataFrame.count]], etc.).
   *
   * For example:
   * {{{
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.xml(path).where(col("xmlget(\$1, 'num', 0):\"$\"") > 1)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * If you want to use the `COPY INTO <table_name>` command to load data from staged files to
   * a specified table, call the `copyInto()` method (e.g.
   * [[CopyableDataFrame.copyInto(tableName:String)*]]).
   *
   * For example: The following example loads the XML files in the stage location specified by
   * `path` to the table `T1`.
   * {{{
   *   // The table "T1" should exist before calling copyInto().
   *   session.read.xml(path).copyInto("T1")
   * }}}
   *
   * @since 0.1.0
   * @param path The path to the XML file (including the stage name).
   * @return A [[CopyableDataFrame]]
   */
  def xml(path: String): CopyableDataFrame = readSemiStructuredFile(path, "XML")

  // scalastyle:off
  /**
   * Sets the specified option in the DataFrameReader.
   *
   * Use this method to configure any
   * [[https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions format-specific options]]
   * and
   * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions copy options]].
   * (Note that although specifying copy options can make error handling more robust during the
   * reading process, it may have an effect on performance.)
   *
   * '''Example 1:''' Loading a LZO compressed Parquet file.
   * {{{
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.option("compression", "lzo").parquet(filePath)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * '''Example 2:''' Loading an uncompressed JSON file.
   * {{{
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.option("compression", "none").json(filePath)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * '''Example 3:''' Loading the first two columns of a colon-delimited CSV file in which the
   * first line is the header:
   * {{{
   *   import com.snowflake.snowpark.types._
   *   // Define the schema for the data in the CSV files.
   *   val userSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
   *   // Create a DataFrame that is configured to load data from the CSV file.
   *   val csvDF = session.read.option("field_delimiter", ":").option("skip_header", 1).schema(userSchema).csv(filePath)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = csvDF.collect()
   * }}}
   *
   * In addition, if you want to load only a subset of files from the stage, you can use the
   * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching pattern]]
   * option to specify a regular expression that matches the files that you want to load.
   *
   * '''Example 4:''' Loading only the CSV files from a stage location.
   * {{{
   *   import com.snowflake.snowpark.types._
   *   // Define the schema for the data in the CSV files.
   *   val userSchema: StructType = StructType(Seq(StructField("a", IntegerType),StructField("b", StringType)))
   *   // Create a DataFrame that is configured to load data from the CSV files in the stage.
   *   val csvDF = session.read.option("pattern", ".*[.]csv").schema(userSchema).csv("@stage_location")
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = csvDF.collect()
   * }}}
   *
   * @since 0.1.0
   * @param key Name of the option (e.g. {@code compression}, {@code skip_header}, etc.).
   * @param value Value of the option.
   * @return A [[DataFrameReader]]
   */
  // scalastyle:on
  def option(key: String, value: Any): DataFrameReader = {
    stagedFileReader.option(key, value)
    this
  }
  // scalastyle:off
  /**
   * Sets multiple specified options in the DataFrameReader.
   *
   * Use this method to configure any
   * [[https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions format-specific options]]
   * and
   * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions copy options]].
   * (Note that although specifying copy options can make error handling more robust during the
   * reading process, it may have an effect on performance.)
   *
   * In addition, if you want to load only a subset of files from the stage, you can use the
   * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching pattern]]
   * option to specify a regular expression that matches the files that you want to load.
   *
   * '''Example 1:''' Loading a LZO compressed Parquet file and removing any white space from the
   * fields.
   *
   * {{{
   *   // Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
   *   val df = session.read.option(Map("compression"-> "lzo", "trim_space" -> true)).parquet(filePath)
   *   // Load the data into the DataFrame and return an Array of Rows containing the results.
   *   val results = df.collect()
   * }}}
   *
   * @since 0.1.0
   * @param configs Map of the names of options (e.g. {@code compression}, {@code skip_header},
   *   etc.) and their corresponding values.
   * @return A [[DataFrameReader]]
   */
  // scalastyle:on
  def options(configs: Map[String, Any]): DataFrameReader = {
    stagedFileReader.options(configs)
    this
  }

  private def readSemiStructuredFile(path: String, format: String): CopyableDataFrame = {
    stagedFileReader
      .path(path)
      .format(format)
      .databaseSchema(session.getFullyQualifiedCurrentSchema)
    new CopyableDataFrame(session, stagedFileReader.createSnowflakePlan(), stagedFileReader)
  }
}
