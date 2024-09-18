package com.snowflake.snowpark

import com.snowflake.snowpark.internal.{ErrorMessage, OpenTelemetry, Utils}
import com.snowflake.snowpark.internal.analyzer.{
  CopyIntoLocation,
  SnowflakeCreateTable,
  SnowflakePlan,
  StagedFileWriter,
  quoteName
}
import com.snowflake.snowpark.types.StructType

import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable

/** Provides methods for writing data from a DataFrame to supported output destinations.
  *
  * You can write data to the following locations:
  *   - A Snowflake table
  *   - A file on a stage
  *
  * =Saving Data to a Table=
  * To use this object to write into a table:
  *
  *   1. Access an instance of a DataFrameWriter by calling the [[DataFrame.write]] method.
  *   1. Specify the save mode to use (overwrite or append) by calling the
  *      [[mode(saveMode:com\.snowflake\.snowpark\.SaveMode* mode]] method. This method returns a
  *      DataFrameWriter that is configured to save data using the specified mode. The default
  *      [[SaveMode]] is [[SaveMode.Append]].
  *   1. (Optional) If you need to set some options for the save operation (e.g. columnOrder), call
  *      the [[options]] or [[option]] method.
  *   1. Call a `saveAs*` method to save the data to the specified destination.
  *
  * For example:
  *
  * {{{
  *   df.write.mode("overwrite").saveAsTable("T")
  * }}}
  *
  * =Saving Data to a File on a Stage=
  * To save data to a file on a stage:
  *
  *   1. Access an instance of a DataFrameWriter by calling the [[DataFrame.write]] method.
  *   1. Specify the save mode to use (Overwrite or ErrorIfExists) by calling the
  *      [[mode(saveMode:com\.snowflake\.snowpark\.SaveMode* mode]] method. This method returns a
  *      DataFrameWriter that is configured to save data using the specified mode. The default
  *      [[SaveMode]] is [[SaveMode.ErrorIfExists]] for this case.
  *   1. (Optional) If you need to set some options for the save operation (e.g. file format
  *      options), call the [[options]] or [[option]] method.
  *   1. Call the method named after a file format to save the data in the specified format:
  *      - To save the data in CSV format, call the [[csv]] method.
  *      - To save the data in JSON format, call the [[json]] method.
  *      - To save the data in PARQUET format, call the [[parquet]] method.
  *
  * For example:
  *
  * '''Example 1:''' Write a DataFrame to a CSV file.
  * {{{
  *   val result = df.write.csv("@myStage/prefix")
  * }}}
  *
  * '''Example 2:''' Write a DataFrame to a CSV file without compression.
  * {{{
  *   val result = df.write.option("compression", "none").csv("@myStage/prefix")
  * }}}
  *
  * @param dataFrame
  *   Input [[DataFrame]]
  * @since 0.1.0
  */
class DataFrameWriter(private[snowpark] val dataFrame: DataFrame) {
  private var saveMode: Option[SaveMode] = None

  private val COLUMN_ORDER = "COLUMNORDER"
  private val writeOptions = mutable.Map[String, Any]()
  private[snowpark] def getCopyIntoLocationPlan(path: String, formatType: String): SnowflakePlan = {
    dataFrame.session.conn.telemetry.reportActionSaveAsFile(formatType)
    // The default mode for saving as a file is ErrorIfExists
    val writeFileMode = saveMode.getOrElse(SaveMode.ErrorIfExists)
    val stagedFileWriter = new StagedFileWriter(this)
      .options(this.writeOptions.toMap)
      .mode(writeFileMode)
      .format(formatType)
      .path(path)
    dataFrame.session.analyzer.resolve(CopyIntoLocation(stagedFileWriter, dataFrame.plan))
  }

  /** Saves the contents of the DataFrame to a CSV file on a stage.
    *
    * '''Example 1:''' Write a DataFrame to a CSV file.
    * {{{
    *   val result = df.write.csv("@myStage/prefix")
    * }}}
    *
    * '''Example 2:''' Write a DataFrame to a CSV file without compression.
    * {{{
    *   val result = df.write.option("compression", "none").csv("@myStage/prefix")
    * }}}
    *
    * @since 1.5.0
    * @param path
    *   The path (including the stage name) to the CSV file.
    * @return
    *   A [[WriteFileResult]]
    */
  def csv(path: String): WriteFileResult = action("csv") {
    val plan = getCopyIntoLocationPlan(path, "CSV")
    val (rows, attributes) = dataFrame.session.conn.getResultAndMetadata(plan)
    WriteFileResult(rows, StructType.fromAttributes(attributes))
  }

  // scalastyle:off
  /** Saves the contents of the DataFrame to a JSON file on a stage.
    *
    * NOTE: You can call this method only on a DataFrame that contains a column of the type Variant,
    * Array, or Map. If the DataFrame does not contain a column of one of these types, you must call
    * the `to_variant`, `array_construct`, or `object_construct` to return a DataFrame that contains
    * a column of one of these types.
    *
    * '''Example 1:''' Write a DataFrame with one variant to a JSON file.
    * {{{
    *   val result = session.sql("select to_variant('a')").write.json("@myStage/prefix")
    * }}}
    *
    * '''Example 2:''' Transform a DataFrame with some columns with array_construct() and write to a
    * JSON file without compression.
    * {{{
    *   val df = Seq((1, 1.1, "a"), (2, 2.2, "b")).toDF("a", "b", "c")
    *   val df2 = df.select(array_construct(df.schema.names.map(df(_)): _*))
    *   val result = df2.write.option("compression", "none").json("@myStage/prefix")
    * }}}
    *
    * '''Example 3:''' Transform a DataFrame with some columns with object_construct() and write to
    * a JSON file without compression.
    * {{{
    *   val df = Seq((1, 1.1, "a"), (2, 2.2, "b")).toDF("a", "b", "c")
    *   val df2 = df.select(object_construct(df.schema.names.map(x => Seq(lit(x), df(x))).flatten: _*))
    *   val result = df2.write.option("compression", "none").json("@myStage/prefix")
    * }}}
    *
    * @since 1.5.0
    * @param path
    *   The path (including the stage name) to the JSON file.
    * @return
    *   A [[WriteFileResult]]
    */
  // scalastyle:on
  def json(path: String): WriteFileResult = action("json") {
    val plan = getCopyIntoLocationPlan(path, "JSON")
    val (rows, attributes) = dataFrame.session.conn.getResultAndMetadata(plan)
    WriteFileResult(rows, StructType.fromAttributes(attributes))
  }

  /** Saves the contents of the DataFrame to a Parquet file on a stage.
    *
    * '''Example 1:''' Write a DataFrame to a parquet file.
    * {{{
    *   val result = df.write.parquet("@myStage/prefix")
    * }}}
    *
    * '''Example 2:''' Write a DataFrame to a Parquet file without compression.
    * {{{
    *   val result = df.write.option("compression", "LZO").parquet("@myStage/prefix")
    * }}}
    *
    * @since 1.5.0
    * @param path
    *   The path (including the stage name) to the Parquet file.
    * @return
    *   A [[WriteFileResult]]
    */
  def parquet(path: String): WriteFileResult = action("parquet") {
    val plan = getCopyIntoLocationPlan(path, "PARQUET")
    val (rows, attributes) = dataFrame.session.conn.getResultAndMetadata(plan)
    WriteFileResult(rows, StructType.fromAttributes(attributes))
  }

  // scalastyle:off
  /** Sets the specified option in the DataFrameWriter.
    *
    * =Sets the specified option for saving data to a table=
    *
    * Use this method to configure options:
    *   - columnOrder: save data into a table with table's column name order if saveMode is Append
    *     and target table exists.
    *
    * =Sets the specified option for saving data to a file on a stage=
    *
    * Use this method to configure options:
    *   - [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions format-specific options]]
    *   - [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions copy options]]
    *   - [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#optional-parameters PARTITION BY or HEADER]]
    *
    * Note that you cannot use the `option` and `options` methods to set the following options:
    *   - The `TYPE` format type option.
    *   - The `OVERWRITE` copy option. To set this option, use the
    *     [[mode(saveMode:com\.snowflake\.snowpark\.SaveMode* mode]] method instead.
    *     - To set `OVERWRITE` to `TRUE`, use `SaveMode.Overwrite`.
    *     - To set `OVERWRITE` to `FALSE`, use `SaveMode.ErrorIfExists`.
    *
    * '''Example 1:''' Write a DataFrame to a CSV file.
    * {{{
    *   val result = df.write.csv("@myStage/prefix")
    * }}}
    *
    * '''Example 2:''' Write a DataFrame to a CSV file without compression.
    * {{{
    *   val result = df.write.option("compression", "none").csv("@myStage/prefix")
    * }}}
    *
    * @since 1.4.0
    * @param key
    *   Name of the option.
    * @param value
    *   Value of the option.
    * @return
    *   A [[DataFrameWriter]]
    */
  // scalastyle:on
  def option(key: String, value: Any): DataFrameWriter = {
    this.writeOptions.put(key.toUpperCase(Locale.ROOT), value)
    this
  }

  // scalastyle:off
  /** Sets multiple specified options in the DataFrameWriter.
    *
    * =Sets the specified options for saving Data to a Table=
    *
    * Use this method to configure options:
    *   - columnOrder: save data into a table with table's column name order if saveMode is Append
    *     and target table exists.
    *
    * =Sets the specified options for saving data to a file on a stage=
    *
    * Use this method to configure options:
    *   - [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#format-type-options-formattypeoptions format-specific options]]
    *   - [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#copy-options-copyoptions copy options]]
    *   - [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#optional-parameters PARTITION BY or HEADER]]
    *
    * Note that you cannot use the `option` and `options` methods to set the following options:
    *   - The `TYPE` format type option.
    *   - The `OVERWRITE` copy option. To set this option, use the
    *     [[mode(saveMode:com\.snowflake\.snowpark\.SaveMode* mode]] method instead.
    *     - To set `OVERWRITE` to `TRUE`, use `SaveMode.Overwrite`.
    *     - To set `OVERWRITE` to `FALSE`, use `SaveMode.ErrorIfExists`.
    *
    * '''Example 1:''' Write a DataFrame to a CSV file.
    * {{{
    *   val result = df.write.csv("@myStage/prefix")
    * }}}
    *
    * '''Example 2:''' Write a DataFrame to a CSV file without compression.
    * {{{
    *   val result = df.write.option("compression", "none").csv("@myStage/prefix")
    * }}}
    *
    * @since 1.5.0
    * @param configs
    *   Map of the names of options (e.g. {@code compression} , etc.) and their corresponding
    *   values.
    * @return
    *   A [[DataFrameWriter]]
    */
  def options(configs: Map[String, Any]): DataFrameWriter = {
    configs.foreach(e => option(e._1, e._2))
    this
  }

  /** Writes the data to the specified table in a Snowflake database. {@code tableName} can be a
    * fully-qualified object identifier.
    *
    * For example:
    * {{{
    *   df.write.saveAsTable("db1.public_schema.table1")
    * }}}
    * @param tableName
    *   Name of the table where the data should be saved.
    * @since 0.1.0
    */
  def saveAsTable(tableName: String): Unit = action("saveAsTable") {
    val writePlan = getWriteTablePlan(tableName)
    dataFrame.session.conn.execute(writePlan)
  }

  private[snowpark] def getWriteTablePlan(tableName: String): SnowflakePlan = {
    dataFrame.session.conn.telemetry.reportActionSaveAsTable()
    Utils.validateObjectName(tableName)
    val tableSaveMode = saveMode.getOrElse(SaveMode.Append)
    val columnOrderValidValues = Seq("'index'", "'name'")
    // By default, "columnOrder" is "index"
    val columnOrder = Utils.quoteForOption(this.writeOptions.getOrElse(COLUMN_ORDER, "index"))
    if (!columnOrderValidValues.contains(columnOrder)) {
      throw ErrorMessage.DF_WRITER_INVALID_OPTION_VALUE(COLUMN_ORDER, columnOrder, "table")
    } else if (this.writeOptions.keySet.count(!_.equals(COLUMN_ORDER)) > 0) {
      val key = this.writeOptions.keySet.filter(!_.equals(COLUMN_ORDER)).head
      throw ErrorMessage.DF_WRITER_INVALID_OPTION_NAME(key, "table")
    }
    // Add SELECT to adjust the column order if "columnOrder" is "name"
    val newDf = columnOrder match {
      case "'name'" if tableSaveMode.equals(SaveMode.Append) =>
        if (dataFrame.session.tableExists(tableName)) {
          dataFrame.select(dataFrame.session.table(tableName).output.map(_.name).map(quoteName))
        } else {
          dataFrame
        }
      case "'name'" =>
        throw ErrorMessage.DF_WRITER_INVALID_OPTION_NAME_IN_MODE(
          COLUMN_ORDER,
          "name",
          saveMode.toString,
          "table")
      case _ => dataFrame
    }
    val plan = SnowflakeCreateTable(tableName, tableSaveMode, Some(newDf.plan))
    dataFrame.session.analyzer.resolve(plan)
  }

  /** Writes the data to the specified table in a Snowflake database.
    *
    * For example:
    * {{{
    *   df.write.saveAsTable(Seq("db_name", "schema_name", "table_name"))
    * }}}
    *
    * @param multipartIdentifier
    *   A sequence of strings that specify the database name, schema name, and table name (e.g.
    *   {@code Seq("database_name", "schema_name", "table_name")} ).
    * @since 0.5.0
    */
  def saveAsTable(multipartIdentifier: Seq[String]): Unit = action("saveAsTable") {
    val writePlan = getWriteTablePlan(multipartIdentifier.mkString("."))
    dataFrame.session.conn.execute(writePlan)
  }

  /** Writes the data to the specified table in a Snowflake database.
    *
    * For example:
    * {{{
    *     val list = new java.util.ArrayList[String](3)
    *     list.add(db)
    *     list.add(sc)
    *     list.add(tableName)
    *   df.write.saveAsTable(list)
    * }}}
    *
    * @param multipartIdentifier
    *   A list of strings that specify the database name, schema name, and table name.
    * @since 0.5.0
    */
  def saveAsTable(multipartIdentifier: java.util.List[String]): Unit = action("saveAsTable") {
    val writePlan = getWriteTablePlan(multipartIdentifier.asScala.mkString("."))
    dataFrame.session.conn.execute(writePlan)
  }

  /** Returns a new DataFrameWriter with the specified save mode configuration.
    *
    * @param saveMode
    *   One of the following strings: `"APPEND"`, `"OVERWRITE"`, `"ERRORIFEXISTS"`, or `"IGNORE"`
    * @since 0.1.0
    */
  def mode(saveMode: String): DataFrameWriter = mode(SaveMode(saveMode))

  /** Returns a new DataFrameWriter with the specified save mode configuration.
    *
    * @param saveMode
    *   One of the following save modes: [[SaveMode.Append]], [[SaveMode.Overwrite]],
    *   [[SaveMode.ErrorIfExists]], [[SaveMode.Ignore]]
    * @since 0.1.0
    */
  def mode(saveMode: SaveMode): DataFrameWriter = {
    this.saveMode = Some(saveMode)
    this
  }

  /** Returns a [[DataFrameWriterAsyncActor]] object that can be used to execute DataFrameWriter
    * actions asynchronously.
    *
    * Example:
    * {{{
    *   val asyncJob = df.write.mode(SaveMode.Overwrite).async.saveAsTable(tableName)
    *   // At this point, the thread is not blocked. You can perform additional work before
    *   // calling asyncJob.getResult() to retrieve the results of the action.
    *   // NOTE: getResult() is a blocking call.
    *   asyncJob.getResult()
    * }}}
    *
    * @since 0.11.0
    * @return
    *   A [[DataFrameWriterAsyncActor]] object
    */
  def async: DataFrameWriterAsyncActor = new DataFrameWriterAsyncActor(this)

  @inline protected def action[T](funcName: String)(func: => T): T = {
    val isScala: Boolean = dataFrame.session.conn.isScalaAPI
    OpenTelemetry.action("DataFrameWriter", funcName, this.dataFrame.methodChainString + ".writer")(
      func)
  }

}

/** Provides APIs to execute DataFrameWriter actions asynchronously.
  *
  * @since 0.11.0
  */
class DataFrameWriterAsyncActor private[snowpark] (writer: DataFrameWriter) {

  /** Executes `DataFrameWriter.saveAsTable` asynchronously.
    *
    * @param tableName
    *   Name of the table where the data should be saved.
    * @return
    *   A [[TypedAsyncJob]] object that you can use to check the status of the action and get the
    *   results.
    * @since 0.11.0
    */
  def saveAsTable(tableName: String): TypedAsyncJob[Unit] = action("saveAsTable") {
    val writePlan = writer.getWriteTablePlan(tableName)
    writePlan.session.conn.executeAsync[Unit](writePlan)
  }

  /** Executes `DataFrameWriter.saveAsTable` asynchronously.
    *
    * @param multipartIdentifier
    *   A sequence of strings that specify the database name, schema name, and table name (e.g.
    *   {@code Seq("database_name", "schema_name", "table_name")} ).
    * @return
    *   A [[TypedAsyncJob]] object that you can use to check the status of the action and get the
    *   results.
    * @since 0.11.0
    */
  def saveAsTable(multipartIdentifier: Seq[String]): TypedAsyncJob[Unit] = action("saveAsTable") {
    val writePlan = writer.getWriteTablePlan(multipartIdentifier.mkString("."))
    writePlan.session.conn.executeAsync[Unit](writePlan)
  }

  /** Executes `DataFrameWriter.saveAsTable` asynchronously.
    *
    * @param multipartIdentifier
    *   A list of strings that specify the database name, schema name, and table name.
    * @return
    *   A [[TypedAsyncJob]] object that you can use to check the status of the action and get the
    *   results.
    * @since 0.11.0
    */
  def saveAsTable(multipartIdentifier: java.util.List[String]): TypedAsyncJob[Unit] =
    action("saveAsTable") {
      val writePlan = writer.getWriteTablePlan(multipartIdentifier.asScala.mkString("."))
      writePlan.session.conn.executeAsync[Unit](writePlan)
    }

  /** Executes `DataFrameWriter.csv` asynchronously.
    *
    * @param path
    *   The path (including the stage name) to the CSV file.
    * @return
    *   A [[TypedAsyncJob]] object that you can use to check the status of the action and get the
    *   results.
    * @since 1.5.0
    */
  def csv(path: String): TypedAsyncJob[WriteFileResult] = action("csv") {
    val writePlan = writer.getCopyIntoLocationPlan(path, "CSV")
    writePlan.session.conn.executeAsync[WriteFileResult](writePlan)
  }

  /** Executes `DataFrameWriter.json` asynchronously.
    *
    * @param path
    *   The path (including the stage name) to the JSON file.
    * @return
    *   A [[TypedAsyncJob]] object that you can use to check the status of the action and get the
    *   results.
    * @since 1.5.0
    */
  def json(path: String): TypedAsyncJob[WriteFileResult] = action("json") {
    val writePlan = writer.getCopyIntoLocationPlan(path, "JSON")
    writePlan.session.conn.executeAsync[WriteFileResult](writePlan)
  }

  /** Executes `DataFrameWriter.parquet` asynchronously.
    *
    * @param path
    *   The path (including the stage name) to the PARQUET file.
    * @return
    *   A [[TypedAsyncJob]] object that you can use to check the status of the action and get the
    *   results.
    * @since 1.5.0
    */
  def parquet(path: String): TypedAsyncJob[WriteFileResult] = action { "parquet" } {
    val writePlan = writer.getCopyIntoLocationPlan(path, "PARQUET")
    writePlan.session.conn.executeAsync[WriteFileResult](writePlan)
  }

  @inline protected def action[T](funcName: String)(func: => T): T = {
    OpenTelemetry.action(
      "DataFrameWriterAsyncActor",
      funcName,
      writer.dataFrame.methodChainString + ".writer.async")(func)
  }
}

/** Represents the results of writing data from a DataFrame to a file in a stage.
  *
  * To write the data, the DataFrameWriter effectively executes the `COPY INTO <location>` command.
  * WriteFileResult encapsulates the output returned by the command:
  *   - `rows` represents the rows of output from the command.
  *   - `schema` defines the schema for these rows.
  *
  * For example, if the DETAILED_OUTPUT option is TRUE, each row contains a `file_name`,
  * `file_size`, and `row_count` field. `schema` defines the names and types of these fields. If the
  * DETAILED_OUTPUT option is not specified (meaning that the option is FALSE), each row contains a
  * `rows_unloaded`, `input_bytes`, and `output_bytes` field.
  *
  * @param rows
  *   The output rows produced by the `COPY INTO <location>` command.
  * @param schema
  *   The names and types of the fields in the output rows.
  * @since 1.5.0
  */
case class WriteFileResult(rows: Array[Row], schema: StructType)
