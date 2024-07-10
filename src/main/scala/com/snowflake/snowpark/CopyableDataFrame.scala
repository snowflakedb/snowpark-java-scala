package com.snowflake.snowpark

import com.snowflake.snowpark.internal._
import com.snowflake.snowpark.internal.analyzer._

/**
 * DataFrame for loading data from files in a stage to a table.
 * Objects of this type are returned by the [[DataFrameReader]] methods that load data from files
 * (e.g. [[DataFrameReader.csv csv]]).
 *
 * To save the data from the staged files to a table, call the `copyInto()` methods.
 * This method uses the COPY INTO `<table_name>` command to copy the data to a specified table.
 *
 * @groupname actions Actions
 * @groupname basic Basic DataFrame Functions
 *
 * @since 0.9.0
 */
class CopyableDataFrame private[snowpark] (
    override private[snowpark] val session: Session,
    override private[snowpark] val plan: SnowflakePlan,
    private val stagedFileReader: StagedFileReader)
    extends DataFrame(session, plan) {

  /**
   * Executes a `COPY INTO <table_name>` command to
   * load data from files in a stage into a specified table.
   *
   * copyInto is an action method (like the [[collect]] method),
   * so calling the method executes the SQL statement to copy the data.
   *
   * For example, the following code loads data from
   * the path specified by `myFileStage` to the table `T`:
   * {{{
   *   val df = session.read.schema(userSchema).csv(myFileStage)
   *   df.copyInto("T")
   * }}}
   *
   * @group actions
   * @param tableName Name of the table where the data should be saved.
   * @since 0.9.0
   */
  def copyInto(tableName: String): Unit = action("copyInto") {
    getCopyDataFrame(tableName, Seq.empty, Seq.empty, Map.empty).collect()
  }

  // scalastyle:off line.size.limit
  /**
   * Executes a `COPY INTO <table_name>` command with the specified transformations to
   * load data from files in a stage into a specified table.
   *
   * copyInto is an action method (like the [[collect]] method),
   * so calling the method executes the SQL statement to copy the data.
   *
   * When copying the data into the table, you can apply transformations to
   * the data from the files to:
   *  - Rename the columns
   *  - Change the order of the columns
   *  - Omit or insert columns
   *  - Cast the value in a column to a specific type
   *
   * You can use the same techniques described in
   * [[https://docs.snowflake.com/en/user-guide/data-load-transform.html Transforming Data During Load]]
   * expressed as a {@code Seq} of [[Column]] expressions that correspond to the
   * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters SELECT statement parameters]]
   * in the `COPY INTO <table_name>` command.
   *
   * For example, the following code loads data from the path specified
   * by `myFileStage` to the table `T`. The example transforms the data
   * from the file by inserting the value of the first column into the first column of table `T`
   * and inserting the length of that value into the second column of table `T`.
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   val df = session.read.schema(userSchema).csv(myFileStage)
   *   val transformations = Seq(col("\$1"), length(col("\$1")))
   *   df.copyInto("T", transformations)
   * }}}
   *
   * @group actions
   * @param tableName Name of the table where the data should be saved.
   * @param transformations Seq of [[Column]] expressions that specify the transformations to apply
   *        (similar to [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters transformation parameters]]).
   * @since 0.9.0
   */
  // scalastyle:on line.size.limit
  def copyInto(tableName: String, transformations: Seq[Column]): Unit = action("copyInto") {
    getCopyDataFrame(tableName, Seq.empty, transformations, Map.empty).collect()
  }

  // scalastyle:off line.size.limit
  /**
   * Executes a `COPY INTO <table_name>` command with the specified transformations and options to
   * load data from files in a stage into a specified table.
   *
   * copyInto is an action method (like the [[collect]] method),
   * so calling the method executes the SQL statement to copy the data.
   *
   * In addition, you can specify [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#format-type-options-formattypeoptions format type options]]
   * or [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#label-copy-into-table-copyoptions copy options]]
   * that determine how the copy operation should be performed.
   *
   * When copying the data into the table, you can apply transformations to
   * the data from the files to:
   *  - Rename the columns
   *  - Change the order of the columns
   *  - Omit or insert columns
   *  - Cast the value in a column to a specific type
   *
   * You can use the same techniques described in
   * [[https://docs.snowflake.com/en/user-guide/data-load-transform.html Transforming Data During Load]]
   * expressed as a {@code Seq} of [[Column]] expressions that correspond to the
   * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters SELECT statement parameters]]
   * in the `COPY INTO <table_name>` command.
   *
   * For example, the following code loads data from the path specified
   * by `myFileStage` to the table `T`. The example transforms the data
   * from the file by inserting the value of the first column into the first column of table `T`
   * and inserting the length of that value into the second column of table `T`.
   * The example also uses a {@code Map} to set the {@code FORCE} and {@code skip_header} options
   * for the copy operation.
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   val df = session.read.schema(userSchema).option("skip_header", 1).csv(myFileStage)
   *   val transformations = Seq(col("\$1"), length(col("\$1")))
   *   val extraOptions = Map("FORCE" -> "true", "skip_header" -> 2)
   *   df.copyInto("T", transformations, extraOptions)
   * }}}
   *
   * @group actions
   * @param tableName Name of the table where the data should be saved.
   * @param transformations Seq of [[Column]] expressions that specify the transformations to apply
   *        (similar to [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters transformation parameters]]).
   * @param options Map of the names of options (e.g. { @code compression}, { @code skip_header},
   *        etc.) and their corresponding values.NOTE: By default, the {@code CopyableDataFrame} object
   *        uses the options set in the [[DataFrameReader]] used to create that object. You can use
   *        this {@code options} parameter to override the default options or set additional options.
   * @since 0.9.0
   */
  // scalastyle:on line.size.limit
  def copyInto(tableName: String, transformations: Seq[Column], options: Map[String, Any]): Unit =
    action("copyInto") {
      getCopyDataFrame(tableName, Seq.empty, transformations, options).collect()
    }

  // scalastyle:off line.size.limit
  /**
   * Executes a `COPY INTO <table_name>` command with the specified transformations and options to
   * load data from files in a stage into a specified table.
   *
   * copyInto is an action method (like the [[collect]] method),
   * so calling the method executes the SQL statement to copy the data.
   *
   * In addition, you can specify [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#format-type-options-formattypeoptions format type options]]
   * or [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#label-copy-into-table-copyoptions copy options]]
   * that determine how the copy operation should be performed.
   *
   * When copying the data into the table, you can apply transformations to
   * the data from the files to:
   *  - Rename the columns
   *  - Change the order of the columns
   *  - Omit or insert columns
   *  - Cast the value in a column to a specific type
   *
   * You can use the same techniques described in
   * [[https://docs.snowflake.com/en/user-guide/data-load-transform.html Transforming Data During Load]]
   * expressed as a {@code Seq} of [[Column]] expressions that correspond to the
   * [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters SELECT statement parameters]]
   * in the `COPY INTO <table_name>` command.
   *
   * You can specify a subset of the table columns to copy into. The number of provided column names
   * must match the number of transformations.
   *
   * For example, suppose the target table `T` has 3 columns:  "ID", "A" and "A_LEN".
   * "ID" is an `AUTOINCREMENT` column, which should be exceluded from this copy into action.
   * The following code loads data from the path specified by `myFileStage` to the table `T`.
   * The example transforms the data from the file by inserting the value of the first column
   * into the column `A` and inserting the length of that value into the column `A_LEN`.
   * The example also uses a {@code Map} to set the {@code FORCE} and {@code skip_header} options
   * for the copy operation.
   * {{{
   *   import com.snowflake.snowpark.functions._
   *   val df = session.read.schema(userSchema).option("skip_header", 1).csv(myFileStage)
   *   val transformations = Seq(col("\$1"), length(col("\$1")))
   *   val targetColumnNames = Seq("A", "A_LEN")
   *   val extraOptions = Map("FORCE" -> "true", "skip_header" -> 2)
   *   df.copyInto("T", targetColumnNames, transformations, extraOptions)
   * }}}
   *
   * @group actions
   * @param tableName Name of the table where the data should be saved.
   * @param targetColumnNames Name of the columns in the table where the data should be saved.
   * @param transformations Seq of [[Column]] expressions that specify the transformations to apply
   *        (similar to [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters transformation parameters]]).
   * @param options Map of the names of options (e.g. { @code compression}, { @code skip_header},
   *        etc.) and their corresponding values.NOTE: By default, the {@code CopyableDataFrame} object
   *        uses the options set in the [[DataFrameReader]] used to create that object. You can use
   *        this {@code options} parameter to override the default options or set additional options.
   * @since 0.11.0
   */
  // scalastyle:on line.size.limit
  def copyInto(
      tableName: String,
      targetColumnNames: Seq[String],
      transformations: Seq[Column],
      options: Map[String, Any]): Unit = action("copyInto") {
    getCopyDataFrame(tableName, targetColumnNames, transformations, options).collect()
  }

  // Internal function to create plan for COPY
  private[snowpark] def getCopyDataFrame(
      tableName: String,
      targetColumnNames: Seq[String] = Seq.empty,
      transformations: Seq[Column] = Seq.empty,
      options: Map[String, Any] = Map.empty): DataFrame = {
    if (targetColumnNames.nonEmpty && transformations.nonEmpty &&
        targetColumnNames.size != transformations.size) {
      // If columnNames and transformations are provided, the size of them must match.
      throw ErrorMessage.PLAN_COPY_INVALID_COLUMN_NAME_SIZE(
        targetColumnNames.size,
        transformations.size)
    }
    session.conn.telemetry.reportActionCopyInto()
    Utils.validateObjectName(tableName)
    withPlan(
      CopyIntoNode(
        tableName,
        targetColumnNames.map(internal.analyzer.quoteName),
        transformations.map(_.expr),
        options,
        new StagedFileReader(stagedFileReader)))
  }

  /**
   * Returns a clone of this CopyableDataFrame.
   *
   * @return A [[CopyableDataFrame]]
   * @since 0.10.0
   * @group basic
   */
  override def clone: CopyableDataFrame = action("clone") {
    new CopyableDataFrame(session, plan, stagedFileReader)
  }

  /**
   * Returns a [[CopyableDataFrameAsyncActor]] object that can be used to execute
   * CopyableDataFrame actions asynchronously.
   *
   * Example:
   * {{{
   *   val asyncJob = session.read.schema(userSchema).csv(testFileOnStage).async.collect()
   *   // At this point, the thread is not blocked. You can perform additional work before
   *   // calling asyncJob.getResult() to retrieve the results of the action.
   *   // NOTE: getResult() is a blocking call.
   *   asyncJob.getResult()
   * }}}
   *
   * @since 0.11.0
   * @return A [[CopyableDataFrameAsyncActor]] object
   */
  override def async: CopyableDataFrameAsyncActor = new CopyableDataFrameAsyncActor(this)

  @inline override protected def action[T](funcName: String)(func: => T): T = {
    val isScala: Boolean = this.session.conn.isScalaAPI
    OpenTelemetry.action("CopyableDataFrame", funcName, isScala)(func)
  }
}

/**
 * Provides APIs to execute CopyableDataFrame actions asynchronously.
 *
 * @since 0.11.0
 */
class CopyableDataFrameAsyncActor private[snowpark] (cdf: CopyableDataFrame)
    extends DataFrameAsyncActor(cdf) {

  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
   *
   * @param tableName Name of the table where the data should be saved.
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  def copyInto(tableName: String): TypedAsyncJob[Unit] = action("copyInto") {
    val df = cdf.getCopyDataFrame(tableName)
    cdf.session.conn.executeAsync[Unit](df.snowflakePlan)
  }

  // scalastyle:off line.size.limit
  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
   *
   * @param tableName Name of the table where the data should be saved.
   * @param transformations Seq of [[Column]] expressions that specify the transformations to apply
   *        (similar to [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters transformation parameters]]).
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  // scalastyle:on line.size.limit
  def copyInto(tableName: String, transformations: Seq[Column]): TypedAsyncJob[Unit] =
    action("copyInto") {
      val df = cdf.getCopyDataFrame(tableName, Seq.empty, transformations, Map.empty)
      cdf.session.conn.executeAsync[Unit](df.snowflakePlan)
    }

  // scalastyle:off line.size.limit
  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
   *
   * @param tableName Name of the table where the data should be saved.
   * @param transformations Seq of [[Column]] expressions that specify the transformations to apply
   *        (similar to [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters transformation parameters]]).
   * @param options Map of the names of options (e.g. { @code compression}, { @code skip_header},
   *        etc.) and their corresponding values.NOTE: By default, the {@code CopyableDataFrame} object
   *        uses the options set in the [[DataFrameReader]] used to create that object. You can use
   *        this {@code options} parameter to override the default options or set additional options.
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  // scalastyle:on line.size.limit
  def copyInto(
      tableName: String,
      transformations: Seq[Column],
      options: Map[String, Any]): TypedAsyncJob[Unit] = action("copyInto") {
    val df = cdf.getCopyDataFrame(tableName, Seq.empty, transformations, options)
    cdf.session.conn.executeAsync[Unit](df.snowflakePlan)
  }

  // scalastyle:off line.size.limit
  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
   *
   * @param tableName Name of the table where the data should be saved.
   * @param targetColumnNames Name of the columns in the table where the data should be saved.
   * @param transformations Seq of [[Column]] expressions that specify the transformations to apply
   *        (similar to [[https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters transformation parameters]]).
   * @param options Map of the names of options (e.g. { @code compression}, { @code skip_header},
   *        etc.) and their corresponding values.NOTE: By default, the {@code CopyableDataFrame} object
   *        uses the options set in the [[DataFrameReader]] used to create that object. You can use
   *        this {@code options} parameter to override the default options or set additional options.
   * @return A [[TypedAsyncJob]] object that you can use to check the status of the action
   *         and get the results.
   * @since 0.11.0
   */
  // scalastyle:on line.size.limit
  def copyInto(
      tableName: String,
      targetColumnNames: Seq[String],
      transformations: Seq[Column],
      options: Map[String, Any]): TypedAsyncJob[Unit] = action("copyInto") {
    val df = cdf.getCopyDataFrame(tableName, targetColumnNames, transformations, options)
    cdf.session.conn.executeAsync[Unit](df.snowflakePlan)
  }

  @inline override protected def action[T](funcName: String)(func: => T): T = {
    val isScala: Boolean = cdf.session.conn.isScalaAPI
    OpenTelemetry.action("CopyableDataFrameAsyncActor", funcName, isScala)(func)
  }
}
