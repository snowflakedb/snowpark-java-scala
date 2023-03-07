package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * DataFrame for loading data from files in a stage to a table. Objects of this type are returned by
 * the {@code DataFrameReader} methods that load data from files (e.g. {@code
 * DataFrameReader.csv()}).
 *
 * <p>To save the data from the staged files to a table, call the `copyInto()` methods. This method
 * uses the COPY INTO `table_name` command to copy the data to a specified table.
 *
 * @since 1.1.0
 */
public class CopyableDataFrame extends DataFrame {
  private final com.snowflake.snowpark.CopyableDataFrame copy;

  CopyableDataFrame(com.snowflake.snowpark.CopyableDataFrame copy) {
    super(copy);
    this.copy = copy;
  }

  /**
   * Executes a `COPY INTO 'table_name'` command to load data from files in a stage into a specified
   * table.
   *
   * <p>copyInto is an action method (like the 'collect' method), so calling the method executes the
   * SQL statement to copy the data.
   *
   * <p>For example, the following code loads data from the path specified by `myFileStage` to the
   * table `T`:
   *
   * <pre>{@code
   * session.read().schema(userSchema).csv(myFileStage).copyInto("T");
   * }</pre>
   *
   * @param tableName Name of the table where the data should be saved.
   * @since 1.1.0
   */
  public void copyInto(String tableName) {
    copy.copyInto(tableName);
  }

  /**
   * Executes a `COPY INTO 'table_name'` command with the specified transformations to load data
   * from files in a stage into a specified table.
   *
   * <p>copyInto is an action method (like the 'collect' method), so calling the method executes the
   * SQL statement to copy the data.
   *
   * <p>When copying the data into the table, you can apply transformations to the data from the
   * files to: Rename the columns, Change the order of the columns, Omit or insert columns, Cast the
   * value in a column to a specific type
   *
   * <p>You can use the same techniques described in <a
   * href="https://docs.snowflake.com/en/user-guide/data-load-transform.html">Transforming Data
   * During Load</a> expressed as a {@code Seq} of Column expressions that correspond to the <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters">SELECT
   * statement parameters</a> in the `COPY INTO 'table_name'` command.
   *
   * <p>For example, the following code loads data from the path specified by `myFileStage` to the
   * table `T`. The example transforms the data from the file by inserting the value of the first
   * column into the first column of table `T` and inserting the length of that value into the
   * second column of table `T`.
   *
   * <pre>{@code
   * Column[] transformations = {Functions.col("$1"), Functions.length(Functions.col("$1"))};
   * session.read().schema(userSchema).csv(myFileStage).copyInto("T", transformations)
   * }</pre>
   *
   * @param tableName Name of the table where the data should be saved.
   * @param transformations Seq of Column expressions that specify the transformations to apply
   *     (similar to <a
   *     href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters">transformation
   *     parameters</a>).
   * @since 1.1.0
   */
  public void copyInto(String tableName, Column[] transformations) {
    copy.copyInto(
        tableName, JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(transformations)));
  }

  /**
   * Executes a `COPY INTO 'table_name'` command with the specified transformations to load data
   * from files in a stage into a specified table.
   *
   * <p>copyInto is an action method (like the 'collect' method), so calling the method executes the
   * SQL statement to copy the data.
   *
   * <p>In addition, you can specify <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#format-type-options-formattypeoptions">format
   * type options</a> or <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#label-copy-into-table-copyoptions">copy
   * options</a> that determine how the copy operation should be performed.
   *
   * <p>When copying the data into the table, you can apply transformations to the data from the
   * files to: Rename the columns, Change the order of the columns, Omit or insert columns, Cast the
   * value in a column to a specific type
   *
   * <p>You can use the same techniques described in <a
   * href="https://docs.snowflake.com/en/user-guide/data-load-transform.html">Transforming Data
   * During Load</a> expressed as a {@code Seq} of Column expressions that correspond to the <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters">SELECT
   * statement parameters</a> in the `COPY INTO 'table_name'` command.
   *
   * <p>For example, the following code loads data from the path specified by `myFileStage` to the
   * table `T`. The example transforms the data from the file by inserting the value of the first
   * column into the first column of table `T` and inserting the length of that value into the
   * second column of table `T`. The example also uses a {@code Map} to set the {@code FORCE} and
   * {@code skip_header} options for the copy operation.
   *
   * <pre>{@code
   * Map<String, Object> options = new HashMap<>();
   * options.put("FORCE", "TRUE");
   * options.put("skip_header", 1);
   * Column[] transformations = {Functions.col("$1"), Functions.length(Functions.col("$1"))};
   * session.read().schema(userSchema).csv(myFileStage).copyInto("T", transformations, options);
   * }</pre>
   *
   * @param tableName Name of the table where the data should be saved.
   * @param transformations Seq of Column expressions that specify the transformations to apply
   *     (similar to <a
   *     href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters">transformation
   *     parameters</a>).
   * @param options Map of the names of options (e.g. {@code compression}, {@code skip_header},
   *     etc.) and their corresponding values.NOTE: By default, the {@code CopyableDataFrame} object
   *     uses the options set in the DataFrameReader used to create that object. You can use this
   *     {@code options} parameter to override the default options or set additional options.
   * @since 1.1.0
   */
  public void copyInto(String tableName, Column[] transformations, Map<String, ?> options) {
    copy.copyInto(
        tableName,
        JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(transformations)),
        JavaUtils.javaStringAnyMapToScala(options));
  }

  /**
   * Executes a `COPY INTO 'table_name'` command with the specified transformations to load data
   * from files in a stage into a specified table.
   *
   * <p>copyInto is an action method (like the 'collect' method), so calling the method executes the
   * SQL statement to copy the data.
   *
   * <p>In addition, you can specify <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#format-type-options-formattypeoptions">format
   * type options</a> or <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#label-copy-into-table-copyoptions">copy
   * options</a> that determine how the copy operation should be performed.
   *
   * <p>When copying the data into the table, you can apply transformations to the data from the
   * files to: Rename the columns, Change the order of the columns, Omit or insert columns, Cast the
   * value in a column to a specific type
   *
   * <p>You can use the same techniques described in <a
   * href="https://docs.snowflake.com/en/user-guide/data-load-transform.html">Transforming Data
   * During Load</a> expressed as a {@code Seq} of Column expressions that correspond to the <a
   * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters">SELECT
   * statement parameters</a> in the `COPY INTO 'table_name'` command.
   *
   * <p>You can specify a subset of the table columns to copy into. The number of provided column
   * names must match the number of transformations.
   *
   * <p>For example, suppose the target table `T` has 3 columns: "ID", "A" and "A_LEN". "ID" is an
   * `AUTOINCREMENT` column, which should be exceluded from this copy into action. The following
   * code loads data from the path specified by `myFileStage` to the table `T`. The example
   * transforms the data from the file by inserting the value of the first column into the column
   * `A` and inserting the length of that value into the column `A_LEN`. The example also uses a
   * {@code Map} to set the {@code FORCE} and {@code skip_header} options for the copy operation.
   *
   * <pre>{@code
   * Map<String, Object> options = new HashMap<>();
   * options.put("FORCE", "TRUE");
   * options.put("skip_header", 1);
   * Column[] transformations = {Functions.col("$1"), Functions.length(Functions.col("$1"))};
   * String[] targetColumnNames = {"A", "A_LEN"};
   * session.read().schema(userSchema).csv(myFileStage).copyInto("T", targetColumnNames, transformations, options);
   * }</pre>
   *
   * @param tableName Name of the table where the data should be saved.
   * @param targetColumnNames Name of the columns in the table where the data should be saved.
   * @param transformations Seq of Column expressions that specify the transformations to apply
   *     (similar to <a
   *     href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters">transformation
   *     parameters</a>).
   * @param options Map of the names of options (e.g. {@code compression}, {@code skip_header},
   *     etc.) and their corresponding values.NOTE: By default, the {@code CopyableDataFrame} object
   *     uses the options set in the DataFrameReader used to create that object. You can use this
   *     {@code options} parameter to override the default options or set additional options.
   * @since 1.1.0
   */
  public void copyInto(
      String tableName,
      String[] targetColumnNames,
      Column[] transformations,
      Map<String, ?> options) {
    copy.copyInto(
        tableName,
        JavaUtils.stringArrayToStringSeq(targetColumnNames),
        JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(transformations)),
        JavaUtils.javaStringAnyMapToScala(options));
  }

  /**
   * Returns a clone of this CopyableDataFrame.
   *
   * @return A CopyableDataFrame
   * @since 1.1.0
   */
  @Override
  public CopyableDataFrame clone() {
    super.clone();
    return new CopyableDataFrame(copy.clone());
  }

  /**
   * Returns a CopyableDataFrameAsyncActor object that can be used to execute CopyableDataFrame
   * actions asynchronously.
   *
   * @since 1.2.0
   * @return A CopyableDataFrameAsyncActor object
   */
  @Override
  public CopyableDataFrameAsyncActor async() {
    return new CopyableDataFrameAsyncActor(this);
  }

  com.snowflake.snowpark.CopyableDataFrame getScalaCopyableDataFrame() {
    return this.copy;
  }
}
