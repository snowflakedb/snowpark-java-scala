package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.util.Map;

/**
 * Provides APIs to execute CopyableDataFrame actions asynchronously.
 *
 * @since 1.2.0
 */
public class CopyableDataFrameAsyncActor extends DataFrameAsyncActor {
  private final com.snowflake.snowpark.CopyableDataFrameAsyncActor cDfAsync;

  CopyableDataFrameAsyncActor(CopyableDataFrame df) {
    super(df);
    this.cDfAsync = df.getScalaCopyableDataFrame().async();
  }

  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
   *
   * @param tableName Name of the table where the data should be saved.
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Void> copyInto(String tableName) {
    return TypedAsyncJob.createVoidJob(cDfAsync.copyInto(tableName), session);
  }

  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
   *
   * @param tableName Name of the table where the data should be saved.
   * @param transformations Seq of Column expressions that specify the transformations to apply
   *     (similar to <a
   *     href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#transformation-parameters">transformation
   *     parameters</a>).
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Void> copyInto(String tableName, Column[] transformations) {
    return TypedAsyncJob.createVoidJob(
        cDfAsync.copyInto(
            tableName, JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(transformations))),
        session);
  }

  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
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
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Void> copyInto(
      String tableName, Column[] transformations, Map<String, ?> options) {
    return TypedAsyncJob.createVoidJob(
        cDfAsync.copyInto(
            tableName,
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(transformations)),
            JavaUtils.javaStringAnyMapToScala(options)),
        session);
  }

  /**
   * Executes `CopyableDataFrame.copyInto` asynchronously.
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
   * @return A TypedAsyncJob object that you can use to check the status of the action and get the
   *     results.
   * @since 1.2.0
   */
  public TypedAsyncJob<Void> copyInto(
      String tableName,
      String[] targetColumnNames,
      Column[] transformations,
      Map<String, ?> options) {
    return TypedAsyncJob.createVoidJob(
        cDfAsync.copyInto(
            tableName,
            JavaUtils.stringArrayToStringSeq(targetColumnNames),
            JavaUtils.columnArrayToSeq(Column.toScalaColumnArray(transformations)),
            JavaUtils.javaStringAnyMapToScala(options)),
        session);
  }
}
