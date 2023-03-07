package com.snowflake.snowpark_java;

/**
 * SaveMode configures the behavior when data is written from a DataFrame to a data source using a
 * DataFrameWriter instance.
 *
 * <p>Append: In the Append mode, new data is appended to the datasource.
 *
 * <p>Overwrite: In the Overwrite mode, existing data is overwritten with the new data. If the
 * datasource is a table, then the existing data in the table is replaced.
 *
 * <p>ErrorIfExists: In the ErrorIfExists mode, an error is thrown if the data being written already
 * exists in the data source.
 *
 * <p>Ignore: In the Ignore mode, if the data already exists, the write operation is not expected to
 * update existing data.
 *
 * @since 1.1.0
 */
public enum SaveMode {
  Append,
  Overwrite,
  ErrorIfExists,
  Ignore
}
