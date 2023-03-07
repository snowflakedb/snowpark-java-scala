package com.snowflake.snowpark_java;

import com.snowflake.snowpark_java.types.InternalUtils;
import com.snowflake.snowpark_java.types.StructType;

/**
 * Represents the results of writing data from a DataFrame to a file in a stage.
 *
 * <p>To write the data, the DataFrameWriter effectively executes the COPY INTO {@literal
 * <location>} command. WriteFileResult encapsulates the output returned by the command:
 *
 * <ul>
 *   <li>rows represents the rows of output from the command
 *   <li>schema defines the schema for these rows.
 * </ul>
 *
 * <p>For example, if the DETAILED_OUTPUT option is TRUE, each row contains a `file_name`,
 * `file_size`, and `row_count` field. `schema` defines the names and types of these fields. If the
 * DETAILED_OUTPUT option is not specified (meaning that the option is FALSE), each row contains a
 * `rows_unloaded`, `input_bytes`, and `output_bytes` field.
 *
 * @since 1.5.0
 */
public class WriteFileResult {
  private final com.snowflake.snowpark.WriteFileResult result;

  WriteFileResult(com.snowflake.snowpark.WriteFileResult result) {
    this.result = result;
  }

  /**
   * Retrieve the output rows produced by the COPY INTO {@literal <location>} command.
   *
   * @return An array of {@code Row}
   * @since 1.5.0
   */
  public Row[] getRows() {
    com.snowflake.snowpark.Row[] rows = this.result.rows();
    Row[] result = new Row[rows.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = new Row(rows[i]);
    }
    return result;
  }

  /**
   * Retrieve the names and types of the fields in the output rows.
   *
   * @return A {@code StructType}
   * @since 1.5.0
   */
  public StructType getSchema() {
    return InternalUtils.createStructType(this.result.schema());
  }
}
