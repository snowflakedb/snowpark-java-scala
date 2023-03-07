package com.snowflake.snowpark

/**
 * SaveMode configures the behavior when data is written from
 * a DataFrame to a data source using a [[DataFrameWriter]]
 * instance.
 * @since 0.1.0
 */
object SaveMode {

  def apply(mode: String): SaveMode =
    // scalastyle:off
    mode.toUpperCase match {
      case "APPEND" => Append
      case "OVERWRITE" => Overwrite
      case "ERRORIFEXISTS" => ErrorIfExists
      case "IGNORE" => Ignore
    }
  // scalastyle:on
  /**
   * In the Append mode, new data is appended to the datasource.
   * @since 0.1.0
   */
  object Append extends SaveMode

  /**
   * In the Overwrite mode, existing data is overwritten with the new data. If
   * the datasource is a table, then the existing data in the table is replaced.
   * @since 0.1.0
   */
  object Overwrite extends SaveMode

  /**
   * In the ErrorIfExists mode, an error is thrown if the data being written
   * already exists in the data source.
   * @since 0.1.0
   */
  object ErrorIfExists extends SaveMode

  /**
   * In the Ignore mode, if the data already exists, the write operation is
   * not expected to update existing data.
   * @since 0.1.0
   */
  object Ignore extends SaveMode
}

/**
 * Please refer to the companion [[SaveMode$]] object.
 * @since 0.1.0
 */
sealed trait SaveMode {
  override def toString: String = this.getClass.getSimpleName.stripSuffix("$")
}
