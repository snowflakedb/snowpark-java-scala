package com.snowflake.snowpark_java;

/**
 * Represents the results of downloading a file from a stage location to the local file system.
 *
 * <p>NOTE: {@code fileName} is the relative path to the file on the stage. For example, if you
 * download `@myStage/prefix1/file1.csv.gz`, {@code fileName} is `prefix1/file1.csv.gz`.
 *
 * @since 1.2.0
 */
public class GetResult {
  private final com.snowflake.snowpark.GetResult result;

  GetResult(com.snowflake.snowpark.GetResult result) {
    this.result = result;
  }

  /**
   * Retrieves the file name
   *
   * @return A String
   * @since 1.2.0
   */
  public String getFileName() {
    return result.fileName();
  }

  /**
   * Retrieves the file size in bytes
   *
   * @return A long number
   * @since 1.2.0
   */
  public long getSizeBytes() {
    return result.sizeBytes();
  }

  /**
   * Retrieves the file status
   *
   * @return A String
   * @since 1.2.0
   */
  public String getStatus() {
    return result.status();
  }

  /**
   * Retrieves the encryption status
   *
   * @return A String
   * @since 1.2.0
   */
  public String getEncryption() {
    return result.encryption();
  }

  /**
   * Retrieves the message
   *
   * @return A String
   * @since 1.2.0
   */
  public String getMessage() {
    return result.message();
  }
}
