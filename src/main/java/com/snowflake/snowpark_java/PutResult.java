package com.snowflake.snowpark_java;

/**
 * Represents the results of uploading a local file to a stage location.
 *
 * @since 1.2.0
 */
public class PutResult {
  private final com.snowflake.snowpark.PutResult result;

  PutResult(com.snowflake.snowpark.PutResult result) {
    this.result = result;
  }

  /**
   * Retrieves the source file name
   *
   * @since 1.2.0
   * @return A String
   */
  public String getSourceFileName() {
    return result.sourceFileName();
  }

  /**
   * Retrieves the target file name
   *
   * @since 1.2.0
   * @return A String
   */
  public String getTargetFileName() {
    return result.targetFileName();
  }

  /**
   * Retrieves the source file size in bytes
   *
   * @since 1.2.0
   * @return A long number
   */
  public long getSourceSizeBytes() {
    return result.sourceSizeBytes();
  }

  /**
   * Retrieves the target file size in bytes
   *
   * @since 1.2.0
   * @return A long number
   */
  public long getTargetSizeBytes() {
    return result.targetSizeBytes();
  }

  /**
   * Retrieves the source file compression status
   *
   * @since 1.2.0
   * @return A String
   */
  public String getSourceCompression() {
    return result.sourceCompression();
  }

  /**
   * Retrieves the target file compression status
   *
   * @since 1.2.0
   * @return A String
   */
  public String getTargetCompression() {
    return result.targetCompression();
  }

  /**
   * Retrieves the status
   *
   * @since 1.2.0
   * @return A String
   */
  public String getStatus() {
    return result.status();
  }

  /**
   * Retrieves the encryption status
   *
   * @since 1.2.0
   * @return A String
   */
  public String getEncryption() {
    return result.encryption();
  }

  /**
   * Retrieves the message
   *
   * @since 1.2.0
   * @return A String
   */
  public String getMessage() {
    return result.message();
  }
}
