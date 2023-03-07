package com.snowflake.snowpark_java;

import com.snowflake.snowpark.internal.JavaUtils;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides methods for working on files in a stage.
 *
 * <p>To access an object of this class, use {@code Session.file}.
 *
 * @since 1.2.0
 */
public class FileOperation {
  private final com.snowflake.snowpark.FileOperation fileOperation;

  FileOperation(com.snowflake.snowpark.FileOperation file) {
    this.fileOperation = file;
  }

  /**
   * Uploads the local files specified by {@code localFileName} to the stage location specified in
   * {@code stageLocation}.
   *
   * <p>This method returns the results as an Array of {@code PutResult} objects (one for each
   * file). Each object represents the results of uploading a file.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, String> options = new HashMap<>();
   * options.put("AUTO_COMPRESS", "FALSE");
   * session.file().put("file://file_path", "@stage_name", options);
   * }</pre>
   *
   * @param localFileName The path to the local file(s) to upload. Specify the path in the following
   *     format: `file://'path_to_file'/'filename'`. (The `file://` prefix is optional.) To match
   *     multiple files in the path, you can specify the wildcard characters `*` and `?`.
   * @param stageLocation The stage (and prefix) where you want to upload the file(s). The `@`
   *     prefix is optional.
   * @param options A Map containing the names and values of optional <a
   *     href="https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters">parameters</a>
   *     for the PUT command.
   * @return An Array of {@code PutResult} objects (one object for each file uploaded).
   * @since 1.2.0
   */
  public PutResult[] put(String localFileName, String stageLocation, Map<String, String> options) {
    com.snowflake.snowpark.PutResult[] result =
        fileOperation.put(
            localFileName, stageLocation, JavaUtils.javaStringStringMapToScala(options));
    PutResult[] javaResult = new PutResult[result.length];
    for (int i = 0; i < javaResult.length; i++) {
      javaResult[i] = new PutResult(result[i]);
    }
    return javaResult;
  }

  /**
   * Uploads the local files specified by {@code localFileName} to the stage location specified in
   * {@code stageLocation}.
   *
   * <p>This method returns the results as an Array of {@code PutResult} objects (one for each
   * file). Each object represents the results of uploading a file.
   *
   * <p>For example:
   *
   * <pre>{@code
   * session.file().put("file://file_path", "@stage_name");
   * }</pre>
   *
   * @param localFileName The path to the local file(s) to upload. Specify the path in the following
   *     format: `file://'path_to_file'/'filename'`. (The `file://` prefix is optional.) To match
   *     multiple files in the path, you can specify the wildcard characters `*` and `?`.
   * @param stageLocation The stage (and prefix) where you want to upload the file(s). The `@`
   *     prefix is optional.
   * @return An Array of {@code PutResult} objects (one object for each file uploaded).
   * @since 1.2.0
   */
  public PutResult[] put(String localFileName, String stageLocation) {
    return put(localFileName, stageLocation, new HashMap<>());
  }

  /**
   * Downloads the specified files from a path in a stage (specified by {@code stageLocation}) to
   * the local directory specified by {@code targetLocation}.
   *
   * <p>This method returns the results as an Array of {@code GetResult} objects (one for each
   * file). Each object represents the results of downloading a file.
   *
   * <p>For example:
   *
   * <pre>{@code
   * Map<String, String> options = new HashMap<>();
   * options.put("PATTERN", "'.*.csv'");
   * session.file().get("@stage_name/", "file:///tmp/", options);
   * }</pre>
   *
   * @param stageLocation The location (a directory or filename on a stage) from which you want to
   *     download the files. The `@` prefix is optional.
   * @param targetDirectory The path to the local directory where the file(s) should be downloaded.
   *     Specify the path in the following format: `file://'path_to_file'/'filename'`. If {@code
   *     targetDirectory} does not already exist, the method creates the directory.
   * @param options A Map containing the names and values of optional <a
   *     href="https://docs.snowflake.com/en/sql-reference/sql/get.html#optional-parameters">parameters</a>
   *     for the GET command.
   * @return An Array of PutResult objects (one object for each file downloaded).
   * @since 1.2.0
   */
  public GetResult[] get(
      String stageLocation, String targetDirectory, Map<String, String> options) {
    com.snowflake.snowpark.GetResult[] results =
        this.fileOperation.get(
            stageLocation, targetDirectory, JavaUtils.javaStringStringMapToScala(options));
    GetResult[] javaResult = new GetResult[results.length];
    for (int i = 0; i < javaResult.length; i++) {
      javaResult[i] = new GetResult(results[i]);
    }
    return javaResult;
  }

  /**
   * Downloads the specified files from a path in a stage (specified by {@code stageLocation}) to
   * the local directory specified by {@code targetLocation}.
   *
   * <p>This method returns the results as an Array of {@code GetResult} objects (one for each
   * file). Each object represents the results of downloading a file.
   *
   * <p>For example:
   *
   * <pre>{@code
   * getSession().file().get("@stage_name/", "file:///tmp/");
   * }</pre>
   *
   * @param stageLocation The location (a directory or filename on a stage) from which you want to
   *     download the files. The `@` prefix is optional.
   * @param targetDirectory The path to the local directory where the file(s) should be downloaded.
   *     Specify the path in the following format: `file://'path_to_file'/'filename'`. If {@code
   *     targetDirectory} does not already exist, the method creates the directory.
   * @return An Array of PutResult objects (one object for each file downloaded).
   * @since 1.2.0
   */
  public GetResult[] get(String stageLocation, String targetDirectory) {
    return get(stageLocation, targetDirectory, new HashMap<>());
  }

  /**
   * Method to compress data from a stream and upload it at a stage location. The data will be
   * uploaded as one file. No splitting is done in this method.
   *
   * <p>caller is responsible for releasing the inputStream after the method is called.
   *
   * @param stageLocation Full stage path to the file
   * @param inputStream Input stream from which the data will be uploaded
   * @param compress Compress data or not before uploading stream
   * @since 1.4.0
   */
  public void uploadStream(String stageLocation, InputStream inputStream, boolean compress) {
    this.fileOperation.uploadStream(stageLocation, inputStream, compress);
  }

  /**
   * Download file from the given stage and return an input stream
   *
   * @param stageLocation Full stage path to the file
   * @param decompress True if file compressed
   * @return An InputStream object
   * @since 1.4.0
   */
  public InputStream downloadStream(String stageLocation, boolean decompress) {
    return this.fileOperation.downloadStream(stageLocation, decompress);
  }
}
