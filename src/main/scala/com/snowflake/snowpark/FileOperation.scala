package com.snowflake.snowpark

import com.snowflake.snowpark.internal.Utils.parseStageFileLocation
import com.snowflake.snowpark.internal.{ErrorMessage, Logging, Utils}

import java.io.InputStream

// Define 4 kinds of file operation commands
private[snowpark] object FileOperationCommand extends Enumeration {
  type FileOperationCommand = Value

  val PutCommand = Value("Put")
  val GetCommand = Value("Get")
  val RemoveCommand = Value("Remove")
  val ListCommand = Value("List")
}
import FileOperationCommand._

/** Provides methods for working on files in a stage.
  *
  * To access an object of this class, use [[Session.file]].
  *
  * For example:
  * {{{
  *   // Upload a file to a stage.
  *   session.file.put("file:///tmp/file1.csv", "@myStage/prefix1")
  *   // Download a file from a stage.
  *   session.file.get("@myStage/prefix1/file1.csv", "file:///tmp")
  * }}}
  *
  * @since 0.4.0
  */
final class FileOperation(session: Session) extends Logging {

  /** Uploads the local files specified by {@code localFileName} to the stage location specified in
    * {@code stageLocation} .
    *
    * This method returns the results as an Array of [[PutResult]] objects (one for each file). Each
    * object represents the results of uploading a file.
    *
    * For example:
    * {{{
    *   // Upload a file to a stage without compressing the file.
    *   val putOptions = Map("AUTO_COMPRESS" -> "FALSE")
    *   val res1 = session.file.put("file:///tmp/file1.csv", "@myStage", putOptions)
    *
    *   // Upload the CSV files in /tmp with names that start with "file".
    *   // You can use the wildcard characters "*" and "?" to match multiple files.
    *   val res2 = session.file.put("file:///tmp/file*.csv", "@myStage/prefix2")
    * }}}
    *
    * @param localFileName
    *   The path to the local file(s) to upload. Specify the path in the following format:
    *   `file://<path_to_file>/<filename>`. (The `file://` prefix is optional.) To match multiple
    *   files in the path, you can specify the wildcard characters `*` and `?`.
    * @param stageLocation
    *   The stage (and prefix) where you want to upload the file(s). The `@` prefix is optional.
    * @param options
    *   A Map containing the names and values of optional
    *   [[https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters parameters]]
    *   for the PUT command.
    * @return
    *   An Array of [[PutResult]] objects (one object for each file uploaded).
    * @since 0.4.0
    */
  def put(
      localFileName: String,
      stageLocation: String,
      options: Map[String, String] = Map()
  ): Array[PutResult] = {
    val plan =
      session.plans.fileOperationPlan(
        PutCommand,
        Utils.normalizeLocalFile(localFileName),
        Utils.normalizeStageLocation(stageLocation),
        options
      )

    DataFrame(session, plan).collect().map { row =>
      PutResult(
        row.getString(0),
        row.getString(1),
        row.getDecimal(2).longValue(),
        row.getDecimal(3).longValue(),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getString(7),
        row.getString(8)
      )
    }
  }

  /** Downloads the specified files from a path in a stage (specified by {@code stageLocation} ) to
    * the local directory specified by {@code targetLocation} .
    *
    * This method returns the results as an Array of [[GetResult]] objects (one for each file). Each
    * object represents the results of downloading a file.
    *
    * For example:
    * {{{
    *   // Upload files to a stage.
    *   session.file.put("file:///tmp/file_1.csv", "@myStage/prefix2")
    *   session.file.put("file:///tmp/file_2.csv", "@myStage/prefix2")
    *
    *   // Download one file from a stage.
    *   val res1 = session.file.get("@myStage/prefix2/file_1.csv", "file:///tmp/target")
    *   // Download all the files from @myStage/prefix2.
    *   val res2 = session.file.get("@myStage/prefix2", "file:///tmp/target2")
    *   // Download files with names that match a regular expression pattern.
    *   val getOptions = Map("PATTERN" -> s"'.*file_.*.csv.gz'")
    *   val res3 = session.file.get("@myStage/prefix2", "file:///tmp/target3", getOptions)
    * }}}
    *
    * @param stageLocation
    *   The location (a directory or filename on a stage) from which you want to download the files.
    *   The `@` prefix is optional.
    * @param targetDirectory
    *   The path to the local directory where the file(s) should be downloaded. Specify the path in
    *   the following format: `file://<path_to_file>/<filename>`. If {@code targetDirectory} does
    *   not already exist, the method creates the directory.
    * @param options
    *   A Map containing the names and values of optional
    *   [[https://docs.snowflake.com/en/sql-reference/sql/get.html#optional-parameters parameters]]
    *   for the GET command.
    * @return
    *   An Array of [[PutResult]] objects (one object for each file downloaded).
    * @since 0.4.0
    */
  def get(
      stageLocation: String,
      targetDirectory: String,
      options: Map[String, String] = Map()
  ): Array[GetResult] = {
    val plan =
      session.plans.fileOperationPlan(
        GetCommand,
        Utils.normalizeLocalFile(targetDirectory),
        Utils.normalizeStageLocation(stageLocation),
        options
      )

    DataFrame(session, plan).collect().map { row =>
      GetResult(
        row.getString(0),
        row.getDecimal(1).longValue(),
        row.getString(2),
        row.getString(3),
        row.getString(4)
      )
    }
  }

  /** Method to compress data from a stream and upload it at a stage location. The data will be
    * uploaded as one file. No splitting is done in this method.
    *
    * <p>caller is responsible for releasing the inputStream after the method is called.
    *
    * @param stageLocation
    *   Full stage path to the file
    * @param inputStream
    *   Input stream from which the data will be uploaded
    * @param compress
    *   Compress data or not before uploading stream
    * @since 1.4.0
    */
  def uploadStream(stageLocation: String, inputStream: InputStream, compress: Boolean): Unit = {
    val (stageName, pathName, fileName) = parseStageFileLocation(stageLocation)
    session.conn.uploadStream(stageName, pathName, inputStream, fileName, compress)
  }

  /** Download file from the given stage and return an input stream
    *
    * @param stageLocation
    *   Full stage path to the file
    * @param decompress
    *   True if file compressed
    * @return
    *   An InputStream object
    * @since 1.4.0
    */
  def downloadStream(stageLocation: String, decompress: Boolean): InputStream = {
    val (stageName, pathName, fileName) = parseStageFileLocation(stageLocation)
    // TODO: No need to check file existence once this is fixed: SNOW-565154
    if (!stageFileExists(stageLocation)) {
      throw ErrorMessage.MISC_INVALID_STAGE_LOCATION(stageLocation, "Stage file does not exist")
    }
    val pathNameWithPrefix = if (pathName.isEmpty) pathName else s"/$pathName"

    // Added retry here to handle GCS 403 failing issue
    var resultStream: InputStream = null
    Utils.withRetry(
      session.maxFileDownloadRetryCount,
      s"Download stream from stage: $stageName, file: " +
        s"$pathNameWithPrefix/$fileName, decompress: $decompress"
    ) {
      resultStream =
        session.conn.downloadStream(stageName, s"$pathNameWithPrefix/$fileName", decompress)
    }
    resultStream
  }

  private def stageFileExists(stageLocation: String): Boolean = {
    val normalizedLocation = Utils.normalizeStageLocation(stageLocation)
    session.sql(s"ls $normalizedLocation").collect().nonEmpty
  }

}

/** Represents the results of uploading a local file to a stage location.
  *
  * @since 0.4.0
  */
case class PutResult(
    sourceFileName: String,
    targetFileName: String,
    sourceSizeBytes: Long,
    targetSizeBytes: Long,
    sourceCompression: String,
    targetCompression: String,
    status: String,
    encryption: String,
    message: String
)

/** Represents the results of downloading a file from a stage location to the local file system.
  *
  * NOTE: {@code fileName} is the relative path to the file on the stage. For example, if you
  * download `@myStage/prefix1/file1.csv.gz`, {@code fileName} is `prefix1/file1.csv.gz`.
  *
  * @since 0.4.0
  */
case class GetResult(
    fileName: String,
    sizeBytes: Long,
    status: String,
    encryption: String,
    message: String
)
