package com.snowflake.snowpark_test

import com.snowflake.snowpark.TestUtils._
import com.snowflake.snowpark.{SNTestBase, SnowparkClientException, TestUtils}
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.apache.commons.io._

import java.io._
import java.nio.file.Files
import java.sql.Timestamp
import java.util.Locale
import scala.io.Source
import scala.sys.process._
import scala.util.Random

class FileOperationSuite extends SNTestBase {
  // Use lower case because LIST command always returns stage name with lower case.
  private val tempStage = randomStageName().toLowerCase(Locale.ENGLISH)

  override def beforeAll: Unit = {
    super.beforeAll()
    runQuery(s"create or replace temporary stage $tempStage", session)
  }

  override def afterAll(): Unit = {
    runQuery(s"drop stage if exists $tempStage", session)
    // Clean up temp directories
    removeFile(targetDirectoryPath, session)
    removeFile(sourceDirectoryPath, session)
    super.afterAll()
  }

  // Create a temp file and return file path
  private def createTempFile(
      prefix: String = "test_file_",
      suffix: String = ".csv",
      content: String = "abc, 123,\n"): String = {
    val file = File.createTempFile(prefix, suffix, sourceDirectoryFile)
    FileUtils.write(file, content)
    file.getCanonicalPath
  }

  // create test directory and test files
  private val sourceDirectoryFile = Files.createTempDirectory("snowpark_test_source_").toFile
  private val sourceDirectoryPath = escapePath(sourceDirectoryFile.getCanonicalPath)
  private val commonFilePrefix = "file"
  private val path1 = escapePath(createTempFile(prefix = commonFilePrefix + "_1_"))
  private val path2 = escapePath(createTempFile(prefix = commonFilePrefix + "_2_"))
  private val path3 = escapePath(createTempFile(prefix = commonFilePrefix + "_3_"))

  // create test target directory
  private val targetDirectoryFile = Files.createTempDirectory("snowpark_test_target_").toFile
  private val targetDirectoryPath = escapePath(targetDirectoryFile.getCanonicalPath)

  test("put() with one file") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"

    val firstResult = session.file.put(s"file://$path1", stageWithPrefix)
    assert(firstResult.length == 1)
    assert(firstResult(0).sourceFileName.equals(getFileName(path1)))
    assert(firstResult(0).targetFileName.equals(getFileName(path1) + ".gz"))
    assert(firstResult(0).sourceSizeBytes == 10L)
    assert(firstResult(0).targetSizeBytes == 30L)
    assert(firstResult(0).sourceCompression.equals("NONE"))
    assert(firstResult(0).targetCompression.equals("GZIP"))
    assert(firstResult(0).status.equals("UPLOADED"))
    assert(firstResult(0).encryption.equals("ENCRYPTED"))
    assert(firstResult(0).message.equals(""))

    // PUT another file with compression off
    val secondResult =
      session.file.put(s"file://$path2", stageWithPrefix, Map("AUTO_COMPRESS" -> "FALSE"))
    assert(secondResult(0).sourceFileName.equals(getFileName(path2)))
    assert(secondResult(0).targetFileName.equals(getFileName(path2)))
    assert(secondResult(0).sourceSizeBytes == 10L)
    assert(secondResult(0).targetSizeBytes == 10L)
    assert(secondResult(0).sourceCompression.equals("NONE"))
    assert(secondResult(0).targetCompression.equals("NONE"))
    assert(secondResult(0).status.equals("UPLOADED"))
    assert(secondResult(0).encryption.equals("ENCRYPTED"))
    assert(secondResult(0).message.equals(""))

    // PUT another file: without "file://" and "@" for localFileName and stageLocation
    // put() will add "file://" for localFileName, add "@" for stageLocation
    val thirdResult = session.file.put(path3, s"$tempStage/$stagePrefix/")
    assert(thirdResult(0).sourceFileName.equals(getFileName(path3)))
    assert(thirdResult(0).targetFileName.equals(getFileName(path3) + ".gz"))
    assert(thirdResult(0).sourceSizeBytes == 10L)
    assert(thirdResult(0).targetSizeBytes == 30L)
    assert(thirdResult(0).sourceCompression.equals("NONE"))
    assert(thirdResult(0).targetCompression.equals("GZIP"))
    assert(thirdResult(0).status.equals("UPLOADED"))
    assert(thirdResult(0).encryption.equals("ENCRYPTED"))
    assert(thirdResult(0).message.equals(""))
  }

  test("put() with one file twice") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"

    session.file.put(s"file://$path1", stageWithPrefix)
    // PUT the same file again.
    val secondResult = session.file.put(s"file://$path1", stageWithPrefix)
    assert(secondResult(0).sourceFileName.equals(getFileName(path1)))
    assert(secondResult(0).targetFileName.equals(getFileName(path1) + ".gz"))
    assert(secondResult(0).sourceSizeBytes == 10L)
    // On GCP, the files are not skipped if target file already exists
    assert(secondResult(0).targetSizeBytes == 0L || secondResult(0).targetSizeBytes == 30)
    assert(secondResult(0).sourceCompression.equals("NONE"))
    assert(secondResult(0).targetCompression.equals("GZIP"))
    assert(secondResult(0).status.equals("SKIPPED") || secondResult(0).status.equals("UPLOADED"))
    assert(
      secondResult(0).encryption.equals("") || secondResult(0).encryption.equals("ENCRYPTED"))
    assert(
      secondResult(0).message.isEmpty ||
        secondResult(0).message
          .contains("File with same destination name and checksum already exists"))
  }

  test("put() with one relative path file") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"

    try {
      // copy path1 to current directory for relative path test
      Seq("cp", path1, "target").!

      val relativeFile = getFileName(path1)
      val firstResult = session.file
        .put(escapePath(s"file://target${TestUtils.fileSeparator}$relativeFile"), stageWithPrefix)
      assert(firstResult.length == 1)
      assert(firstResult(0).sourceFileName.equals(relativeFile))
      assert(firstResult(0).targetFileName.equals(relativeFile + ".gz"))
      assert(firstResult(0).sourceSizeBytes == 10L)
      assert(firstResult(0).targetSizeBytes == 30L)
      assert(firstResult(0).sourceCompression.equals("NONE"))
      assert(firstResult(0).targetCompression.equals("GZIP"))
      assert(firstResult(0).status.equals("UPLOADED"))
      assert(firstResult(0).encryption.equals("ENCRYPTED"))
      assert(firstResult(0).message.equals(""))
    } finally {
      // remove copied file in current directory
      Seq("rm", "-fr", "target/" + getFileName(path1)).!
    }
  }

  test("put() with multiple files") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"
    val uploadFiles = "file://" + sourceDirectoryPath + escapePath(TestUtils.fileSeparator) +
      commonFilePrefix + "*"

    val firstResult = session.file.put(uploadFiles, stageWithPrefix, Map[String, String]())
    // Validate data for first time, the upload is uploaded corrected.
    assert(firstResult.length == 3)
    firstResult.map(row => assert(row.status.equals("UPLOADED")))

    // Execute the put command in second time, the upload is skipped by default.
    val secondResult = session.file.put(uploadFiles, stageWithPrefix, Map[String, String]())
    // Validate data for first time
    assert(secondResult.length == 3)
    // On GCP, the files are not skipped if target file already exists
    secondResult.map(row => assert(row.status.equals("SKIPPED") || row.status.equals("UPLOADED")))
  }

  test("put() negative test") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"
    val uploadFile = "file://" + sourceDirectoryPath + escapePath(TestUtils.fileSeparator) +
      "not_exists_file.txt"

    val fileNotExistException = intercept[SnowflakeSQLException] {
      session.file.put(uploadFile, stageWithPrefix)
    }
    assert(fileNotExistException.getMessage.contains("File not found"))

    val stageNotExistException = intercept[SnowflakeSQLException] {
      session.file.put(uploadFile, "@NOT_EXIST_STAGE_NAME_TEST")
    }
    assert(
      stageNotExistException.getMessage.contains("Stage") &&
        stageNotExistException.getMessage.contains("does not exist or not authorized."))
  }

  test("get() one file") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"

    try {
      session.file.put(s"file://$path1", stageWithPrefix)

      // call get() and check GET results
      val results = session.file
        .get(s"$stageWithPrefix${getFileName(path1)}.gz", s"file://$targetDirectoryPath")
        .sortBy(_.fileName)
      assert(
        results.length == 1 &&
          results(0).fileName.equals(s"$stagePrefix/${getFileName(path1)}.gz") &&
          results(0).sizeBytes == 30L &&
          results(0).status.equals("DOWNLOADED") &&
          results(0).encryption.equals("DECRYPTED") &&
          results(0).message.equals(""))

      // Check downloaded file
      assert(fileExists(s"$targetDirectoryPath/${getFileName(path1)}.gz"))
    } finally {
      // clean up downloaded file
      removeFile(s"$targetDirectoryPath/${getFileName(path1)}.gz", session)
    }
  }

  test("get() one file with simplified parameters") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"$tempStage/$stagePrefix/"

    try {
      session.file.put(s"file://$path1", stageWithPrefix)

      // call get() and check GET results
      // If targetDirectory doesn't start with  `file://`, get() will add it.
      // if stageLocation does't start with `@`, get() will add one
      val results = session.file
        .get(s"$tempStage/$stagePrefix/${getFileName(path1)}.gz", targetDirectoryPath)
        .sortBy(_.fileName)
      assert(
        results.length == 1 &&
          results(0).fileName.equals(s"$stagePrefix/${getFileName(path1)}.gz") &&
          results(0).sizeBytes == 30L &&
          results(0).status.equals("DOWNLOADED") &&
          results(0).encryption.equals("DECRYPTED") &&
          results(0).message.equals(""))

      // Check downloaded file
      assert(fileExists(s"$targetDirectoryPath/${getFileName(path1)}.gz"))
    } finally {
      // clean up downloaded file
      removeFile(s"$targetDirectoryPath/${getFileName(path1)}.gz", session)
    }
  }

  test("get() multiple files") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix" // End without "/"

    try {
      session.file.put(s"file://$path1", stageWithPrefix)
      session.file.put(s"file://$path2", stageWithPrefix)
      session.file.put(s"file://$path3", stageWithPrefix, Map("AUTO_COMPRESS" -> "FALSE"))

      // call get() and check GET results
      val results = session.file
        .get(stageWithPrefix, s"file://$targetDirectoryPath")
        .sortBy(_.fileName)
      assert(results.length == 3)
      assert(results(0).fileName.equals(s"$stagePrefix/${getFileName(path1)}.gz"))
      assert(results(1).fileName.equals(s"$stagePrefix/${getFileName(path2)}.gz"))
      assert(results(2).fileName.equals(s"$stagePrefix/${getFileName(path3)}"))
      assert(results(0).sizeBytes == 30L)
      assert(results(1).sizeBytes == 30L)
      assert(results(2).sizeBytes == 10L)
      results.foreach(
        r =>
          assert(
            r.status.equals("DOWNLOADED") &&
              r.encryption.equals("DECRYPTED") &&
              r.message.equals("")))

      // Check downloaded files
      assert(fileExists(s"$targetDirectoryPath/${getFileName(path1)}.gz"))
      assert(fileExists(s"$targetDirectoryPath/${getFileName(path2)}.gz"))
      assert(fileExists(s"$targetDirectoryPath/${getFileName(path3)}"))
    } finally {
      // clean up downloaded files
      removeFile(s"$targetDirectoryPath/${getFileName(path1)}.gz", session)
      removeFile(s"$targetDirectoryPath/${getFileName(path2)}.gz", session)
      removeFile(s"$targetDirectoryPath/${getFileName(path3)}", session)
    }
  }

  test("get() with PATTERN and relative target directory") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/" // End with "/"

    try {
      session.file.put(s"file://$path1", stageWithPrefix)
      session.file.put(s"file://$path2", stageWithPrefix)
      session.file.put(s"file://$path3", stageWithPrefix, Map("AUTO_COMPRESS" -> "FALSE"))

      // call get() and check GET results
      val results = session.file
        .get(stageWithPrefix, ".", Map("PATTERN" -> s"'.*${commonFilePrefix}_.*.csv.gz'"))
        .sortBy(_.fileName)
      assert(results.length == 2)
      assert(results(0).fileName.equals(s"$stagePrefix/${getFileName(path1)}.gz"))
      assert(results(1).fileName.equals(s"$stagePrefix/${getFileName(path2)}.gz"))
      assert(results(0).sizeBytes == 30L)
      assert(results(1).sizeBytes == 30L)
      results.foreach(
        r =>
          assert(
            r.status.equals("DOWNLOADED") &&
              r.encryption.equals("DECRYPTED") &&
              r.message.equals("")))

      // Check downloaded files
      assert(fileExists(getFileName(path1) + ".gz"))
      assert(fileExists(getFileName(path2) + ".gz"))
    } finally {
      // clean up downloaded files
      removeFile(getFileName(path1) + ".gz", session)
      removeFile(getFileName(path2) + ".gz", session)
    }
  }

  test("get() negative test") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"

    // Stage name doesn't exist, raise exception.
    val stageNotExistException = intercept[SnowflakeSQLException] {
      session.file.get("@NOT_EXIST_STAGE_NAME_TEST", TestUtils.tempDirWithEscape)
    }
    assert(
      stageNotExistException.getMessage.contains("Stage") &&
        stageNotExistException.getMessage.contains("does not exist or not authorized."))

    // If stage name exists but prefix doesn't exist, download nothing
    var getResults = session.file.get(s"@$tempStage/not_exist_prefix_test/", ".")
    assert(getResults.isEmpty)

    // If target directory doesn't exist, create the directory and download files
    val not_exist_target_dir = "not_exist_target_" + Random.nextLong()
    try {
      val putResults = session.file.put(path1, stageWithPrefix)
      assert(putResults.length == 1 && putResults(0).status.equals("UPLOADED"))
      getResults = session.file.get(stageWithPrefix, s"$not_exist_target_dir/test2")
      assert(getResults.length == 1 && getResults(0).status.equals("DOWNLOADED"))
    } finally {
      // clean up downloaded file
      removeFile(not_exist_target_dir, session)
    }
  }

  test("get() negative test: file name collision") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"

    try {
      // Put the same file to 2 PREFIX
      session.file.put(s"file://$path1", stageWithPrefix + "prefix_1")
      session.file.put(s"file://$path1", stageWithPrefix + "prefix_2")

      // call get() and check GET results
      val results = session.file
        .get(stageWithPrefix, s"file://$targetDirectoryPath")
        .sortBy(_.message)
      assert(results.length == 2)
      assert(
        results(0).sizeBytes == 30L &&
          results(0).status.equals("DOWNLOADED") &&
          results(0).message.equals(""))
      // The error message is like:
      // prefix/prefix_1/file_1.csv.gz has same name as prefix/prefix_2/file_1.csv.gz
      // GET on GCP doesn't detect this download collision.
      assert(
        (results(1).sizeBytes == 0L &&
          results(1).status.equals("COLLISION") &&
          results(1).message.contains("has same name as")) ||
          (results(1).sizeBytes == 30L &&
            results(1).status.equals("DOWNLOADED") &&
            results(1).message.equals("")))

      // Check downloaded files
      assert(fileExists(targetDirectoryPath + "/" + (getFileName(path1) + ".gz")))
    } finally {
      // clean up downloaded file
      removeFile(targetDirectoryPath + "/" + (getFileName(path1) + ".gz"), session)
    }
  }

  test("test quoted local file name") {
    val stagePrefix = "prefix_" + TestUtils.randomString(5)
    val stageWithPrefix = s"@$tempStage/$stagePrefix/"
    val specialDirectory = Files.createTempDirectory("dir with!_").toFile.getCanonicalPath

    try {
      val specialPath1 = createTempFile(prefix = commonFilePrefix + "_!")
      val specialPath2 = createTempFile(prefix = commonFilePrefix + "_ ")

      val put1 =
        session.file.put(s"'file://${TestUtils.escapePath(specialPath1)}'", stageWithPrefix)
      assert(put1.length == 1)
      val put2 =
        session.file.put(s"'file://${TestUtils.escapePath(specialPath2)}'", stageWithPrefix)
      assert(put2.length == 1)

      var directoryFile = new File(specialDirectory)
      assert(directoryFile.list().isEmpty)
      val get1 =
        session.file.get(stageWithPrefix, s"'file://${TestUtils.escapePath(specialDirectory)}'")
      assert(get1.length == 2)
      directoryFile = new File(specialDirectory)
      assert(directoryFile.list().length == 2)
    } finally {
      removeFile(specialDirectory, session)
    }
  }

  test("Test uploadStream and downloadStream") {
    var stagePrefix = "prefix_" + TestUtils.randomString(5)
    var fileName = s"streamFile_${TestUtils.randomString(5)}.csv"

    // Test without @ prefix
    testStreamRoundTrip(
      s"$tempStage/$stagePrefix/$fileName",
      s"$tempStage/$stagePrefix/$fileName",
      false)

    // Test with @ prefix
    stagePrefix = "prefix_" + TestUtils.randomString(5)
    testStreamRoundTrip(
      s"@$tempStage/$stagePrefix/$fileName",
      s"@$tempStage/$stagePrefix/$fileName",
      false)

    // Test compression with .gz extension
    stagePrefix = "prefix_" + TestUtils.randomString(5)
    testStreamRoundTrip(
      s"$tempStage/$stagePrefix/$fileName.gz",
      s"$tempStage/$stagePrefix/$fileName.gz",
      true)

    // Test compression without .gz extension
    stagePrefix = "prefix_" + TestUtils.randomString(5)
    testStreamRoundTrip(
      s"$tempStage/$stagePrefix/$fileName",
      s"$tempStage/$stagePrefix/$fileName.gz",
      true)

    // Test no path
    fileName = s"streamFile_${TestUtils.randomString(5)}.csv"
    testStreamRoundTrip(s"$tempStage/$fileName", s"$tempStage/$fileName.gz", true)

    // Test fully qualified stage path
    val database = session.getCurrentDatabase.get
    val schema = session.getCurrentSchema.get
    fileName = s"streamFile_${TestUtils.randomString(5)}.csv"
    testStreamRoundTrip(
      s"$database.$schema.$tempStage/$fileName",
      s"$database.$schema.$tempStage/$fileName.gz",
      true)

    fileName = s"streamFile_${TestUtils.randomString(5)}.csv"
    testStreamRoundTrip(s"$schema.$tempStage/$fileName", s"$schema.$tempStage/$fileName.gz", true)

  }

  test("Test uploadStream and downloadStream separate schema") {
    val randomNewSchema = randomName()
    val oldSchema = session.getCurrentSchema.get
    session.sql(s"CREATE SCHEMA $randomNewSchema").collect()
    session.sql(s"USE SCHEMA $oldSchema").collect()
    assert(session.getCurrentSchema.get == oldSchema)
    runQuery(s"create or replace temporary stage $randomNewSchema.$tempStage", session)
    try {
      val fileName = s"streamFile_${TestUtils.randomString(5)}.csv"
      testStreamRoundTrip(
        s"$randomNewSchema.$tempStage/$fileName",
        s"$randomNewSchema.$tempStage/$fileName.gz",
        true)
    } finally {
      session.sql(s"DROP SCHEMA $randomNewSchema").collect()
    }
  }

  test("Negative test uploadStream and downloadStream") {

    // Test no file name
    assertThrows[SnowparkClientException](
      testStreamRoundTrip(s"$tempStage", s"$tempStage", false))

    // Test no file name
    assertThrows[SnowparkClientException](
      testStreamRoundTrip(s"$tempStage/", s"$tempStage/", false))

    var stagePrefix = "prefix_" + TestUtils.randomString(5)
    var fileName = s"streamFile_${TestUtils.randomString(5)}.csv"

    // Test upload no stage
    assertThrows[SnowflakeSQLException](
      testStreamRoundTrip(s"nonExistStage/$fileName", s"nonExistStage/$fileName", false))

    // Test download no stage
    assertThrows[SnowflakeSQLException](
      testStreamRoundTrip(s"$tempStage/$fileName", s"nonExistStage/$fileName", false))

    stagePrefix = "prefix_" + TestUtils.randomString(5)
    fileName = s"streamFile_${TestUtils.randomString(5)}.csv"

    // Test download no file
    assertThrows[SnowparkClientException](
      testStreamRoundTrip(s"$tempStage/$fileName", s"$tempStage/$stagePrefix/$fileName", false))

  }

  private def testStreamRoundTrip(
      uploadLocation: String,
      downloadLocation: String,
      compress: Boolean): Unit = {
    val fileContent = "test, file, csv"
    session.file.uploadStream(
      uploadLocation,
      new ByteArrayInputStream(fileContent.getBytes),
      compress)
    assert(
      Source
        .fromInputStream(session.file.downloadStream(downloadLocation, compress))
        .mkString
        .equals(fileContent))
  }

}
