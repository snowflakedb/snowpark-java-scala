package com.snowflake.snowpark_test;

import static org.junit.Assert.assertThrows;

import com.snowflake.snowpark.SnowparkClientException;
import com.snowflake.snowpark.TestUtils;
import com.snowflake.snowpark_java.GetResult;
import com.snowflake.snowpark_java.PutResult;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.Test;

public class JavaFileOperationSuite extends TestBase {
  public static String getTempDir() throws IOException {
    String path = Files.createTempDirectory("snowpark_test_source_").toFile().getCanonicalPath();
    return TestUtils.escapePath(path);
  }

  @Test
  public void put1() {
    String stageName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTempStage(stageName);
      String filePath = this.getClass().getResource("/" + TestFiles.testFileCsv).getPath();
      PutResult[] result = getSession().file().put(filePath, stageName);
      checkAnswer(
          getSession().read().schema(schema).csv("@" + stageName),
          new Row[] {Row.create(1, "one", 1.2), Row.create(2, "two", 2.2)},
          false);
      assert result.length == 1;
      assert result[0].getSourceFileName().equals(TestFiles.testFileCsv);
      assert result[0].getTargetFileName().equals(TestFiles.testFileCsv + ".gz");
      assert result[0].getSourceSizeBytes() == 19;
      assert result[0].getTargetSizeBytes() == 39;
      assert result[0].getSourceCompression().equals("NONE");
      assert result[0].getTargetCompression().equals("GZIP");
      assert result[0].getStatus().equals("UPLOADED");
      assert result[0].getEncryption().equals("ENCRYPTED");
      assert result[0].getMessage().equals("");
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void put2() {
    String stageName = randomName();
    StructType schema =
        StructType.create(
            new StructField("num", DataTypes.IntegerType),
            new StructField("str", DataTypes.StringType),
            new StructField("double", DataTypes.DoubleType));
    try {
      createTempStage(stageName);
      Map<String, String> options = new HashMap<>();
      options.put("AUTO_COMPRESS", "FALSE");
      String filePath = this.getClass().getResource("/" + TestFiles.testFileCsv).getPath();
      PutResult[] result = getSession().file().put(filePath, stageName, options);
      checkAnswer(
          getSession().read().schema(schema).csv("@" + stageName),
          new Row[] {Row.create(1, "one", 1.2), Row.create(2, "two", 2.2)},
          false);
      assert result.length == 1;
      assert result[0].getSourceFileName().equals(TestFiles.testFileCsv);
      assert result[0].getTargetFileName().equals(TestFiles.testFileCsv);
      assert result[0].getSourceSizeBytes() == 19;
      assert result[0].getTargetSizeBytes() == 19;
      assert result[0].getSourceCompression().equals("NONE");
      assert result[0].getTargetCompression().equals("NONE");
      assert result[0].getStatus().equals("UPLOADED");
      assert result[0].getEncryption().equals("ENCRYPTED");
      assert result[0].getMessage().equals("");
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void get() throws IOException {
    String stageName = randomName();
    try {
      createTempStage(stageName);
      String filePath = this.getClass().getResource("/" + TestFiles.testFileCsv).getPath();
      getSession().file().put(filePath, stageName);
      Map<String, String> putOptions = new HashMap<>();
      putOptions.put("AUTO_COMPRESS", "FALSE");
      getSession().file().put(filePath, stageName, putOptions);
      Map<String, String> getOptions = new HashMap<>();
      getOptions.put("PATTERN", "'.*.csv'");
      getSession()
          .file()
          .get("@" + stageName + "/" + TestFiles.testFileCsv, "file://" + getTempDir(), getOptions);
      GetResult[] results =
          getSession()
              .file()
              .get(
                  "@" + stageName + "/" + TestFiles.testFileCsv,
                  "file://" + getTempDir(),
                  getOptions);
      assert results.length == 1;
      assert results[0].getFileName().equals(TestFiles.testFileCsv);
      assert results[0].getSizeBytes() == 19;
      assert results[0].getStatus().equals("DOWNLOADED");
      assert results[0].getEncryption().equals("DECRYPTED");
      assert results[0].getMessage().equals("");

      // get without option
      results =
          getSession()
              .file()
              .get("@" + stageName + "/" + TestFiles.testFileCsv, "file://" + getTempDir());
      assert results.length == 2;
      assert results[0].getFileName().equals(TestFiles.testFileCsv)
          || results[0].getFileName().equals(TestFiles.testFileCsv + ".gz");
      assert results[0].getSizeBytes() == 19 || results[0].getSizeBytes() == 39;
      assert results[0].getStatus().equals("DOWNLOADED");
      assert results[0].getEncryption().equals("DECRYPTED");
      assert results[0].getMessage().equals("");
    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void testStreamRoundTrip() {
    String stageName = TestUtils.randomStageName();
    try {
      createTempStage(stageName);
      String stagePrefix = "prefix_" + TestUtils.randomString(5);
      String fileName = "streamFile_" + TestUtils.randomString(5) + ".csv";

      // Test without @ prefix
      testStreamRoundTrip(
          stageName + "/" + stagePrefix + "/" + fileName,
          stageName + "/" + stagePrefix + "/" + fileName,
          false);

      // Test with @ prefix
      stagePrefix = "prefix_" + TestUtils.randomString(5);
      testStreamRoundTrip(
          "@" + stageName + "/" + stagePrefix + "/" + fileName,
          "@" + stageName + "/" + stagePrefix + "/" + fileName,
          false);

      // Test compression with .gz extension
      stagePrefix = "prefix_" + TestUtils.randomString(5);
      testStreamRoundTrip(
          stageName + "/" + stagePrefix + "/" + fileName + ".gz",
          stageName + "/" + stagePrefix + "/" + fileName + ".gz",
          true);

      // Test compression without .gz extension
      stagePrefix = "prefix_" + TestUtils.randomString(5);
      testStreamRoundTrip(
          stageName + "/" + stagePrefix + "/" + fileName,
          stageName + "/" + stagePrefix + "/" + fileName + ".gz",
          true);

      // Test no path
      fileName = "streamFile_" + TestUtils.randomString(5) + ".csv";
      testStreamRoundTrip(stageName + "/" + fileName, stageName + "/" + fileName + ".gz", true);

      // Test fully qualified stage path
      String database = getSession().getCurrentDatabase().get();
      String schema = getSession().getCurrentSchema().get();
      fileName = "streamFile_" + TestUtils.randomString(5) + ".csv";
      testStreamRoundTrip(
          database + "." + schema + "." + stageName + "/" + fileName,
          database + "." + schema + "." + stageName + "/" + fileName + ".gz",
          true);

      fileName = "streamFile_" + TestUtils.randomString(5) + ".csv";
      testStreamRoundTrip(
          schema + "." + stageName + "/" + fileName,
          schema + "." + stageName + "/" + fileName + ".gz",
          true);

    } finally {
      dropStage(stageName);
    }
  }

  @Test
  public void testStreamRoundTripNegative() {
    String stageName = TestUtils.randomStageName();
    try {
      createTempStage(stageName);

      // Test no file name
      assertThrows(
          SnowparkClientException.class, () -> testStreamRoundTrip(stageName, stageName, false));

      // Test no file name
      assertThrows(
          SnowparkClientException.class,
          () -> testStreamRoundTrip(stageName + "/", stageName + "/", false));

      String fileName = "streamFile_" + TestUtils.randomString(5) + ".csv";

      // Test upload no stage
      assertThrows(
          SnowflakeSQLException.class,
          () ->
              testStreamRoundTrip(
                  "nonExistStage/nonExistFile", "nonExistStage/nonExistFile", false));

      // Test download no stage
      String finalFileName = fileName;
      assertThrows(
          SnowflakeSQLException.class,
          () ->
              testStreamRoundTrip(
                  stageName + "/" + finalFileName, "nonExistStage/nonExistFile", false));

      // Test download no file
      fileName = "streamFile_" + TestUtils.randomString(5) + ".csv";
      String finalFileName2 = fileName;
      assertThrows(
          SnowparkClientException.class,
          () ->
              testStreamRoundTrip(
                  stageName + "/" + finalFileName2, stageName + "/nonExistFile", false));
    } finally {
      dropStage(stageName);
    }
  }

  private void testStreamRoundTrip(
      String uploadLocation, String downloadLocation, boolean compress) {
    String fileContent = "test, file, csv";
    getSession()
        .file()
        .uploadStream(uploadLocation, new ByteArrayInputStream(fileContent.getBytes()), compress);
    InputStream is = getSession().file().downloadStream(downloadLocation, compress);
    assert (new BufferedReader(new InputStreamReader(is))
        .lines()
        .collect(Collectors.joining("\n"))
        .equals(fileContent));
  }
}
