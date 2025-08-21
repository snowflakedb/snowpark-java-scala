package com.snowflake.snowpark_test

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.analyzer.quoteName
import com.snowflake.snowpark.types._
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.sql.{Date, Time}
import scala.util.Random

class DataFrameWriterSuite extends TestData {
  import session.implicits._
  val sourceStageName = randomStageName()
  val targetStageName = randomStageName()
  val tableName = randomTableName()

  private val userSchema: StructType = StructType(
    Seq(StructField("a", IntegerType), StructField("b", StringType), StructField("c", DoubleType)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    createStage(sourceStageName)
    createStage(targetStageName)
    uploadFileToStage(sourceStageName, testFileCsv, compress = false)
  }

  override def afterAll(): Unit = {
    dropStage(sourceStageName)
    dropStage(targetStageName)
    dropTable(tableName)
    super.afterAll()
  }

  test("Unit test for SaveMode") {
    assert(SaveMode.Overwrite.toString.equals("Overwrite"))
    assert(SaveMode.Append.toString.equals("Append"))
    assert(SaveMode.ErrorIfExists.toString.equals("ErrorIfExists"))
    assert(SaveMode.Ignore.toString.equals("Ignore"))
  }

  test("Negative: Write with target column name order") {
    createTable(tableName, "a int, b int")
    val df1 = Seq((1, 2)).toDF("a", "c")

    // The "columnOrder = name" needs the DataFrame has the same column name set
    val ex1 = intercept[Exception] {
      df1.write.option("columnOrder", "name").saveAsTable(tableName)
    }
    assert(ex1.asInstanceOf[SnowflakeSQLException].getMessage.contains("invalid identifier 'B'"))

    // Test unsupported option name
    val ex2 = intercept[SnowparkClientException] {
      df1.write.option("invalid_option", "invalid_value").saveAsTable(tableName)
    }
    assert(ex2.errorCode.equals("0126"))

    // Test unsupported option name
    val ex3 = intercept[SnowparkClientException] {
      df1.write.option("columnOrder", "invalid_value").saveAsTable(tableName)
    }
    assert(ex3.errorCode.equals("0127"))

    // "columnOrder = name" should be used for Append mode only
    val ex4 = intercept[SnowparkClientException] {
      df1.write.option("columnOrder", "name").mode(SaveMode.Overwrite).saveAsTable(tableName)
    }
    assert(ex4.errorCode.equals("0128"))
  }

  test("Catch COPY INTO LOCATION output schema change") {
    // This test case catches the future potential COPY INTO LOCATION output schema change
    // If it is failed, we need report with COPY team to provide a workaround for customer.
    // NOTE: The server side change may affect old SnowPark versions. This test case
    // will be run in jenkins test for preprod, so we can catch it before deployed to prod.
    val df1 = Seq((1, 2)).toDF("a", "c")
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    val copySchema = df1.write.csv(path).schema
    assert(copySchema.size == 3)
    val field0 = copySchema.fields(0)
    assert(field0.name.equals("\"rows_unloaded\"") && field0.dataType == LongType)
    val field1 = copySchema.fields(1)
    assert(field1.name.equals("\"input_bytes\"") && field1.dataType == LongType)
    val field2 = copySchema.fields(2)
    assert(field2.name.equals("\"output_bytes\"") && field2.dataType == LongType)
  }

  test("Write with target column name order") {
    createTable(tableName, "a int, b int")
    val df1 = Seq((1, 2)).toDF("b", "a")

    // By default, it is by index
    df1.write.saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(1, 2)))

    // Explicitly use "index"
    runQuery(s"truncate table $tableName", session)
    df1.write.option("columnOrder", "index").saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(1, 2)))

    // use order by "name"
    runQuery(s"truncate table $tableName", session)
    df1.write.option("columnOrder", "name").saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(2, 1)))

    // column name and table name with special characters
    val specialTableName = "\"test table name\""
    try {
      val df2 = Seq((1, 2)).toDF("b b", "a a")
      createTable(specialTableName, "\"a a\" int, \"b b\" int")
      df2.write.option("columnOrder", "name").saveAsTable(specialTableName)
      checkAnswer(session.table(specialTableName), Seq(Row(2, 1)))
    } finally {
      dropTable(specialTableName)
    }

    // If target table doesn't exists, "order by name" is not actually used.
    dropTable(tableName)
    df1.write.option("columnOrder", "name").saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(1, 2)))
  }

  test("Write with target column name order: all kinds of DataFrames") {
    createTable(tableName, "a int, b int")
    val df1 = Seq((1, 2)).toDF("b", "a")

    // DataFrame.cache()
    runQuery(s"truncate table $tableName", session)
    val dfCache = df1.cacheResult()
    dfCache.write.option("columnOrder", "name").saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(2, 1)))

    // DataFrame.clone()
    runQuery(s"truncate table $tableName", session)
    val dfClone = df1.cacheResult()
    dfClone.write.option("columnOrder", "name").saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(2, 1)))

    // Large local relation
    runQuery(s"truncate table $tableName", session)
    val largeDf = (1 to 1024)
      .map { x =>
        (x, x * 10)
      }
      .toDF("b", "a")
    largeDf.write.option("columnOrder", "name").saveAsTable(tableName)
    val rows = session.table(tableName).collect()
    assert(rows.length == 1024 && rows.count(r => r.getInt(0) / r.getInt(1) == 10) == 1024)

    // show tables
    runQuery(s"truncate table $tableName", session)
    val showTableColumns = session
      .sql("show tables")
      .schema
      .fields
      .map { f =>
        s"${quoteName(f.name)} ${convertToSFType(f.dataType)}"
      }
      .mkString(", ")
    // exchange column orders: "name"(1) <-> "kind"(4)
    val schemaString = showTableColumns
      .replaceAll("\"kind\"", "test_place_holder")
      .replaceAll("\"name\"", "\"kind\"")
      .replaceAll("test_place_holder", "\"name\"")
    createTable(tableName, schemaString)
    session.sql("show tables").write.option("columnOrder", "name").saveAsTable(tableName)
    // In "show tables" result, "name" is 2nd column.
    // In the target table, "name" is the 4th column.
    assert(session.table(tableName).collect().map(_.getString(4)).contains(tableName))

    // Read file, table columns are in reverse order
    createTable(tableName, "c double, b string, a int")
    val testFileOnStage = s"@$sourceStageName/$testFileCsv"
    val dfReadFile = session.read.schema(userSchema).csv(testFileOnStage)
    checkAnswer(dfReadFile, Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
    dfReadFile.write.option("columnOrder", "name").saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(1.2, "one", 1), Row(2.2, "two", 2)))
    // read with copy options
    val dfReadCopy = session.read.schema(userSchema).option("PURGE", false).csv(testFileOnStage)
    runQuery(s"truncate table $tableName", session)
    dfReadCopy.write.option("columnOrder", "name").saveAsTable(tableName)
    checkAnswer(session.table(tableName), Seq(Row(1.2, "one", 1), Row(2.2, "two", 2)))
  }

  private def runCSvTest(
      df: DataFrame,
      stageLocation: String,
      options: Map[String, Any],
      expectedWriteResult: Array[Row],
      outputFileExtension: String,
      saveMode: Option[SaveMode] = None) = {
    // Execute COPY INTO location and check result
    val writer = df.write.options(options)
    saveMode.foreach(writer.mode)
    val writeResult = writer.csv(stageLocation)
    checkResult(writeResult.rows, expectedWriteResult)

    // Check files are written to stage location
    val resultFiles = session.sql(s"ls $stageLocation").collect()
    assert(resultFiles.length == 1 && resultFiles(0).getString(0).endsWith(outputFileExtension))
  }

  private def runJsonTest(
      df: DataFrame,
      stageLocation: String,
      options: Map[String, Any],
      expectedWriteResult: Array[Row],
      outputFileExtension: String,
      saveMode: Option[SaveMode] = None) = {
    // Execute COPY INTO location and check result
    val writer = df.write.options(options)
    saveMode.foreach(writer.mode)
    val writeResult = writer.json(stageLocation)
    checkResult(writeResult.rows, expectedWriteResult)

    // Check files are written to stage location
    val resultFiles = session.sql(s"ls $stageLocation").collect()
    assert(resultFiles.length == 1 && resultFiles(0).getString(0).endsWith(outputFileExtension))
  }

  private def runParquetTest(
      df: DataFrame,
      stageLocation: String,
      options: Map[String, Any],
      expectedNumberOfRow: Int,
      outputFileExtension: String,
      saveMode: Option[SaveMode] = None) = {
    // Execute COPY INTO location and check result
    val writer = df.write.options(options)
    saveMode.foreach(writer.mode)
    val writeResult = writer.parquet(stageLocation)
    assert(writeResult.rows.head.getInt(0) == expectedNumberOfRow)

    // Check files are written to stage location
    val resultFiles = session.sql(s"ls $stageLocation").collect()
    assert(resultFiles.length == 1 && resultFiles(0).getString(0).endsWith(outputFileExtension))
  }

  test("write CSV files: save mode and file format options") {
    createTable(tableName, "c1 int, c2 double, c3 string")
    runQuery(s"insert into $tableName values (1,1.1,'one'),(2,2.2,'two'),(null,null,null)", session)
    val schema = StructType(
      Seq(
        StructField("c1", IntegerType),
        StructField("c2", DoubleType),
        StructField("c3", StringType)))
    val df = session.table(tableName)
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // without any options
    runCSvTest(df, path, Map.empty, Array(Row(3, 32, 46)), ".csv.gz")
    checkAnswer(
      session.read.schema(schema).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // by default, the mode is ErrorIfExist
    val ex = intercept[SnowflakeSQLException] {
      runCSvTest(df, path, Map.empty, Array(Row(3, 32, 46)), ".csv.gz")
    }
    assert(ex.getMessage.contains("Files already existing at the unload destination"))

    // Test overwrite mode
    runCSvTest(df, path, Map.empty, Array(Row(3, 32, 46)), ".csv.gz", Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.schema(schema).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // test some file format options and values
    session.sql(s"remove $path").collect()
    val options1 = Map(
      "FIELD_DELIMITER" -> "'aa'",
      "RECORD_DELIMITER" -> "bbbb",
      "COMPRESSION" -> "NONE",
      "FILE_EXTENSION" -> "mycsv")
    runCSvTest(df, path, options1, Array(Row(3, 47, 47)), ".mycsv")
    checkAnswer(
      session.read.schema(schema).options(options1).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // Test file format name only
    val fileFormatName = randomTableName()
    session
      .sql(
        s"CREATE OR REPLACE TEMPORARY FILE FORMAT $fileFormatName " +
          s"TYPE = CSV FIELD_DELIMITER = 'aa' RECORD_DELIMITER = 'bbbb' " +
          s"COMPRESSION = 'NONE' FILE_EXTENSION = 'mycsv'")
      .collect()
    runCSvTest(
      df,
      path,
      Map("FORMAT_NAME" -> fileFormatName),
      Array(Row(3, 47, 47)),
      ".mycsv",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.schema(schema).options(options1).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // Test file format name and some extra format options
    val fileFormatName2 = randomTableName()
    session
      .sql(
        s"CREATE OR REPLACE TEMPORARY FILE FORMAT $fileFormatName2 " +
          s"TYPE = CSV FIELD_DELIMITER = 'aa' RECORD_DELIMITER = 'bbbb'")
      .collect()
    val formatNameAndOptions =
      Map("FORMAT_NAME" -> fileFormatName2, "COMPRESSION" -> "NONE", "FILE_EXTENSION" -> "mycsv")
    runCSvTest(
      df,
      path,
      formatNameAndOptions,
      Array(Row(3, 47, 47)),
      ".mycsv",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.schema(schema).options(options1).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))
  }

  // copyOptions ::=
  //     OVERWRITE = TRUE | FALSE
  //     SINGLE = TRUE | FALSE
  //     MAX_FILE_SIZE = <num>
  //     INCLUDE_QUERY_ID = TRUE | FALSE
  //     DETAILED_OUTPUT = TRUE | FALSE
  test("write CSV files: copy options") {
    createTable(tableName, "c1 int, c2 double, c3 string")
    runQuery(s"insert into $tableName values (1,1.1,'one'),(2,2.2,'two'),(null,null,null)", session)
    val schema = StructType(
      Seq(
        StructField("c1", IntegerType),
        StructField("c2", DoubleType),
        StructField("c3", StringType)))
    val df = session.table(tableName)
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // Test SINGLE
    val targetFile = "my.data.csv.gz"
    val path2 = s"$path/$targetFile"
    runCSvTest(df, path2, Map("SINGLE" -> true), Array(Row(3, 32, 46)), targetFile)
    checkAnswer(
      session.read.schema(schema).csv(path2),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // other copy options
    session.sql(s"rm $path").collect()
    val otherOptions =
      Map("MAX_FILE_SIZE" -> 1, "INCLUDE_QUERY_ID" -> "TRUE", "DETAILED_OUTPUT" -> true)
    val copyResult = df.write.options(otherOptions).csv(path).rows
    val queryId = session.sql("select last_query_id()").collect()(0).getString(0)
    assert(copyResult.length == 1 && copyResult(0).getString(0).contains(queryId))
    val resultFiles = session.sql(s"ls $path").collect()
    assert(resultFiles.length == 1 && resultFiles(0).getString(0).contains(queryId))
    checkAnswer(
      session.read.schema(schema).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))
  }

  // sub clause:
  //  [ PARTITION BY <expr> ]
  //  [ HEADER ]
  test("write CSV files: sub clause", JavaStoredProcExclude) {
    testWithTimezone() {
      runQuery(
        s"""create or replace table $tableName (
           |  dt date,
           |  ts time
           |  )
           |as
           |  select to_date($$1)
           |        ,to_time($$2)
           |    from values
           |           ('2020-01-26', '18:05')
           |          ,('2020-01-27', '22:57')
           |          ,('2020-01-28', null)
           |          ,('2020-01-29', '02:15')
           |""".stripMargin,
        session)
      val schema = StructType(Seq(StructField("dt", DateType), StructField("tm", TimeType)))
      val df = session.table(tableName)
      val path = s"@$targetStageName/p_${Random.nextInt().abs}"

      val options =
        Map(
          "header" -> true,
          "partition by" -> ("('date=' || to_varchar(dt, 'YYYY-MM-DD') ||" +
            " '/hour=' || to_varchar(date_part(hour, ts)))"))

      val copyResult = df.write.options(options).csv(path).rows
      checkResult(copyResult, Array(Row(4, 99, 179)))
      val resultFiles = session.sql(s"ls $path").collect()
      assert(resultFiles.length == 4)
      checkAnswer(
        session.read.schema(schema).option("skip_header", 1).csv(path),
        Seq(
          Row(Date.valueOf("2020-01-26"), Time.valueOf("18:05:00")),
          Row(Date.valueOf("2020-01-27"), Time.valueOf("22:57:00")),
          Row(Date.valueOf("2020-01-28"), null),
          Row(Date.valueOf("2020-01-29"), Time.valueOf("02:15:00"))))
    }
  }

  test("Write as CSV with all kinds of DataFrames") {
    val schema = StructType(Seq(StructField("c1", IntegerType), StructField("c2", IntegerType)))
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"
    val df1 = Seq((1, 2)).toDF("b", "a")

    // DataFrame.cache()
    runQuery(s"rm $path", session)
    df1.cacheResult().write.csv(path)
    checkAnswer(session.read.schema(schema).csv(path), Seq(Row(1, 2)))

    // DataFrame.clone()
    runQuery(s"rm $path", session)
    df1.clone().write.csv(path)
    checkAnswer(session.read.schema(schema).csv(path), Seq(Row(1, 2)))

    // Large local relation
    runQuery(s"rm $path", session)
    val largeDf = (1 to 1024)
      .map { x =>
        (x, x * 10)
      }
      .toDF("a", "b")
    largeDf.write.csv(path)
    val rows = session.read.schema(schema).csv(path).collect()
    assert(rows.length == 1024 && rows.count(r => r.getInt(1) / r.getInt(0) == 10) == 1024)

    // show tables
    runQuery(s"rm $path", session)
    createTable(tableName, "c1 int, c2 double, c3 string")
    session.sql("show tables").write.option("FIELD_OPTIONALLY_ENCLOSED_BY", "\"").csv(path)
    val schema2 = StructType((0 to 12).map { i =>
      StructField(s"C$i", StringType)
    })
    val tableNames = session.read.schema(schema2).csv(path).collect().map(_.getString(1))
    assert(tableNames.contains(s""""$tableName""""))

    // Read from file and write to another file
    runQuery(s"rm $path", session)
    val testFileOnStage = s"@$sourceStageName/$testFileCsv"
    val dfReadFile = session.read.schema(userSchema).csv(testFileOnStage)
    checkAnswer(dfReadFile, Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
    dfReadFile.write.csv(path)
    checkAnswer(
      session.read.schema(userSchema).csv(path),
      Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
    // read with copy options
    runQuery(s"rm $path", session)
    val dfReadCopy = session.read.schema(userSchema).option("PURGE", false).csv(testFileOnStage)
    dfReadCopy.write.csv(path)
    checkAnswer(
      session.read.schema(userSchema).csv(path),
      Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
  }

  test("negative test") {
    createTable(tableName, "c1 int, c2 double, c3 string")
    runQuery(s"insert into $tableName values (1,1.1,'one'),(2,2.2,'two'),(null,null,null)", session)
    val df = session.table(tableName)
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // OVERWRITE should be set by mode to avoid conflict
    val ex = intercept[SnowparkClientException] {
      df.write.option("OVERWRITE", true).csv(path)
    }
    assert(
      ex.getMessage.contains(
        "DataFrameWriter doesn't support option 'OVERWRITE' when writing to a file."))

    val ex2 = intercept[SnowparkClientException] {
      df.write.option("TYPE", "CSV").csv(path)
    }
    assert(
      ex2.getMessage.contains(
        "DataFrameWriter doesn't support option 'TYPE' when writing to a file."))

    val ex3 = intercept[SnowparkClientException] {
      df.write.option("unknown", "abc").csv(path)
    }
    assert(
      ex3.getMessage.contains(
        "DataFrameWriter doesn't support option 'UNKNOWN' when writing to a file."))

    // only support ErrorIfExists and Overwrite mode
    val ex4 = intercept[SnowparkClientException] {
      df.write.mode("append").csv(path)
    }
    assert(
      ex4.getMessage.contains(
        "DataFrameWriter doesn't support mode 'Append' when writing to a file."))
  }

  // JSON can only be used to unload data from columns of type VARIANT
  // (i.e. columns containing JSON data).
  test("write JSON files: file format options") {
    createTable(tableName, "c1 int, c2 string")
    runQuery(s"insert into $tableName values (1,'one'),(2,'two')", session)
    val df = session.table(tableName).select(array_construct(col("c1"), col("c2")))
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // write with default
    runJsonTest(df, path, Map.empty, Array(Row(2, 20, 40)), ".json.gz")
    checkAnswer(
      session.read.json(path),
      Seq(Row("[\n  1,\n  \"one\"\n]"), Row("[\n  2,\n  \"two\"\n]")))

    // write one column and overwrite
    val df2 = session.table(tableName).select(to_variant(col("c2")))
    runJsonTest(df2, path, Map.empty, Array(Row(2, 12, 32)), ".json.gz", Some(SaveMode.Overwrite))
    checkAnswer(session.read.json(path), Seq(Row("\"one\""), Row("\"two\"")))

    // write with format_name
    val formatName = randomTableName()
    session.sql(s"CREATE or REPLACE TEMPORARY  FILE  FORMAT $formatName TYPE  = JSON").collect()
    val df3 = session.table(tableName).select(to_variant(col("c1")))
    runJsonTest(
      df3,
      path,
      Map("FORMAT_NAME" -> formatName),
      Array(Row(2, 4, 24)),
      ".json.gz",
      Some(SaveMode.Overwrite))
    session.read.json(path).show()
    checkAnswer(session.read.json(path), Seq(Row("1"), Row("2")))

    // write with format_name format and some extra option
    session.sql(s"rm $path").collect()
    val df4 = session.table(tableName).select(to_variant(col("c1")))
    runJsonTest(
      df4,
      path,
      Map("FORMAT_NAME" -> formatName, "FILE_EXTENSION" -> "myjson.json", "COMPRESSION" -> "NONE"),
      Array(Row(2, 4, 4)),
      ".myjson.json",
      Some(SaveMode.Overwrite))
    session.read.json(path).show()
    checkAnswer(session.read.json(path), Seq(Row("1"), Row("2")))
  }

  test("write PARQUET files: file format options") {
    createTable(tableName, "c1 int, c2 string")
    runQuery(s"insert into $tableName values (1,'one'),(2,'two')", session)
    val df = session.table(tableName)
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // write by default
    runParquetTest(df, path, Map.empty, 2, ".parquet")
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))

    // write with overwrite
    runParquetTest(df, path, Map.empty, 2, ".snappy.parquet", Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))

    // write with format_name
    val formatName = randomTableName()
    session
      .sql(s"CREATE or REPLACE TEMPORARY  FILE  FORMAT $formatName TYPE  = PARQUET")
      .collect()
    runParquetTest(
      df,
      path,
      Map("FORMAT_NAME" -> formatName),
      2,
      ".snappy.parquet",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))

    // write with format_name format and some extra option
    session.sql(s"rm $path").collect()
    val df4 = session.table(tableName).select(to_variant(col("c1")))
    runParquetTest(
      df,
      path,
      Map("FORMAT_NAME" -> formatName, "COMPRESSION" -> "LZO"),
      2,
      ".lzo.parquet",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))
  }
}
