package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.scalatest.Tag

import scala.util.Random

class DataFrameReaderSuite extends SNTestBase {
  val tmpStageName: String = randomStageName()
  val tmpStageName2: String = randomStageName()
  private val userSchema: StructType = StructType(
    Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", DoubleType)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    // create temporary stage to store the file
    runQuery(s"CREATE TEMPORARY STAGE $tmpStageName", session)
    runQuery(s"CREATE TEMPORARY STAGE $tmpStageName2", session)
    // upload the file to stage
    uploadFileToStage(tmpStageName, testFileCsv, compress = false)

    uploadFileToStage(tmpStageName, testFile2Csv, compress = false)

    uploadFileToStage(tmpStageName, testFileCsvColon, compress = false)

    uploadFileToStage(tmpStageName, testFileCsvQuotes, compress = false)

    uploadFileToStage(tmpStageName2, testFileCsv, compress = false)

    uploadFileToStage(tmpStageName, testFileJson, compress = false)

    uploadFileToStage(tmpStageName, testFileAvro, compress = false)

    uploadFileToStage(tmpStageName, testFileParquet, compress = false)

    uploadFileToStage(tmpStageName, testFileOrc, compress = false)

    uploadFileToStage(tmpStageName, testFileXml, compress = false)

    uploadFileToStage(tmpStageName, testBrokenCsv, compress = false)
  }

  override def afterAll(): Unit = {
    // drop the temporary stages
    runQuery(s"DROP STAGE IF EXISTS $tmpStageName", session)
    runQuery(s"DROP STAGE IF EXISTS $tmpStageName2", session)

    super.afterAll()
  }

  testReadFile("read csv test")(reader => {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val df1 = reader().schema(userSchema).csv(testFileOnStage).collect()

    assert(df1.length == 2)
    assert(df1(0).length == 3)
    assert(df1 sameElements Array[Row](Row(1, "one", 1.2), Row(2, "two", 2.2)))

    assertThrows[SnowparkClientException](session.read.csv(testFileOnStage))

    // if user give a incorrect schema that there are type error
    // the system will throw SnowflakeSQLException during execution
    val incorrectSchema: StructType = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))
    val df2 = reader().schema(incorrectSchema).csv(testFileOnStage)
    assertThrows[SnowflakeSQLException](df2.collect())
  })

  test("read csv, incorrect schema - SELECT") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // if user give a incorrect schema where there are no type error
    // the system casts the type to user schema.
    // If there are more columns than there actually are
    // in the file, the result adds null to these extra columns
    val incorrectSchema2: StructType = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", IntegerType),
        StructField("d", IntegerType)))
    val df = session.read.schema(incorrectSchema2).csv(testFileOnStage)
    assert(df.collect() sameElements Array[Row](Row(1, "one", 1, null), Row(2, "two", 2, null)))
  }

  test("read csv, incorrect schema - COPY") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val incorrectSchema2: StructType = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", IntegerType),
        StructField("d", IntegerType)))
    val df = session.read.option("purge", false).schema(incorrectSchema2).csv(testFileOnStage)
    // throw exception from COPY
    assertThrows[SnowflakeSQLException](df.collect())
  }

  // self join doesn't work with COPY todo: fix SNOW-208728
  test("read csv with more operations") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"

    val df1 = session.read.schema(userSchema).csv(testFileOnStage).filter(col("a") < 2)
    checkAnswer(df1, Seq(Row(1, "one", 1.2)))

    // test for self union
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    val df2 = df.union(df)
    assert(df2.collect() sameElements Array[Row](Row(1, "one", 1.2), Row(2, "two", 2.2)))
    val df22 = df.unionAll(df)
    assert(
      df22.collect() sameElements Array[Row](
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2)))

    // test for union between two stages
    val testFileOnStage2 = s"@$tmpStageName2/$testFileCsv"
    val df3 = session.read.schema(userSchema).csv(testFileOnStage2)
    val df4 = df.union(df3)
    assert(df4.collect() sameElements Array[Row](Row(1, "one", 1.2), Row(2, "two", 2.2)))
    val df44 = df.unionAll(df3)
    assert(
      df44.collect() sameElements Array[Row](
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2)))
  }

  testReadFile("read csv with formatTypeOptions")(reader => {
    val testFileColon = s"@$tmpStageName/$testFileCsvColon"
    val options = Map("field_delimiter" -> "';'", "skip_blank_lines" -> true, "skip_header" -> 1)
    val df1 = reader().schema(userSchema).options(options).csv(testFileColon)
    assert(df1.collect() sameElements Array[Row](Row(1, "one", 1.2), Row(2, "two", 2.2)))

    // test when user does not input a right option
    val df2 = reader().schema(userSchema).csv(testFileColon)
    assertThrows[SnowflakeSQLException](df2.collect())

    // test for multiple formatTypeOptions
    val df3 = reader()
      .schema(userSchema)
      .option("field_delimiter", ";")
      .option("ENCODING", "wrongEncoding")
      .option("ENCODING", "UTF8")
      .option("COMPRESSION", "NONE")
      .option("skip_header", 1)
      .csv(testFileColon)
    assert(df3.collect() sameElements Array[Row](Row(1, "one", 1.2), Row(2, "two", 2.2)))

    // test for union between files with different schema and different stage
    val testFileOnStage2 = s"@$tmpStageName2/$testFileCsv"
    val df4 = reader().schema(userSchema).csv(testFileOnStage2)
    val df5 = df1.unionAll(df4)
    assert(
      df5.collect() sameElements Array[Row](
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2)))

    val df6 = df1.union(df4)
    assert(df6.collect() sameElements Array[Row](Row(1, "one", 1.2), Row(2, "two", 2.2)))
  })

  // Compression is not supported in file uploads from stored procs
  testReadFile("Test to read files from stage", JavaStoredProcExclude)(reader => {
    val dataFilesStage = randomStageName()
    runQuery(s"CREATE TEMPORARY STAGE $dataFilesStage", session)
    uploadFileToStage(dataFilesStage, testFileCsv, true)
    uploadFileToStage(dataFilesStage, testFile2Csv, true)

    try {
      val df = session.read
        .schema(userSchema)
        .option("compression", "auto")
        .csv(s"@$dataFilesStage/")
      checkAnswer(
        df,
        Seq(Row(1, "one", 1.2), Row(2, "two", 2.2), Row(3, "three", 3.3), Row(4, "four", 4.4)))
    } finally {
      runQuery(s"DROP STAGE IF EXISTS $dataFilesStage", session)
    }
  })

  testReadFile("Test for all csv compression keywords")(reader => {
    val result = Seq(Row(1, "one", 1.2), Row(2, "two", 2.2))
    val tmpTable = getFullyQualifiedTempSchema + "." + randomName()
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    reader()
      .schema(userSchema)
      .option("compression", "auto")
      .csv(testFileOnStage)
      .write
      .saveAsTable(tmpTable)

    val formatName = randomName()
    try {
      runQuery(s"create file format $formatName type = 'csv'", session)

      Seq("gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate").foreach(ctype => {
        val path = s"@$tmpStageName/$ctype/${Random.nextInt.abs}/"
        // Upload data
        session
          .sql(
            s"copy into $path from" +
              s" ( select * from $tmpTable) file_format=(format_name='$formatName'" +
              s" compression='$ctype')")
          .collect()

        // Read the data
        checkAnswer(
          reader()
            .option("COMPRESSION", ctype)
            .schema(userSchema)
            .csv(path),
          result)
      })
    } finally {
      runQuery(s"drop file format $formatName", session)
    }
  })

  testReadFile("read csv with special chars in formatTypeOptions")(reader => {
    val schema1 = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", DoubleType),
        StructField("d", IntegerType)))
    val testFile = s"@$tmpStageName/$testFileCsvQuotes"
    val df1 = reader()
      .schema(schema1)
      .option("field_optionally_enclosed_by", "\"")
      .csv(testFile)

    checkAnswer(df1, Seq(Row(1, "one", 1.2, 1), Row(2, "two", 2.2, 2)))

    // without the setting it should fail schema validation
    val df2 = reader().schema(schema1).csv(testFile)
    val ex = intercept[SnowflakeSQLException] {
      df2.collect()
    }
    assert(ex.getMessage.contains("Numeric value '\"1\"' is not recognized"))

    val schema2 = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", DoubleType),
        StructField("d", StringType)))
    val df3 = reader().schema(schema2).csv(testFile)
    val res = df3.collect()
    checkAnswer(df3.select("d"), Seq(Row("\"1\""), Row("\"2\"")))
  })

  testReadFile("read json with no schema")(reader => {
    val jsonPath: String = s"@$tmpStageName/$testFileJson"
    val df1 = reader().json(jsonPath)

    checkAnswer(
      df1,
      Seq(Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}")))

    // query test
    checkAnswer(
      df1.where(sqlExpr("$1:color") === "Red"),
      Seq(Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}")))

    // assert user cannot input a schema to read json
    assertThrows[IllegalArgumentException](reader().schema(userSchema).json(jsonPath))

    // user can input customize formatTypeOptions
    val df2 = reader().option("FILE_EXTENSION", "json").json(jsonPath)
    checkAnswer(
      df2,
      Seq(Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}")))
  })

  testReadFile("read avro with no schema")(reader => {
    val avroPath: String = s"@$tmpStageName/$testFileAvro"
    val df1 = reader().avro(avroPath)
    checkAnswer(
      df1,
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)

    // query test
    checkAnswer(
      df1.where(sqlExpr("$1:num") > 1),
      Seq(Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")))

    // assert user cannot input a schema to read avro
    assertThrows[IllegalArgumentException](session.read.schema(userSchema).avro(avroPath))

    // user can input customize formatTypeOptions
    val df2 = reader().option("COMPRESSION", "NONE").avro(avroPath)
    checkAnswer(
      df2,
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)
  })

  testReadFile("Test for all parquet compression types")(reader => {
    val tmpTable = getFullyQualifiedTempSchema + "." + randomName()
    val testFileOnStage = s"@$tmpStageName/$testFileParquet"
    reader().parquet(testFileOnStage).toDF("a").write.saveAsTable(tmpTable)

    val formatName = randomName()
    session.sql(s"create file format $formatName type = 'parquet'").collect

    Seq("snappy", "lzo").foreach(ctype => {
      // Upload data
      session
        .sql(
          s"copy into @$tmpStageName/$ctype/ from" +
            s" ( select * from $tmpTable) file_format=(format_name='$formatName'" +
            s" compression='$ctype') overwrite = true")
        .collect()

      // Read the data
      reader()
        .option("COMPRESSION", ctype)
        .parquet(s"@$tmpStageName/$ctype/")
        .collect()
    })
  })

  testReadFile("read parquet with no schema")(reader => {
    val path: String = s"@$tmpStageName/$testFileParquet"
    val df1 = reader().parquet(path)
    checkAnswer(
      df1,
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)

    // query test
    checkAnswer(
      df1.where(sqlExpr("$1:num") > 1),
      Seq(Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)

    // assert user cannot input a schema to read parquet
    assertThrows[IllegalArgumentException](session.read.schema(userSchema).parquet(path))

    // user can input customize formatTypeOptions
    val df2 = reader().option("COMPRESSION", "NONE").parquet(path)
    checkAnswer(
      df2,
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)
  })

  testReadFile("read orc with no schema")(reader => {
    val path: String = s"@$tmpStageName/$testFileOrc"
    val df1 = reader().orc(path)
    checkAnswer(
      df1,
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)

    // query test
    checkAnswer(
      df1.where(sqlExpr("$1:num") > 1),
      Seq(Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)

    // assert user cannot input a schema to read avro
    assertThrows[IllegalArgumentException](session.read.schema(userSchema).orc(path))

    // user can input customize formatTypeOptions
    val df2 = reader().option("TRIM_SPACE", false).orc(path)
    checkAnswer(
      df2,
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")),
      sort = false)
  })

  testReadFile("read xml with no schema")(reader => {
    val path: String = s"@$tmpStageName/$testFileXml"
    val df1 = reader().xml(path)
    checkAnswer(
      df1,
      Seq(
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")),
      sort = false)

    // query test
    checkAnswer(
      df1
        .where(sqlExpr("xmlget($1, 'num', 0):\"$\"") > 1),
      Seq(Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")),
      sort = false)

    // assert user cannot input a schema to read avro
    assertThrows[IllegalArgumentException](session.read.schema(userSchema).xml(path))

    // user can input customize formatTypeOptions
    val df2 = reader().option("COMPRESSION", "NONE").xml(path)
    checkAnswer(
      df2,
      Seq(
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")),
      sort = false)
  })

  test("read file on_error = continue on CSV") {
    val brokenFile = s"@$tmpStageName/$testBrokenCsv"

    // skip (2, two, wrong)
    checkAnswer(
      session.read
        .schema(userSchema)
        .option("on_error", "continue")
        .option("COMPRESSION", "none")
        .csv(brokenFile),
      Seq(Row(1, "one", 1.1), Row(3, "three", 3.3)))

  }

  test("read file on_error = continue on AVRO") {
    val brokenFile = s"@$tmpStageName/$testBrokenCsv"

    // skip all
    checkAnswer(
      session.read
        .option("on_error", "continue")
        .option("COMPRESSION", "none")
        .avro(brokenFile),
      Seq.empty)
  }

  test("SELECT and COPY on non CSV format have same result schema") {
    val path: String = s"@$tmpStageName/$testFileParquet"
    val copy = session.read.option("purge", false).option("COMPRESSION", "none").parquet(path)
    val select = session.read.option("COMPRESSION", "none").parquet(path)

    assert(getSchemaString(copy.schema) == getSchemaString(select.schema))
  }

  test("Read staged files doesn't commit open transaction") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"

    // Test read from staged file with TEMP FILE FORMAT
    session.sql("begin").collect()
    session.read.schema(userSchema).csv(testFileOnStage).collect()
    assert(isActiveTransaction(session))
    session.sql("commit").collect()
    assert(!isActiveTransaction(session))

    // Test read from staged file with TEMP TABLE
    session.sql("begin").collect()
    session.read.option("purge", false).schema(userSchema).csv(testFileOnStage).collect()
    assert(isActiveTransaction(session))
    session.sql("commit").collect()
    assert(!isActiveTransaction(session))
  }

  testReadFile("pattern")(reader => {
    assert(
      reader()
        .schema(userSchema)
        .option("COMPRESSION", "none")
        .option("pattern", ".*CSV[.]csv")
        .csv(s"@$tmpStageName")
        .count() == 4)
  })

  testReadFile("table function on csv dataframe reader test")(reader => {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val df1 = reader().schema(userSchema).csv(testFileOnStage)

    checkAnswer(
      session
        .tableFunction(TableFunction("split_to_table"), Seq(df1("b"), lit(" ")))
        .select("VALUE"),
      Seq(Row("one"), Row("two")))
  })

  def testReadFile(testName: String, testTags: Tag*)(
      thunk: (() => DataFrameReader) => Unit): Unit = {
    // test select
    test(testName + " - SELECT", testTags: _*) {
      thunk(() => session.read)
    }

    // test copy
    test(testName + " - COPY", testTags: _*) {
      thunk(() => session.read.option("PURGE", false))
    }
  }
}
