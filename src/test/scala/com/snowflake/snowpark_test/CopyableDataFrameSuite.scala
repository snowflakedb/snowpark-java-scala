package com.snowflake.snowpark_test

import java.util.Locale

import com.snowflake.snowpark._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.functions._
import net.snowflake.client.jdbc.SnowflakeSQLException

class CopyableDataFrameSuite extends SNTestBase {
  val tmpStageName: String = randomStageName()
  val tmpStageName2: String = randomStageName()
  val testTableName: String = randomName()

  private val userSchema: StructType = StructType(
    Seq(StructField("a", IntegerType), StructField("b", StringType), StructField("c", DoubleType))
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    // create temporary stage to store the file
    TestUtils.createStage(tmpStageName, true, session)
    TestUtils.createStage(tmpStageName2, true, session)

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
    dropStage(tmpStageName)(session)
    dropStage(tmpStageName2)(session)
    dropTable(testTableName)(session)

    super.afterAll()
  }

  test("copy csv test: basic") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "a Int, b String, c Double")
    assert(session.table(testTableName).count() == 0)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    df.copyInto(testTableName)
    checkAnswer(session.table(testTableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))

    // run COPY again, the loaded files will be skipped by default
    df.copyInto(testTableName)
    checkAnswer(session.table(testTableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))

    // Copy again with FORCE = TRUE, loaded file are NOT skipped.
    session.read
      .schema(userSchema)
      .option("FORCE", true)
      .csv(testFileOnStage)
      .copyInto(testTableName)
    checkAnswer(
      session.table(testTableName),
      Seq(Row(1, "one", 1.2), Row(2, "two", 2.2), Row(1, "one", 1.2), Row(2, "two", 2.2))
    )
  }

  test("copy csv test: create target table automatically if not exists") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // make sure target table doesn't exist
    dropTable(testTableName)(session)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    df.copyInto(testTableName)
    checkAnswer(session.table(testTableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
    // target table is created from schema
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--A: Long (nullable = true)
           | |--B: String (nullable = true)
           | |--C: Double (nullable = true)
           |""".stripMargin
    )

    // run COPY again, the loaded files will be skipped by default
    df.copyInto(testTableName)
    checkAnswer(session.table(testTableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
  }

  test("copy csv test: saveAsTable() doesn't affect copyInto()") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "c1 Int, c2 String, c3 Double")
    assert(session.table(testTableName).count() == 0)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    df.copyInto(testTableName)
    checkAnswer(session.table(testTableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
    // Target table schema is the existing table schema
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
        | |--C1: Long (nullable = true)
        | |--C2: String (nullable = true)
        | |--C3: Double (nullable = true)
        |""".stripMargin
    )

    // run COPY again, the loaded files will be skipped by default
    df.copyInto(testTableName)
    checkAnswer(session.table(testTableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))

    // Write data with saveAsTable(), loaded file are NOT skipped.
    df.write.saveAsTable(testTableName)
    checkAnswer(
      session.table(testTableName),
      Seq(Row(1, "one", 1.2), Row(2, "two", 2.2), Row(1, "one", 1.2), Row(2, "two", 2.2))
    )

    // Write data with saveAsTable() again, loaded file are NOT skipped.
    df.write.saveAsTable(testTableName)
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2)
      )
    )
  }

  test("copy csv test: copy transformation") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "c1 String, c2 String, c3 String")
    assert(session.table(testTableName).count() == 0)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    // copy data with $1, $2, $3
    df.copyInto(testTableName, Seq(col("$1"), col("$2"), col("$3")))
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: String (nullable = true)
           | |--C2: String (nullable = true)
           | |--C3: String (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row("1", "one", "1.2"), Row("2", "two", "2.2")))

    // Copy data in order of $3, $2, $1 with FORCE = TRUE
    df.copyInto(testTableName, Seq(col("$3"), col("$2"), col("$1")), Map("FORCE" -> "TRUE"))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("1", "one", "1.2"),
        Row("2", "two", "2.2"),
        Row("1.2", "one", "1"),
        Row("2.2", "two", "2")
      )
    )

    // Copy data in order of $2, $3, $1 with FORCE = TRUE and skip_header = 1
    df.copyInto(
      testTableName,
      Seq(col("$2"), col("$3"), col("$1")),
      Map("FORCE" -> "TRUE", "skip_header" -> 1)
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("1", "one", "1.2"),
        Row("2", "two", "2.2"),
        Row("1.2", "one", "1"),
        Row("2.2", "two", "2"),
        Row("two", "2.2", "2")
      )
    )
  }

  test("copy csv test: negative test") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val df = session.read.schema(userSchema).csv(testFileOnStage)

    // case 1: copyInto with transformation, but table doesn't exist
    dropTable(testTableName)(session)
    val ex1 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1_alias")))
    }
    assert(
      ex1.errorCode.equals("0122") && ex1.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 2: copyInto transformation doesn't match table schema.
    createTable(testTableName, "c1 String")
    // table has one column, transformation has 2 columns.
    val ex2 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1"), col("$2").as("c2")))
    }
    assert(ex2.getMessage.contains("Insert value list does not match column list"))

    // case 3: copyInto transformation doesn't match table schema.
    createTable(testTableName, "c1 String, c2 String")
    // table has two column, transformation has one columns.
    val ex3 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1")))
    }
    assert(ex3.getMessage.contains("Insert value list does not match column list"))
  }

  test("copy csv test: copy transformation with column names") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "c1 String, c2 String, c3 String")
    assert(session.table(testTableName).count() == 0)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    // copy data to column c1 and c2 with $1, $2
    df.copyInto(testTableName, Seq("c1", "c2"), Seq(col("$1"), col("$2")), Map.empty)
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: String (nullable = true)
           | |--C2: String (nullable = true)
           | |--C3: String (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row("1", "one", null), Row("2", "two", null)))

    // Copy data in order of $3, $2 to column c3 and c2 with FORCE = TRUE
    df.copyInto(testTableName, Seq("c3", "c2"), Seq(col("$3"), col("$2")), Map("FORCE" -> "TRUE"))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("1", "one", null),
        Row("2", "two", null),
        Row(null, "one", "1.2"),
        Row(null, "two", "2.2")
      )
    )

    // Copy data $1 to column c3 with FORCE = TRUE and skip_header = 1
    df.copyInto(
      testTableName,
      Seq("c3"),
      Seq(col("$1")),
      Map("FORCE" -> "TRUE", "skip_header" -> 1)
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("1", "one", null),
        Row("2", "two", null),
        Row(null, "one", "1.2"),
        Row(null, "two", "2.2"),
        Row(null, null, "2")
      )
    )
  }

  test("copy csv test: copy into column names without transformation") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "c1 String, c2 String, c3 String, c4 String")
    assert(session.table(testTableName).count() == 0)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    // copy data to column c1 and c2
    df.copyInto(testTableName, Seq("c1", "c2", "c3"), Seq.empty, Map.empty)
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: String (nullable = true)
           | |--C2: String (nullable = true)
           | |--C3: String (nullable = true)
           | |--C4: String (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(Row("1", "one", "1.2", null), Row("2", "two", "2.2", null))
    )

    // case 2: select more columns from csv than it have
    // There is only 3 columns in the schema of the csv file.
    df.copyInto(testTableName, Seq("c1", "c2", "c3", "c4"), Seq.empty, Map("FORCE" -> "TRUE"))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("1", "one", "1.2", null),
        Row("2", "two", "2.2", null),
        Row("1", "one", "1.2", null),
        Row("2", "two", "2.2", null)
      )
    )
  }

  test("copy json test: write with column names") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileJson"
    val df = session.read.json(testFileOnStage)

    // create target table
    createTable(testTableName, "c1 String, c2 Variant, c3 String")
    df.copyInto(
      testTableName,
      Seq("c1", "c2"),
      Seq(sqlExpr("$1:color"), sqlExpr("$1:fruit")),
      Map.empty
    )
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: String (nullable = true)
           | |--C2: Variant (nullable = true)
           | |--C3: String (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row("Red", "\"Apple\"", null)))
  }

  test("copy json test: negative test with column names") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileJson"
    val df = session.read.json(testFileOnStage)

    // Case 1: select more than one column from json file without transformation
    createTable(testTableName, "c1 String, c2 Variant, c3 String")
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: String (nullable = true)
           | |--C2: Variant (nullable = true)
           | |--C3: String (nullable = true)
           |""".stripMargin
    )
    val ex = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq("c1", "c2"), Seq.empty, Map.empty)
    }
    assert(
      ex.getMessage.contains("JSON file format can produce one and only one column of type variant")
    )
  }

  test("copy csv test: negative test with column names") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val df = session.read.schema(userSchema).csv(testFileOnStage)

    // case 1: the number of column names does not match the number of transformations
    createTable(testTableName, "c1 String, c2 String, c3 String")
    // table has 3 column, transformation has 2 columns, column name has 1
    val ex2 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq("c1"), Seq(col("$1"), col("$2")), Map.empty)
    }
    assert(
      ex2.getMessage.contains(
        "Number of column names provided to copy " +
          "into does not match the number of transformations"
      )
    )
    // table has 3 column, transformation has 2 columns, column name has 3
    val ex3 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq("c1", "c2", "c3"), Seq(col("$1"), col("$2")), Map.empty)
    }
    assert(
      ex3.getMessage.contains(
        "Number of column names provided to copy " +
          "into does not match the number of transformations"
      )
    )

    // case 2: column names contains unknown columns
    // table has 3 column, transformation has 4 columns, column name has 4
    val ex4 = intercept[SnowflakeSQLException] {
      df.copyInto(
        testTableName,
        Seq("c1", "c2", "c3", "c4"),
        Seq(col("$1"), col("$2"), col("$3"), col("$4")),
        Map.empty
      )
    }
    assert(ex4.getMessage.contains("invalid identifier 'C4'"))
  }

  test("test transformation `as` clause does not have effect") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "c1 String, c2 String")
    assert(session.table(testTableName).count() == 0)
    val df2 = session.read.schema(userSchema).csv(testFileOnStage)

    df2.copyInto(testTableName, Seq(col("$1").as("aaa"), col("$2").as("bbb")))
    checkAnswer(session.table(testTableName), Seq(Row("1", "one"), Row("2", "two")))
  }

  test("copy json test: basic") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileJson"
    val df = session.read.json(testFileOnStage)

    // create target table
    createTable(testTableName, "c1 Variant")
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: Variant (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}"))
    )

    // copy again: loaded file is skipped.
    df.copyInto(testTableName, Seq(col("$1").as("B")))
    checkAnswer(
      session.table(testTableName),
      Seq(Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}"))
    )

    // copy again with FORCE = true.
    df.copyInto(testTableName, Seq(col("$1").as("B")), Map("FORCE" -> true))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}"),
        Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}")
      )
    )
  }

  test("copy json test: write with transformation") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileJson"
    val df = session.read.json(testFileOnStage)

    // create target table
    createTable(testTableName, "c1 String, c2 Variant, c3 String")
    df.copyInto(
      testTableName,
      Seq(
        sqlExpr("$1:color").as("color"),
        sqlExpr("$1:fruit").as("fruit"),
        sqlExpr("$1:size").as("size")
      )
    )
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: String (nullable = true)
           | |--C2: Variant (nullable = true)
           | |--C3: String (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row("Red", "\"Apple\"", "Large")))

    // copy again with existed table and FORCE = true.
    // use different order: size, fruit, color
    df.copyInto(
      testTableName,
      Seq(
        sqlExpr("$1:size").as("size"),
        sqlExpr("$1:fruit").as("fruit"),
        sqlExpr("$1:color").as("color")
      ),
      Map("FORCE" -> true)
    )
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--C1: String (nullable = true)
           | |--C2: Variant (nullable = true)
           | |--C3: String (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(Row("Red", "\"Apple\"", "Large"), Row("Large", "\"Apple\"", "Red"))
    )
  }

  test("copy json test: negative test") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileJson"
    val df = session.read.json(testFileOnStage)

    // case 1: copy without transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex1 = intercept[SnowparkClientException] {
      df.copyInto(testTableName)
    }
    assert(
      ex1.errorCode.equals("0122") && ex1.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 2: copy with transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex2 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq(col("$1").as("A")))
    }
    assert(
      ex2.errorCode.equals("0122") && ex2.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 3: COPY transformation doesn't match target table
    createTable(testTableName, "c1 String")
    // table has one column, transformation has 2 columns.
    val ex3 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1"), col("$2").as("c2")))
    }
    assert(ex3.getMessage.contains("Insert value list does not match column list"))
  }

  test("copy parquet test: basic") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileParquet"
    val df = session.read.parquet(testFileOnStage)

    createTable(testTableName, "A Variant")
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--A: Variant (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )

    // copy again, skip loaded files
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )

    // copy again with FORCE = true.
    df.copyInto(testTableName, Seq(col("$1").as("B")), Map("FORCE" -> true))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}"),
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )
  }

  test("copy parquet test: write with transformation") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileParquet"
    val df = session.read.parquet(testFileOnStage)

    createTable(testTableName, "NUM Bigint, STR variant, str_length bigint")
    df.copyInto(
      testTableName,
      Seq(
        sqlExpr("$1:num").cast(IntegerType).as("num"),
        sqlExpr("$1:str").as("str"),
        length(sqlExpr("$1:str")).as("str_length")
      )
    )

    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--NUM: Long (nullable = true)
           | |--STR: Variant (nullable = true)
           | |--STR_LENGTH: Long (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row(1, "\"str1\"", 4), Row(2, "\"str2\"", 4)))

    // copy again with existed table and FORCE = true.
    // use different order: length(str), str, num
    df.copyInto(
      testTableName,
      Seq(
        length(sqlExpr("$1:str")).as("str_length"),
        sqlExpr("$1:str").as("str"),
        sqlExpr("$1:num").cast(IntegerType).as("num")
      ),
      Map("FORCE" -> true)
    )
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--NUM: Long (nullable = true)
           | |--STR: Variant (nullable = true)
           | |--STR_LENGTH: Long (nullable = true)
           |""".stripMargin
    )

    checkAnswer(
      session.table(testTableName),
      Seq(
        Row(1, "\"str1\"", 4),
        Row(2, "\"str2\"", 4),
        Row(4, "\"str1\"", 1),
        Row(4, "\"str2\"", 2)
      )
    )
  }

  test("copy parquet test: negative test") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileParquet"
    val df = session.read.parquet(testFileOnStage)

    // case 1: copy without transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex1 = intercept[SnowparkClientException] {
      df.copyInto(testTableName)
    }
    assert(
      ex1.errorCode.equals("0122") && ex1.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 2: copy with transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex2 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq(col("$1").as("A")))
    }
    assert(
      ex2.errorCode.equals("0122") && ex2.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 3: COPY transformation doesn't match target table
    createTable(testTableName, "c1 String")
    // table has one column, transformation has 2 columns.
    val ex3 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1"), col("$2").as("c2")))
    }
    assert(ex3.getMessage.contains("Insert value list does not match column list"))
  }

  test("copy avro test: basic") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileAvro"
    val df = session.read.avro(testFileOnStage)

    createTable(testTableName, "A Variant")
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--A: Variant (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )

    // copy again, skip loaded files
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )

    // copy again with FORCE = true.
    df.copyInto(testTableName, Seq(col("$1").as("B")), Map("FORCE" -> true))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}"),
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )
  }

  test("copy avro test: write with transformation") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileAvro"
    val df = session.read.avro(testFileOnStage)

    createTable(testTableName, "NUM Bigint, STR variant, str_length bigint")
    df.copyInto(
      testTableName,
      Seq(
        sqlExpr("$1:num").cast(IntegerType).as("num"),
        sqlExpr("$1:str").as("str"),
        length(sqlExpr("$1:str")).as("str_length")
      )
    )

    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--NUM: Long (nullable = true)
           | |--STR: Variant (nullable = true)
           | |--STR_LENGTH: Long (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row(1, "\"str1\"", 4), Row(2, "\"str2\"", 4)))

    // copy again with existed table and FORCE = true.
    // use different order: length(str), str, num
    df.copyInto(
      testTableName,
      Seq(
        length(sqlExpr("$1:str")).as("str_length"),
        sqlExpr("$1:str").as("str"),
        sqlExpr("$1:num").cast(IntegerType).as("num")
      ),
      Map("FORCE" -> true)
    )
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--NUM: Long (nullable = true)
           | |--STR: Variant (nullable = true)
           | |--STR_LENGTH: Long (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row(1, "\"str1\"", 4),
        Row(2, "\"str2\"", 4),
        Row(4, "\"str1\"", 1),
        Row(4, "\"str2\"", 2)
      )
    )
  }

  test("copy avro test: negative test") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileAvro"
    val df = session.read.avro(testFileOnStage)

    // case 1: copy without transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex1 = intercept[SnowparkClientException] {
      df.copyInto(testTableName)
    }
    assert(
      ex1.errorCode.equals("0122") && ex1.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 2: copy with transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex2 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq(col("$1").as("A")))
    }
    assert(
      ex2.errorCode.equals("0122") && ex2.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 3: COPY transformation doesn't match target table
    createTable(testTableName, "c1 String")
    // table has one column, transformation has 2 columns.
    val ex3 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1"), col("$2").as("c2")))
    }
    assert(ex3.getMessage.contains("Insert value list does not match column list"))
  }

  test("copy orc test: basic") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileOrc"
    val df = session.read.orc(testFileOnStage)

    createTable(testTableName, "A Variant")
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--A: Variant (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )

    // copy again, skip loaded files
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )

    // copy again with FORCE = true.
    df.copyInto(testTableName, Seq(col("$1").as("B")), Map("FORCE" -> true))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}"),
        Row("{\n  \"num\": 1,\n  \"str\": \"str1\"\n}"),
        Row("{\n  \"num\": 2,\n  \"str\": \"str2\"\n}")
      )
    )
  }

  test("copy orc test: write with transformation") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileOrc"
    val df = session.read.orc(testFileOnStage)

    createTable(testTableName, "NUM Bigint, STR variant, str_length bigint")
    df.copyInto(
      testTableName,
      Seq(
        sqlExpr("$1:num").cast(IntegerType).as("num"),
        sqlExpr("$1:str").as("str"),
        length(sqlExpr("$1:str")).as("str_length")
      )
    )

    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--NUM: Long (nullable = true)
           | |--STR: Variant (nullable = true)
           | |--STR_LENGTH: Long (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row(1, "\"str1\"", 4), Row(2, "\"str2\"", 4)))

    // copy again with existed table and FORCE = true.
    // use different order: length(str), str, num
    df.copyInto(
      testTableName,
      Seq(
        length(sqlExpr("$1:str")).as("str_length"),
        sqlExpr("$1:str").as("str"),
        sqlExpr("$1:num").cast(IntegerType).as("num")
      ),
      Map("FORCE" -> true)
    )
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--NUM: Long (nullable = true)
           | |--STR: Variant (nullable = true)
           | |--STR_LENGTH: Long (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row(1, "\"str1\"", 4),
        Row(2, "\"str2\"", 4),
        Row(4, "\"str1\"", 1),
        Row(4, "\"str2\"", 2)
      )
    )
  }

  test("copy orc test: negative test") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileOrc"
    val df = session.read.orc(testFileOnStage)

    // case 1: copy without transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex1 = intercept[SnowparkClientException] {
      df.copyInto(testTableName)
    }
    assert(
      ex1.errorCode.equals("0122") && ex1.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 2: copy with transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex2 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq(col("$1").as("A")))
    }
    assert(
      ex2.errorCode.equals("0122") && ex2.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 3: COPY transformation doesn't match target table
    createTable(testTableName, "c1 String")
    // table has one column, transformation has 2 columns.
    val ex3 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1"), col("$2").as("c2")))
    }
    assert(ex3.getMessage.contains("Insert value list does not match column list"))
  }

  test("copy xml test: basic") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileXml"
    val df = session.read.xml(testFileOnStage)

    createTable(testTableName, "A Variant")
    df.copyInto(testTableName, Seq(col("$1").as("B")))
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--A: Variant (nullable = true)
           |""".stripMargin
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")
      )
    )

    // copy again, skip loaded files
    df.copyInto(testTableName, Seq(col("$1").as("A")))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")
      )
    )

    // copy again with FORCE = true.
    df.copyInto(testTableName, Seq(col("$1").as("B")), Map("FORCE" -> true))
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>"),
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")
      )
    )
  }

  test("copy xml test: write with transformation") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileXml"
    val df = session.read.xml(testFileOnStage)

    createTable(testTableName, "NUM Bigint, STR variant, str_length bigint")
    df.copyInto(
      testTableName,
      Seq(
        get(xmlget(col("$1"), lit("num"), lit(0)), lit("$")).cast(IntegerType).as("num"),
        get(xmlget(col("$1"), lit("str"), lit(0)), lit("$")).as("str"),
        length(get(xmlget(col("$1"), lit("str"), lit(0)), lit("$"))).as("str_length")
      )
    )
    assert(
      TestUtils.treeString(session.table(testTableName).schema, 0) ==
        s"""root
           | |--NUM: Long (nullable = true)
           | |--STR: Variant (nullable = true)
           | |--STR_LENGTH: Long (nullable = true)
           |""".stripMargin
    )
    checkAnswer(session.table(testTableName), Seq(Row(1, "\"str1\"", 4), Row(2, "\"str2\"", 4)))

    // copy again, skip loaded files
    df.copyInto(
      testTableName,
      Seq(
        get(xmlget(col("$1"), lit("num"), lit(0)), lit("$")).cast(IntegerType).as("num"),
        get(xmlget(col("$1"), lit("str"), lit(0)), lit("$")).as("str"),
        length(get(xmlget(col("$1"), lit("str"), lit(0)), lit("$"))).as("str_length")
      )
    )
    checkAnswer(session.table(testTableName), Seq(Row(1, "\"str1\"", 4), Row(2, "\"str2\"", 4)))

    // copy again with existed table and FORCE = true.
    // use different order: length(str), str, num
    df.copyInto(
      testTableName,
      Seq(
        length(get(xmlget(col("$1"), lit("str"), lit(0)), lit("$"))).as("str_length"),
        get(xmlget(col("$1"), lit("str"), lit(0)), lit("$")).as("str"),
        get(xmlget(col("$1"), lit("num"), lit(0)), lit("$")).cast(IntegerType).as("num")
      ),
      Map("FORCE" -> true)
    )
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row(1, "\"str1\"", 4),
        Row(2, "\"str2\"", 4),
        Row(4, "\"str1\"", 1),
        Row(4, "\"str2\"", 2)
      )
    )
  }

  test("copy xml test: negative test") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileXml"
    val df = session.read.xml(testFileOnStage)

    // case 1: copy without transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex1 = intercept[SnowparkClientException] {
      df.copyInto(testTableName)
    }
    assert(
      ex1.errorCode.equals("0122") && ex1.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 2: copy with transformation when target table doesn't exist
    dropTable(testTableName)(session)
    val ex2 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq(col("$1").as("A")))
    }
    assert(
      ex2.errorCode.equals("0122") && ex2.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."
      )
    )

    // case 3: COPY transformation doesn't match target table
    createTable(testTableName, "c1 String")
    // table has one column, transformation has 2 columns.
    val ex3 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1"), col("$2").as("c2")))
    }
    assert(ex3.getMessage.contains("Insert value list does not match column list"))
  }

  test("clone") {
    val testFileOnStage: String = s"@$tmpStageName/$testFileXml"
    val df = session.read.xml(testFileOnStage)

    try {
      createTable(testTableName, "A Variant")
      assert(df.isInstanceOf[CopyableDataFrame])
      val cloned = df.clone
      assert(cloned.isInstanceOf[CopyableDataFrame])
      cloned.copyInto(testTableName, Seq(col("$1").as("B")))
    } finally {
      dropTable(testTableName)
    }
  }

  test("async copyInto() basic") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "a Int, b String, c Double")
    assert(session.table(testTableName).count() == 0)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    // Execute copy in async mode
    val asyncJob = df.async.copyInto(testTableName)
    val res = asyncJob.getResult()
    assert(res.isInstanceOf[Unit])
    // Get copy result with getRows() and getIterator()
    val fileName = s"${tmpStageName.toLowerCase(Locale.ROOT)}/$testFileCsv"
    val expectedResult = Seq(Row(fileName, "LOADED", 2, 2, 1, 0, null, null, null, null))
    val copyResultRows = asyncJob.getRows()
    checkResult(copyResultRows, expectedResult)
    val copyResultIterator = asyncJob.getIterator()
    checkResultIterator(copyResultIterator, expectedResult)
    // Check result in target table
    checkAnswer(session.table(testTableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))

    // negative test to run copyInto
    createTable(testTableName, "a date, b date, c date")
    val asyncJob2 = df.async.copyInto(testTableName)
    val ex = intercept[SnowflakeSQLException] {
      asyncJob2.getResult()
    }
    assert(ex.getMessage.contains("is not recognized"))
  }

  test("async copyInto(): the other actions") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    // create target table
    createTable(testTableName, "c1 String, c2 String, c3 String")
    assert(session.table(testTableName).count() == 0)
    val df = session.read.schema(userSchema).csv(testFileOnStage)
    // copy data with transformation ($3, $2, $1)
    val asyncJob = df.async.copyInto(testTableName, Seq(col("$3"), col("$2"), col("$1")))
    val res1 = asyncJob.getResult()
    assert(res1.isInstanceOf[Unit])
    // Check result in target table
    checkAnswer(session.table(testTableName), Seq(Row("1.2", "one", "1"), Row("2.2", "two", "2")))

    // copy data with transformation ($1, $1, $1) and extra options
    val asyncJob2 = df.async.copyInto(
      testTableName,
      Seq(col("$1"), col("$1"), col("$1")),
      Map("skip_header" -> 1, "FORCE" -> "true")
    )
    val res2 = asyncJob2.getResult()
    assert(res2.isInstanceOf[Unit]) // Check result in target table
    checkAnswer(
      session.table(testTableName),
      Seq(Row("1.2", "one", "1"), Row("2.2", "two", "2"), Row("2", "2", "2"))
    )

    // copy data with transformation, options and target columns
    val asyncJob3 = df.async.copyInto(
      testTableName,
      Seq("c3", "c2", "c1"),
      Seq(length(col("$1")), col("$2"), length(col("$3"))),
      Map("FORCE" -> "true")
    )
    asyncJob3.getResult()
    val res3 = asyncJob3.getResult()
    assert(res3.isInstanceOf[Unit])
    // Check result in target table
    checkAnswer(
      session.table(testTableName),
      Seq(
        Row("1.2", "one", "1"),
        Row("2.2", "two", "2"),
        Row("2", "2", "2"),
        Row("3", "one", "1"),
        Row("3", "two", "1")
      )
    )
  }

}
