package com.snowflake.snowpark

import java.io.File
import java.sql.{Date, Time, Timestamp}
import com.snowflake.snowpark.internal.{
  JavaUtils,
  Logging,
  ParameterUtils,
  TypeToSchemaConverter,
  Utils
}
import com.snowflake.snowpark.internal.analyzer.quoteName
import com.snowflake.snowpark.types._

import java.math.{BigDecimal => JavaBigDecimal}
import java.lang.{
  Boolean => JavaBoolean,
  Byte => JavaByte,
  Double => JavaDouble,
  Float => JavaFloat,
  Integer => JavaInteger,
  Long => JavaLong,
  Short => JavaShort
}
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.util
import scala.collection.mutable.ArrayBuffer

class UtilsSuite extends SNTestBase {

  test("isSnowparkJar") {
    Seq(
      "snowpark.jar",
      "snowpark-0.3.0.jar",
      "snowpark-SNAPSHOT-0.4.0.jar",
      "SNOWPARK-0.2.1.jar",
      "snowpark-0.3.0.jar.gz").foreach(jarName => {
      assert(Utils.isSnowparkJar(jarName))
    })
    Seq("random.jar", "snow-0.3.0.jar", "snowpark", "snowpark.tar.gz").foreach(jarName => {
      assert(!Utils.isSnowparkJar(jarName))
    })

  }

  test("Logging") {
    LoggingTester.test()
  }

  test("utils.version") {
    // Stored Proc jdbc relies on Utils.Version. This test will prevent changes to this method.
    Utils.getClass.getMethod("Version")
  }

  test("test mask secrets") {
    // JDBC UNIT test: net.snowflake.client.util.SecretDetectorTest has done a lot of test
    // for SecretDetector.maskSecrets(msg).
    // We only run simple test to make sure it works.
    var messageText: String = "\"PASSWORD\": \"aslkjdflasjf\""
    var filteredMessageText: String = "\"PASSWORD\": \"**** "
    var result: String = Logging.maskSecrets(messageText)
    assert(result.equals(filteredMessageText))

    messageText = "PASSWORD=aslkjdflasjf"
    filteredMessageText = "PASSWORD=**** "
    result = Logging.maskSecrets(messageText)
    assert(result.equals(filteredMessageText))

    val password = "aslkjdflasjf1223"
    val exceptionMessage = s"PASSWORD=$password"
    val logMessage = s"PASSWORD : $password"
    result = Logging.maskSecrets(logMessage, new Exception(exceptionMessage))
    assert(!result.contains(password))

    assert(Logging.maskSecrets(null) == null)
  }

  test("normalize name") {

    assert(quoteName("\"_AF0*9A_\"") == "\"_AF0*9A_\"")
    assert(quoteName("\"_AF0 9A_\"") == "\"_AF0 9A_\"")

    // upper case
    assert(quoteName("abcd") == "\"ABCD\"")
    assert(quoteName("_09$aB") == "\"_09$AB\"")

    // don't upper case
    assert(quoteName("One One") == "\"One One\"")
    assert(quoteName("$1") == "\"$1\"")
    assert(quoteName("$a") == "\"$a\"")
  }

  test("schema inference") {
    assert(TypeToSchemaConverter.inferSchema[Byte]().head.dataType == ByteType)
    assert(TypeToSchemaConverter.inferSchema[Short]().head.dataType == ShortType)
    assert(TypeToSchemaConverter.inferSchema[Int]().head.dataType == IntegerType)
    assert(TypeToSchemaConverter.inferSchema[Long]().head.dataType == LongType)
    assert(TypeToSchemaConverter.inferSchema[Float]().head.dataType == FloatType)
    assert(TypeToSchemaConverter.inferSchema[Double]().head.dataType == DoubleType)
    assert(TypeToSchemaConverter.inferSchema[String]().head.dataType == StringType)
    assert(TypeToSchemaConverter.inferSchema[Boolean]().head.dataType == BooleanType)
    assert(TypeToSchemaConverter.inferSchema[BigDecimal]().head.dataType == DecimalType(34, 6))
    assert(
      TypeToSchemaConverter.inferSchema[JavaBigDecimal]().head.dataType ==
        DecimalType(34, 6))
    assert(TypeToSchemaConverter.inferSchema[Date]().head.dataType == DateType)
    assert(TypeToSchemaConverter.inferSchema[Timestamp]().head.dataType == TimestampType)
    assert(TypeToSchemaConverter.inferSchema[Time]().head.dataType == TimeType)
    assert(TypeToSchemaConverter.inferSchema[Array[Int]]().head.dataType == ArrayType(IntegerType))
    assert(
      TypeToSchemaConverter.inferSchema[Map[String, Boolean]]().head.dataType == MapType(
        StringType,
        BooleanType))
    assert(TypeToSchemaConverter.inferSchema[Variant]().head.dataType == VariantType)
    assert(TypeToSchemaConverter.inferSchema[Geography]().head.dataType == GeographyType)
    assert(TypeToSchemaConverter.inferSchema[Geometry]().head.dataType == GeometryType)

    // tuple
    assert(
      TypeToSchemaConverter
        .inferSchema[(Int, Boolean, Double, Geography, Map[String, Boolean], Geometry)]()
        .treeString(0) ==
        """root
      | |--_1: Integer (nullable = false)
      | |--_2: Boolean (nullable = false)
      | |--_3: Double (nullable = false)
      | |--_4: Geography (nullable = true)
      | |--_5: Map (nullable = true)
      | |--_6: Geometry (nullable = true)
      |""".stripMargin)

    // case class
    assert(
      TypeToSchemaConverter
        .inferSchema[Table1]()
        .treeString(0) ==
        """root
        | |--INT: Integer (nullable = false)
        | |--DOUBLE: Double (nullable = false)
        | |--VARIANT: Variant (nullable = true)
        | |--ARRAY: Array (nullable = true)
        |""".stripMargin)
  }

  case class Table1(int: Int, double: Double, variant: Variant, array: Array[String])

  test("schema inference java objects") {
    assert(
      TypeToSchemaConverter
        .inferSchema[Table2]()
        .treeString(0) == """root
                                     | |--BOOL: Boolean (nullable = true)
                                     | |--BYTE: Byte (nullable = true)
                                     | |--SHORT: Short (nullable = true)
                                     | |--INT: Integer (nullable = true)
                                     | |--LONG: Long (nullable = true)
                                     | |--FLOAT: Float (nullable = true)
                                     | |--DOUBLE: Double (nullable = true)
                                     |""".stripMargin)
  }

  case class Table2(
      bool: java.lang.Boolean,
      byte: java.lang.Byte,
      short: java.lang.Short,
      int: Integer,
      long: java.lang.Long,
      float: java.lang.Float,
      double: java.lang.Double)

  test("Non-nullable types") {
    TypeToSchemaConverter
      .inferSchema[(Boolean, Byte, Short, Int, Long, Float, Double)]()
      .treeString(0) ==
      """root
        | |--_1: Boolean (nullable = false)
        | |--_2: Byte (nullable = false)
        | |--_3: Short (nullable = false)
        | |--_4: Integer (nullable = false)
        | |--_5: Long (nullable = false)
        | |--_6: Float (nullable = false)
        | |--_7: Double (nullable = false)
        |""".stripMargin
  }

  test("Nullable types") {
    TypeToSchemaConverter
      .inferSchema[(
          Option[Int],
          JavaBoolean,
          JavaByte,
          JavaShort,
          JavaInteger,
          JavaLong,
          JavaFloat,
          JavaDouble,
          Array[Boolean],
          Map[String, Double],
          JavaBigDecimal,
          BigDecimal,
          Variant,
          Geography,
          Date,
          Time,
          Timestamp,
          Geometry)]()
      .treeString(0) ==
      """root
          | |--_1: Integer (nullable = true)
          | |--_2: Boolean (nullable = true)
          | |--_3: Byte (nullable = true)
          | |--_4: Short (nullable = true)
          | |--_5: Integer (nullable = true)
          | |--_6: Long (nullable = true)
          | |--_7: Float (nullable = true)
          | |--_8: Double (nullable = true)
          | |--_9: Array (nullable = true)
          | |--_10: Map (nullable = true)
          | |--_11: Decimal(34, 6) (nullable = true)
          | |--_12: Decimal(34, 6) (nullable = true)
          | |--_13: Variant (nullable = true)
          | |--_14: Geography (nullable = true)
          | |--_15: Date (nullable = true)
          | |--_16: Time (nullable = true)
          | |--_17: Timestamp (nullable = true)
          | |--_18: Geometry (nullable = true)
          |""".stripMargin
  }

  test("normalizeStageLocation") {
    val name1 = "stage"
    assert(Utils.normalizeStageLocation(name1 + "  ").equals(s"@$name1"))
    assert(Utils.normalizeStageLocation("@" + name1 + "  ").equals(s"@$name1"))
    val name2 = """"DATABASE"."SCHEMA"."STAGE""""
    assert(Utils.normalizeStageLocation(" " + name2).equals(s"@$name2"))
    assert(Utils.normalizeStageLocation("@" + name2 + "  ").equals(s"@$name2"))
  }

  test("normalizeLocalFile") {
    val name1 = "/tmp/absolute/path/file1.csv"
    assert(Utils.normalizeLocalFile(" " + name1 + " ").equals(s"file://$name1"))
    assert(Utils.normalizeLocalFile(" file://" + name1 + " ").equals(s"file://$name1"))
    val name2 = "relative/path/file2.csv"
    assert(Utils.normalizeLocalFile(" " + name2 + " ").equals(s"file://$name2"))
    assert(Utils.normalizeLocalFile(" file://" + name2 + " ").equals(s"file://$name2"))
    // Don't add 'file//' for single quoted file
    assert(Utils.normalizeLocalFile("'" + name1 + "'").equals("'" + name1 + "'"))
    assert(Utils.normalizeLocalFile("'" + name2 + "'").equals("'" + name2 + "'"))
  }

  test("md5") {
    val file = new File(getClass.getResource("/" + testFileAvro).toURI)

    assert(
      Utils.calculateMD5(file) ==
        "85bd7b9363853f1815254b1cbc608c22"
    ) // pragma: allowlist secret
  }

  test("stage file prefix length") {
    val stageName = "@stage" // stage/
    assert(Utils.stageFilePrefixLength(stageName) == 6)

    val stageName2 = "@stage/" // stage/
    assert(Utils.stageFilePrefixLength(stageName2) == 6)

    val stageName3 = """@"sta/ge"/""" // sta/ge/
    assert(Utils.stageFilePrefixLength(stageName3) == 7)

    val stageName4 = """@"stage.1"/dir""" // stage.1/dir/
    assert(Utils.stageFilePrefixLength(stageName4) == 12)

    val quotedStageName = """@"stage"""" // stage/
    assert(Utils.stageFilePrefixLength(quotedStageName) == 6)

    val quotedStageName2 = """@"stage"/""" // stage/
    assert(Utils.stageFilePrefixLength(quotedStageName2) == 6)

    val stagePrefix = "@stage/dir" // stage/dir/
    assert(Utils.stageFilePrefixLength(stagePrefix) == 10)

    val stagePrefix2 = """@"stage"/dir""" // stage/dir/
    assert(Utils.stageFilePrefixLength(stagePrefix2) == 10)

    val schemaStage = "@schema.stage" // stage/
    assert(Utils.stageFilePrefixLength(schemaStage) == 6)

    val schemaStage2 = "@schema.stage/" // stage/
    assert(Utils.stageFilePrefixLength(schemaStage2) == 6)

    val schemaStage3 = """@"schema".stage""" // stage/
    assert(Utils.stageFilePrefixLength(schemaStage3) == 6)

    val schemaStage4 = """@"schema".stage/""" // stage/
    assert(Utils.stageFilePrefixLength(schemaStage4) == 6)

    val schemaStage5 = """@"schema"."stage"""" // stage/
    assert(Utils.stageFilePrefixLength(schemaStage5) == 6)

    val schemaStage6 = """@"schema"."sta/ge"/""" // sta/ge/
    assert(Utils.stageFilePrefixLength(schemaStage6) == 7)

    val schemaStage7 = """@"schema.1".stage/dir""" // stage/dir/
    assert(Utils.stageFilePrefixLength(schemaStage7) == 10)

    val dbStage = """@db.schema.stage""" // stage/
    assert(Utils.stageFilePrefixLength(dbStage) == 6)

    val dbStage1 = """@db..stage""" // stage/
    assert(Utils.stageFilePrefixLength(dbStage1) == 6)

    val dbStage2 = """@db.schema.stage/""" // stage/
    assert(Utils.stageFilePrefixLength(dbStage2) == 6)

    val dbStage3 = """@db..stage/""" // stage/
    assert(Utils.stageFilePrefixLength(dbStage3) == 6)

    val dbStage4 = """@"db"."schema"."stage"""" // stage/
    assert(Utils.stageFilePrefixLength(dbStage4) == 6)

    val dbStage5 = """@"db".."stage"/""" // stage/
    assert(Utils.stageFilePrefixLength(dbStage5) == 6)

    val dbStage6 = """@"db.1"."schema.1"."stage.1"/dir""" // stage.1/dir/
    assert(Utils.stageFilePrefixLength(dbStage6) == 12)

    val tempStage =
      """@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849".SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL"""
    assert(Utils.stageFilePrefixLength(tempStage) == 36)

    val tempStage2 =
      """@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849".SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL/"""
    assert(Utils.stageFilePrefixLength(tempStage2) == 36)

    val tempStage3 =
      """@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849"."SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL"/"""
    assert(Utils.stageFilePrefixLength(tempStage3) == 36)

    val userStage = "@~/dir" // dir/
    assert(Utils.stageFilePrefixLength(userStage) == 4)

    val tableStage = """db.schema.%table/dir""" // dir/
    assert(Utils.stageFilePrefixLength(tableStage) == 4)
  }

  test("test parseStageFileLocation") {
    assert(Utils.parseStageFileLocation("@stage/path/file") == ("@stage", "path", "file"))
    assert(Utils.parseStageFileLocation("@stage/file") == ("@stage", "", "file"))
    assert(
      Utils.parseStageFileLocation("@\"st\\age\"/path/file")
        == ("@\"st\\age\"", "path", "file"))
    assert(
      Utils.parseStageFileLocation("@\"\\db\".\"\\Schema\".\"\\stage\"/path/file")
        == ("@\"\\db\".\"\\Schema\".\"\\stage\"", "path", "file"))
    assert(Utils.parseStageFileLocation("@stage/////file") == ("@stage", "///", "file"))
  }

  test("negative test parseStageFileLocation") {
    assertThrows[SnowparkClientException](Utils.parseStageFileLocation("@stage/path/"))
    assertThrows[SnowparkClientException](Utils.parseStageFileLocation("@stage"))

  }

  test("get java version") {
    assert(Utils.JavaVersion.nonEmpty)
  }

  val validIdentifiers = Seq(
    "a",
    "b.a",
    "c.b.a",
    "c..a",
    "table_name",
    "_azAz09$",
    "schema.table_name",
    "db.schema.table_name",
    "db..table_name",
    "\"table name\"",
    "\"SN_TEST_OBJECT_1364386155.!| @,#$\"",
    "\"schema \".\" table name\"",
    "\"db\".\"schema\".\"table_name\"",
    "\"db\"..\"table_name\"",
    "\"\"\"db\"\"\"..\"\"\"table_name\"\"\"",
    "\"\"\"name\"\"\"",
    "\"n\"\"am\"\"e\"",
    "\"\"\"na.me\"\"\"",
    "\"n\"\"a..m\"\"e\"",
    "\"schema\"\"\".\"n\"\"a..m\"\"e\"",
    "\"\"\"db\".\"schema\"\"\".\"n\"\"a..m\"\"e\"")

  test("test Utils.validateObjectName()") {
    validIdentifiers.foreach { name =>
      // println(s"test: $name")
      Utils.validateObjectName(name)
    }
  }

  test("test Utils.getUDFUploadPrefix()") {
    assert(Utils.getUDFUploadPrefix("name").equals("name"))
    assert(Utils.getUDFUploadPrefix("abcABC_0123456789").equals("abcABC_0123456789"))
    assert(Utils.getUDFUploadPrefix("\"name\"").equals("name_1077976085"))
    assert(Utils.getUDFUploadPrefix(" table").equals("table_1026248622"))
    assert(Utils.getUDFUploadPrefix("table ").equals("table_881377774"))
    assert(Utils.getUDFUploadPrefix("schema.view").equals("schemaview_1055679790"))
    assert(Utils.getUDFUploadPrefix(""""SCHEMA"."VIEW"""").equals("SCHEMAVIEW_1919772726"))
    assert(Utils.getUDFUploadPrefix("db.schema.table").equals("dbschematable_848839503"))
    assert(Utils.getUDFUploadPrefix(""""db"."schema"."table"""").equals("dbschematable_964272755"))

    validIdentifiers.foreach { name =>
      // println(s"test: $name")
      assert(Utils.getUDFUploadPrefix(name).matches("[\\w]+"))
    }
  }

  test("negative test Utils.validateObjectName()") {
    val names = Seq(
      "",
      ".table",
      "table.",
      ".table.",
      "table name",
      "table-name",
      "table!name",
      "table ",
      "\ttable",
      "table\n",
      "table_name\"",
      "\"table_name",
      ".name",
      "..name",
      "...name",
      ".db.name",
      "..db.name",
      "...db.name",
      ".d..b.name",
      ".db.schema.abc",
      ".db.schema..abc",
      "db. schema. table",
      "a.b.c.d",
      "a.b.c.",
      ".a.b.c",
      ".a.b.c.d",
      "\"db\".\"schema\".\"a\".\"table\"",
      "\"db\".\"schema\".\"a\".\"table\"",
      "\"\"",
      "\"a\"b\"",
      "a\"\"b\"",
      "a.\"\"b\"",
      "\"\".t",
      "\"a\"b\".t",
      "a\"\"b\".t",
      "a.\"\"b\".t",
      "\"\".c.t",
      "\"a\"b\".c.t",
      "a\"\"b\".c.t",
      ".\"name..\"",
      "..\"name\"",
      "\"\".\"name\"")

    names.foreach { name =>
      // println(s"negative test: $name")
      val ex = intercept[SnowparkClientException] { Utils.validateObjectName(name) }
      assert(ex.getMessage.replaceAll("\n", "").matches(".*The object name .* is invalid."))
    }
  }

  test("os name") {
    assert(Utils.OSName.nonEmpty)
  }

  test("convert windows path to linux") {
    val testItems = Seq(
      ("\\com\\snowflake\\snowpark\\", "/com/snowflake/snowpark/"),
      ("\\com\\snowflake\\snowpark\\TestClass.class", "/com/snowflake/snowpark/TestClass.class"),
      ("com\\snowflake\\snowpark\\", "com/snowflake/snowpark/"),
      ("com\\snowflake\\snowpark", "com/snowflake/snowpark"),
      ("/com/snowflake/snowpark/", "/com/snowflake/snowpark/"),
      ("/com/snowflake/snowpark/TestClass.class", "/com/snowflake/snowpark/TestClass.class"),
      ("com/snowflake/snowpark/", "com/snowflake/snowpark/"),
      ("com/snowflake/snowpark", "com/snowflake/snowpark"),
      ("d:", "d:"),
      ("d:\\dir", "d:/dir"))

    testItems.foreach { item =>
      assert(Utils.convertWindowsPathToLinux(item._1).equals(item._2))
    }
  }

  test("Utils.version matches sbt build") {
    assert(Utils.Version == "1.15.0-SNAPSHOT")
  }

  test("Utils.retrySleepTimeInMS") {
    var count = 0
    while (count < 10) {
      count = count + 1
      val sleep0 = Utils.retrySleepTimeInMS(0)
      assert(sleep0 >= 750 && sleep0 <= 1500)
      val sleep1 = Utils.retrySleepTimeInMS(1)
      assert(sleep1 >= 1500 && sleep1 <= 3000)
      val sleep2 = Utils.retrySleepTimeInMS(2)
      assert(sleep2 >= 3000 && sleep2 <= 6000)
      val sleep3 = Utils.retrySleepTimeInMS(3)
      assert(sleep3 >= 6000 && sleep3 <= 12000)
      val sleep4 = Utils.retrySleepTimeInMS(4)
      assert(sleep4 >= 12000 && sleep4 <= 24000)
      val sleep5 = Utils.retrySleepTimeInMS(5)
      assert(sleep5 >= 24000 && sleep5 <= 48000)
      val sleep6 = Utils.retrySleepTimeInMS(6)
      assert(sleep6 >= 30000 && sleep6 <= 60000)
      val sleep7 = Utils.retrySleepTimeInMS(7)
      assert(sleep7 >= 30000 && sleep7 <= 60000)
    }
  }

  test("Utils.isRetryable") {
    // positive test
    assert(Utils.isRetryable(new SnowflakeSQLException("JDBC driver internal error", "state_1")))
    assert(Utils.isRetryable(new SnowflakeSQLException("JDBC driver internal error", "state_2")))
    // negative test
    assert(!Utils.isRetryable(new Exception("test error")))
    assert(!Utils.isRetryable(new Exception("JDBC driver internal error")))
    assert(!Utils.isRetryable(new SnowflakeSQLException("test error", "state_1")))
  }

  test("Utils.withRetry") {
    val result = new ArrayBuffer[Int]()

    // retry 3 times, 1st failed, 2nd succeed
    result.clear()
    Utils.withRetry(3, "test_A") {
      result.append(1)
      if (result.size < 2) {
        throw new SnowflakeSQLException("JDBC driver internal error", "state_1")
      }
    }
    assert(result.size == 2)

    // retry 4 times, 1 & 2 failed, 3 succeed
    result.clear()
    Utils.withRetry(4, "test_A") {
      result.append(1)
      if (result.size < 3) {
        throw new SnowflakeSQLException("JDBC driver internal error", "state_1")
      }
    }
    assert(result.size == 3)

    // retry 3 times and all failed.
    result.clear()
    var ex = intercept[SnowflakeSQLException] {
      Utils.withRetry(3, "test_A") {
        result.append(1)
        throw new SnowflakeSQLException("JDBC driver internal error", "state_1")
      }
    }
    assert(ex.getMessage.contains("JDBC driver internal error"))
    assert(result.size == 3)

    // retry 3 times, but it is not retryable, so no retry
    result.clear()
    ex = intercept[SnowflakeSQLException] {
      Utils.withRetry(3, "test_A") {
        result.append(1)
        throw new SnowflakeSQLException("User error", "state_1")
      }
    }
    assert(ex.getMessage.contains("User error"))
    assert(result.size == 1)
  }

  test("Utils.isPutOrGetCommand") {
    // positive test
    assert(Utils.isPutOrGetCommand(" put file:///tmp/ @stage"))
    assert(Utils.isPutOrGetCommand(" get @stage file:///tmp/"))

    // negative test
    assert(!Utils.isPutOrGetCommand(null))
    assert(!Utils.isPutOrGetCommand(""))
    assert(!Utils.isPutOrGetCommand("select ' put ' "))
    assert(!Utils.isPutOrGetCommand("select ' get ' "))
    assert(!Utils.isPutOrGetCommand("show table "))
  }

  test("Utils.isStringEmpty") {
    assert(Utils.isStringEmpty(null))
    assert(Utils.isStringEmpty(""))
    assert(!Utils.isStringEmpty(" "))
    assert(!Utils.isStringEmpty("abc"))
  }

  test("java utils javaSaveModeToScala") {
    assert(
      JavaUtils.javaSaveModeToScala(com.snowflake.snowpark_java.SaveMode.Append)
        == SaveMode.Append)
    assert(
      JavaUtils.javaSaveModeToScala(com.snowflake.snowpark_java.SaveMode.Ignore)
        == SaveMode.Ignore)
    assert(
      JavaUtils.javaSaveModeToScala(com.snowflake.snowpark_java.SaveMode.Overwrite)
        == SaveMode.Overwrite)
    assert(
      JavaUtils.javaSaveModeToScala(com.snowflake.snowpark_java.SaveMode.ErrorIfExists)
        == SaveMode.ErrorIfExists)
  }

  test("isValidateJavaIdentifier()") {
    assert(Utils.isValidJavaIdentifier("a"))
    assert(Utils.isValidJavaIdentifier("_A"))
    assert(Utils.isValidJavaIdentifier("a123"))
    assert(Utils.isValidJavaIdentifier("a123___"))

    // negative test
    assert(!Utils.isValidJavaIdentifier(""))
    assert(!Utils.isValidJavaIdentifier("1"))
    assert(!Utils.isValidJavaIdentifier("1a"))
    assert(!Utils.isValidJavaIdentifier("a b"))
    assert(!Utils.isValidJavaIdentifier("a*b"))
    assert(!Utils.isValidJavaIdentifier(" ab"))
  }

  test("JavaUtils.readFileAsByteArray()") {
    val data =
      JavaUtils.readFileAsByteArray(this.getClass.getCanonicalName.replace('.', '/') + ".class")
    assert(data.nonEmpty)

    val ex = intercept[Exception] {
      JavaUtils.readFileAsByteArray("not_exist_file")
    }
    assert(ex.getMessage.equals("JavaUtils.readFileAsByteArray() cannot find file: not_exist_file"))
  }

  test("invalid private key") {
    val ex = intercept[SnowparkClientException](ParameterUtils.parsePrivateKey("fake key"))
    assert(ex.getMessage.contains("Invalid RSA private key"))
  }

  test("Utils.quoteForOption") {
    //      case b: Boolean => b.toString
    //      case i: Int => i.toString
    //      case it: Integer => it.toString
    //      case s: String if s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false") => s
    //      case _ => singleQuote(v.toString)
    assert(Utils.quoteForOption(true).equals("true"))
    assert(Utils.quoteForOption(false).equals("false"))
    assert(Utils.quoteForOption(123.toInt).equals("123"))
    assert(Utils.quoteForOption(Integer.valueOf(123)).equals("123"))
    assert(Utils.quoteForOption("TRUE").equals("TRUE"))
    assert(Utils.quoteForOption("FALSE").equals("FALSE"))
    assert(Utils.quoteForOption("abc").equals("'abc'"))
  }

  test("Scala and Json format transformation") {
    val javaHashMap = new util.HashMap[String, String]() {
      {
        put("one", "1")
        put("two", "2")
        put("three", "3")
      }
    }
    val map = Map(
      "nullKey" -> null,
      "integerKey" -> 42,
      "shortKey" -> 123.toShort,
      "longKey" -> 1234567890L,
      "byteKey" -> 123.toByte,
      "doubleKey" -> 3.1415926,
      "floatKey" -> 3.14f,
      "boolKey" -> false,
      "javaListKey" -> new util.ArrayList[String](util.Arrays.asList("a", "b")),
      "javaMapKey" -> javaHashMap,
      "seqKey" -> Seq(1, 2, 3),
      "arrayKey" -> Array(1, 2, 3),
      "seqOfStringKey" -> Seq("1", "2", "3"),
      "stringKey" -> "stringValue",
      "nestedMap" -> Map("insideKey" -> "stringValue", "insideList" -> Seq(1, 2, 3)),
      "nestedList" -> Seq(1, Map("nestedKey" -> "nestedValue"), Array(1, 2, 3)))
    val jsonString = Utils.mapToJson(map)
    val expected_string = "{" +
      "\"floatKey\":3.14," +
      "\"javaMapKey\":{" +
      "\"one\":\"1\"," +
      "\"two\":\"2\"," +
      "\"three\":\"3\"}," +
      "\"integerKey\":42," +
      "\"nullKey\":null," +
      "\"longKey\":1234567890," +
      "\"byteKey\":123," +
      "\"seqKey\":[1,2,3]," +
      "\"nestedMap\":{\"insideKey\":\"stringValue\",\"insideList\":[1,2,3]}," +
      "\"stringKey\":\"stringValue\"," +
      "\"doubleKey\":3.1415926," +
      "\"seqOfStringKey\":[\"1\",\"2\",\"3\"]," +
      "\"nestedList\":[1,{\"nestedKey\":\"nestedValue\"},[1,2,3]]," +
      "\"javaListKey\":[\"a\",\"b\"]," +
      "\"arrayKey\":[1,2,3]," +
      "\"boolKey\":false," +
      "\"shortKey\":123}"
    val readMap = Utils.jsonToMap(jsonString.getOrElse(""))
    val transformedString = Utils.mapToJson(readMap.getOrElse(Map()))
    assert(jsonString.getOrElse("").equals(expected_string))
    assert(jsonString.equals(transformedString))
  }
}

object LoggingTester extends Logging {
  def test(): Unit = {
    // no error report
    val password = "failed_error_log_password"
    logInfo(s"info PASSWORD=$password")
    logDebug(s"debug PASSWORD=$password")
    logTrace(s"trace PASSWORD=$password")
    logWarning(s"warning PASSWORD=$password")
    logError(s"error PASSWORD=$password")

    val exception = new Exception(s"PASSWORD : $password")

    logInfo(s"info PASSWORD=$password", exception)
    logDebug(s"debug PASSWORD=$password", exception)
    logTrace(s"trace PASSWORD=$password", exception)
    logWarning(s"warning PASSWORD=$password", exception)
    logError(s"error PASSWORD=$password", exception)

  }
}
