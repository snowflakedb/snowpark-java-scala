package com.snowflake.snowpark

import com.snowflake.snowpark.internal.analyzer.StagedFileReader
import com.snowflake.snowpark.types._

class StagedFileReaderSuite extends SNTestBase {

  private val userSchema: StructType = StructType(
    Seq(StructField("a", IntegerType), StructField("b", StringType), StructField("c", DoubleType))
  )

  test("File Format Type") {
    val fileReadOrCopyPlanBuilder = new StagedFileReader(session)

    assert(fileReadOrCopyPlanBuilder.format("csv").formatType.equals("CSV"))
    assert(fileReadOrCopyPlanBuilder.format("CSV").formatType.equals("CSV"))
    assert(fileReadOrCopyPlanBuilder.format("json").formatType.equals("JSON"))
    assert(fileReadOrCopyPlanBuilder.format("Json").formatType.equals("JSON"))
    assert(fileReadOrCopyPlanBuilder.format("parquet").formatType.equals("PARQUET"))
    assert(fileReadOrCopyPlanBuilder.format("Parquet").formatType.equals("PARQUET"))
    assert(fileReadOrCopyPlanBuilder.format("avro").formatType.equals("AVRO"))
    assert(fileReadOrCopyPlanBuilder.format("AVRO").formatType.equals("AVRO"))
    assert(fileReadOrCopyPlanBuilder.format("ORC").formatType.equals("ORC"))
    assert(fileReadOrCopyPlanBuilder.format("orc").formatType.equals("ORC"))
    assert(fileReadOrCopyPlanBuilder.format("Xml").formatType.equals("XML"))
    assert(fileReadOrCopyPlanBuilder.format("XML").formatType.equals("XML"))

    val ex = intercept[SnowparkClientException] {
      fileReadOrCopyPlanBuilder.format("unknown_type")
    }
    assert(ex.message.contains("Internal error: unsupported file format type: 'unknown_type'."))
  }

  test("option") {
    val fileReadOrCopyPlanBuilder = new StagedFileReader(session)

    val configs: Map[String, Any] = Map(
      "Boolean" -> true,
      "Int" -> 123,
      "Integer" -> java.lang.Integer.valueOf("1"),
      "true" -> "True",
      "false" -> "false",
      "string" -> "string"
    )
    val savedOptions = fileReadOrCopyPlanBuilder.options(configs).curOptions
    assert(savedOptions.size == 6)
    assert(savedOptions("BOOLEAN").equals("true"))
    assert(savedOptions("INT").equals("123"))
    assert(savedOptions("INTEGER").equals("1"))
    assert(savedOptions("TRUE").equals("True"))
    assert(savedOptions("FALSE").equals("false"))
    assert(savedOptions("STRING").equals("'string'"))
  }

  test("create plan for copy") {
    val stageLocation = "@myStage/prefix1/prefix2"
    val builder = new StagedFileReader(session)
      .userSchema(userSchema)
      .path(stageLocation)
      .option("SKIP_HEADER", 10)
      .options(Map("DATE_FORMAT" -> "YYYY-MM-DD"))
      .format("csv")
      .table("test_table_name")

    val plan = builder.createSnowflakePlan()
    assert(plan.queries.size == 2)
    val crt = plan.queries.head.sql
    assert(
      TestUtils
        .containIgnoreCaseAndWhiteSpaces(crt, s"create table test_table_name if not exists")
    )
    val copy = plan.queries.last.sql
    assert(TestUtils.containIgnoreCaseAndWhiteSpaces(copy, s"copy into test_table_name"))
    assert(TestUtils.containIgnoreCaseAndWhiteSpaces(copy, "skip_header = 10"))
    assert(TestUtils.containIgnoreCaseAndWhiteSpaces(copy, "DATE_FORMAT = 'YYYY-MM-DD'"))
    assert(TestUtils.containIgnoreCaseAndWhiteSpaces(copy, "TYPE = CSV"))
  }

}
