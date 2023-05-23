package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.{GeographyType, TimeType}
import com.snowflake.snowpark.{TestData, UnstableTest}

import java.sql.Types

class ResultSchemaSuite extends TestData {

  val stageName: String = randomName()
  val tableName: String = randomName()
  val fullTypesTable: String = randomName()
  val fullTypesTable2: String = randomName()
  val stageName1: String = randomName()
  val tableName1: String = randomName()
  val pipeName1: String = randomName()
  val viewName1: String = randomName()
  val streamName1: String = randomName()

  override def beforeAll: Unit = {
    super.beforeAll
    createStage(stageName)
    createTable(tableName, "num int")

    createTable(
      fullTypesTable,
      typeMap
        .map(row => s"""${row.colName} ${row.sfType}, "${row.colName}" ${row.sfType} not null,""")
        .reduce((x, y) => x + y)
        .dropRight(1)
        .stripMargin)

    createTable(
      fullTypesTable2,
      typeMap
        .map(row => s"""${row.colName} ${row.sfType},""")
        .reduce((x, y) => x + y)
        .dropRight(1)
        .stripMargin)
  }

  override def afterAll: Unit = {
    dropStage(stageName)
    dropTable(tableName)
    dropTable(fullTypesTable)
    dropTable(fullTypesTable2)

    runQuery(s"drop pipe if exists $pipeName1", session)
    runQuery(s"drop stream if exists $streamName1", session)
    dropTable(tableName1)
    dropStage(stageName1)
    dropView(viewName1)
    super.afterAll
  }

  test("alter") {
    verifySchema(
      "alter session set ABORT_DETACHED_QUERY=false",
      session.sql("alter session set ABORT_DETACHED_QUERY=false").schema)
  }

  test("list, remove file") {
    verifySchema(s"ls @$stageName", session.sql(s"ls @$stageName").schema)
    verifySchema(s"list @$stageName", session.sql(s"list @$stageName").schema)
    uploadFileToStage(stageName, testFileCsv, compress = false)
    uploadFileToStage(stageName, testFile2Csv, compress = false)
    verifySchema(
      s"rm @$stageName/$testFileCsv",
      session.sql(s"rm @$stageName/$testFile2Csv").schema)

    // Re-upload to test remove
    uploadFileToStage(stageName, testFileCsv, compress = false)
    uploadFileToStage(stageName, testFile2Csv, compress = false)
    verifySchema(
      s"remove @$stageName/$testFileCsv",
      session.sql(s"remove @$stageName/$testFile2Csv").schema)
  }

  test("select") {
    verifySchema(s"select * from  $tableName", session.sql(s"select * from  $tableName").schema)
  }

  test("analyze schema") {
    verifySchema(s"select * from $fullTypesTable", session.table(fullTypesTable).schema)
    val table = session.table(fullTypesTable)
    val df1 = table.select("string", "\"int\"", "array", "\"date\"")
    verifySchema(s"""select string, "int", array, "date" from $fullTypesTable""", df1.schema)
    val df2 = df1.filter(col("\"int\"") > 0)
    verifySchema(
      s"""select string, "int", array, "date" from $fullTypesTable where \"int\" > 0""",
      df2.schema)
  }

  // ignore it for now since we are modifying the analyzer system.
  // the schema string system may be not work as previous anymore.
  // TODO: reconsider this test after the new analyzer deployed in SNOW-562493
  ignore("use VALUES instead of real SQL query", UnstableTest) {
    var df = columnNameHasSpecialCharacter
    val start = System.currentTimeMillis()
    (1 to 50).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
    val time1 = System.currentTimeMillis()
    (1 to 50).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
    (1 to 50).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
    (1 to 50).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
    val time4 = System.currentTimeMillis()
    /*
     * t1, t2 ,t3 ,t4 should be similar, then
     * (t1 + t2 + t3 + t4) / 4 = t1
     * if it progressively increasing, then
     * t2 = 2t1, t3 = 3t1, t4 = 4t1, and
     * (t1 + t2 + t3 + t4) / 4 = 2.5 t1
     */
    assert((time4 - start) / 4 < (time1 - start) * 1.5)
  }

  test("verify full schema type") {
    val statement = runQueryReturnStatement(s"select * from $fullTypesTable2", session)
    val resultMeta = statement.getResultSet.getMetaData
    val columnCount = resultMeta.getColumnCount
    val tsSchema = session.table(fullTypesTable2).schema
    (0 until columnCount).foreach(index => {
      assert(resultMeta.getColumnType(index + 1) == typeMap(index).jdbcType)
      assert(
        getDataType(
          resultMeta.getColumnType(index + 1),
          resultMeta.getColumnTypeName(index + 1),
          resultMeta.getPrecision(index + 1),
          resultMeta.getScale(index + 1),
          resultMeta.isSigned(index + 1)) == typeMap(index).tsType)
      assert(tsSchema(index).dataType == typeMap(index).tsType)
    })
    statement.close()
  }

  test("verify Geometry schema type") {
    try {
      runQuery(s"alter session set GEOGRAPHY_OUTPUT_FORMAT='GeoJSON'", session)
      var statement = runQueryReturnStatement(s"select geography from $fullTypesTable2", session)
      var resultMeta = statement.getResultSet.getMetaData
      var tsSchema = session.table(fullTypesTable2).select(col("geography")).schema
      assert(resultMeta.getColumnType(1) == Types.VARCHAR)
      assert(tsSchema.head.dataType == GeographyType)
      statement.close()

      runQuery(s"alter session set GEOGRAPHY_OUTPUT_FORMAT='WKT'", session)
      statement = runQueryReturnStatement(s"select geography from $fullTypesTable2", session)
      resultMeta = statement.getResultSet.getMetaData
      tsSchema = session.table(fullTypesTable2).select(col("geography")).schema
      assert(resultMeta.getColumnType(1) == Types.VARCHAR)
      assert(tsSchema.head.dataType == GeographyType)
      statement.close()

      runQuery(s"alter session set GEOGRAPHY_OUTPUT_FORMAT='WKB'", session)
      statement = runQueryReturnStatement(s"select geography from $fullTypesTable2", session)
      resultMeta = statement.getResultSet.getMetaData
      tsSchema = session.table(fullTypesTable2).select(col("geography")).schema
      assert(resultMeta.getColumnType(1) == Types.BINARY)
      assert(tsSchema.head.dataType == GeographyType)
      statement.close()
    } finally {
      // Assign output format to the default value
      runQuery(s"alter session set GEOGRAPHY_OUTPUT_FORMAT='GeoJSON'", session)
    }
  }

  test("verify Time schema type") {
    val statement = runQueryReturnStatement(s"select time from $fullTypesTable2", session)
    val resultMeta = statement.getResultSet.getMetaData
    val tsSchema = session.table(fullTypesTable2).select(col("time")).schema
    assert(resultMeta.getColumnType(1) == Types.TIME)
    assert(tsSchema.head.dataType == TimeType)
    statement.close()
  }
}
