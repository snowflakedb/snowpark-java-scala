package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.ServerConnection
import com.snowflake.snowpark.types.{GeographyType, TimeType}
import com.snowflake.snowpark.{JavaStoredProcExclude, TestData, UnstableTest}
import net.snowflake.client.jdbc.SnowflakeSQLException

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

  test("show database objects") {
    verifySchema("show schemas", session.sql("show schemas").schema)
    verifySchema("show objects", session.sql("show objects").schema)
    verifySchema("show external tables", session.sql("show external tables").schema)
    verifySchema("show views", session.sql("show views").schema)
    verifySchema("show columns", session.sql("show columns").schema)
    verifySchema("show file formats", session.sql("show file formats").schema)
    verifySchema("show sequences", session.sql("show sequences").schema)
    verifySchema("show stages", session.sql("show stages").schema)
    verifySchema("show pipes", session.sql("show pipes").schema)
    verifySchema("show streams", session.sql("show streams").schema)
    verifySchema("show tasks", session.sql("show tasks").schema)
    verifySchema("show user functions", session.sql("show user functions").schema)
    verifySchema("show external functions", session.sql("show external functions").schema)
    verifySchema("show procedures", session.sql("show procedures").schema)
  }

  test("show account object") {
    verifySchema("show functions", session.sql("show functions").schema)
    verifySchema("show network policies", session.sql("show network policies").schema)
    verifySchema("show roles", session.sql("show roles").schema)
    verifySchema("show databases", session.sql("show databases").schema)
  }

  test("show session operations") {
    verifySchema("show variables", session.sql("show variables").schema)
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

  test("insert into") {
    verifySchema(
      s"insert into $tableName values(1)",
      session.sql(s"insert into $tableName values(1)").schema)
  }

  test("delete") {
    verifySchema(
      s"delete from $tableName where num = 1",
      session.sql(s"delete from $tableName where num = 1").schema)
  }

  test("update") {
    verifySchema(
      s"update $tableName set num = 2 where num = 1",
      session.sql(s"update $tableName set num = 2 where num = 1").schema)
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

  test("these show commands also works on Jenkins now") {
    verifySchema("show tables", session.sql("show tables").schema)
    verifySchema("show parameters", session.sql("show parameters").schema)
    verifySchema("show shares", session.sql("show shares").schema)
    verifySchema("show warehouses", session.sql("show warehouses").schema)
    verifySchema("show transactions", session.sql("show transactions").schema)
    verifySchema("show locks", session.sql("show locks").schema)
    verifySchema("show regions", session.sql("show regions").schema)
  }

  test("terse") {
    verifySchema("show terse databases", session.sql("show terse databases").schema)
    verifySchema("show terse schemas", session.sql("show terse schemas").schema)
    verifySchema("show terse tables", session.sql("show terse tables").schema)
    verifySchema("show terse views", session.sql("show terse views").schema)
    verifySchema("show terse streams", session.sql("show terse streams").schema)
    verifySchema("show terse tasks", session.sql("show terse tasks").schema)
    verifySchema("show terse external tables", session.sql("show terse external tables").schema)
  }

  test("invalid query show commands") {
    assertThrows[SnowflakeSQLException](session.sql("show").schema)
    assertThrows[SnowflakeSQLException](session.sql("show table").schema)
    assertThrows[SnowflakeSQLException](session.sql("show tables abc").schema)
    assertThrows[SnowflakeSQLException](session.sql("show external abc").schema)
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

  // Java Stored Proc does not support use in owner's right.
  test("use", JavaStoredProcExclude) {
    val currentDB = session.getCurrentDatabase.get
    val currentSchema = session.getCurrentSchema.get
    verifySchema(s"use database $currentDB", session.sql(s"use database $currentDB").schema)
    verifySchema(s"use schema $currentSchema", session.sql(s"use schema $currentSchema").schema)
  }

  test("create drop") {
    verifySchema(
      s"create or replace table $tableName1 (num int)",
      session.sql(s"create or replace table $tableName1 (num int)").schema)
    verifySchema(s"drop table $tableName1", session.sql(s"drop table $tableName1").schema)

    runQuery(s"create or replace table $tableName1 (num int)", session)
    verifySchema(
      s"create or replace stream $streamName1 on table $tableName1",
      session.sql(s"create or replace stream $streamName1 on table $tableName1").schema)
    verifySchema(s"drop stream $streamName1", session.sql(s"drop stream $streamName1").schema)

    verifySchema(
      s"create or replace stage $stageName1",
      session.sql(s"create or replace stage $stageName1").schema)
    verifySchema(s"drop stage $stageName1", session.sql(s"drop stage $stageName1").schema)

    verifySchema(
      s"create or replace view $viewName1 as select * from $tableName1",
      session.sql(s"create or replace view $viewName1 as select * from $tableName1").schema)
    verifySchema(s"drop view $viewName1", session.sql(s"drop view $viewName1").schema)

    runQuery(s"create or replace stage $stageName1", session)
    verifySchema(
      s"create or replace pipe $pipeName1 as copy into $tableName1 from @$stageName1",
      session
        .sql(s"create or replace pipe $pipeName1 as copy into $tableName1 from @$stageName1")
        .schema)

    verifySchema(s"drop pipe $pipeName1", session.sql(s"drop pipe $pipeName1").schema)
  }

  test("comment on") {
    runQuery(s"create or replace table $tableName1 (num int)", session)
    verifySchema(
      s"comment on table $tableName1 is 'test'",
      session.sql(s"comment on table $tableName1 is 'test'").schema)
  }

  test("grant/revoke") {
    val tempTable = randomName()
    val currentRole: String = session.sql("select current_role()").collect().head.getString(0)
    runQuery(s"create or replace temp table $tempTable (num int)", session)

    verifySchema(
      s"grant select on table $tempTable to role $currentRole",
      session.sql(s"grant select on table $tempTable to role $currentRole").schema)

    verifySchema(
      s"revoke select on table $tempTable from role $currentRole",
      session.sql(s"revoke select on table $tempTable from role $currentRole").schema)
  }

  test("describe") {
    runQuery(s"create or replace table $tableName1 (num int)", session)
    verifySchema(s"describe table $tableName1", session.sql(s"describe table $tableName1").schema)

    runQuery(s"create or replace stream $streamName1 on table $tableName1", session)
    verifySchema(
      s"describe stream $streamName1",
      session.sql(s"describe stream $streamName1").schema)

    runQuery(s"create or replace stage $stageName1", session)
    verifySchema(s"describe stage $stageName1", session.sql(s"describe stage $stageName1").schema)

    runQuery(s"create or replace view $viewName1 as select * from $tableName1", session)
    verifySchema(s"describe view $viewName1", session.sql(s"describe view $viewName1").schema)

    runQuery(
      s"create or replace pipe $pipeName1 as copy into $tableName1 from @$stageName1",
      session)
    verifySchema(s"describe pipe $pipeName1", session.sql(s"describe pipe $pipeName1").schema)
  }
}
