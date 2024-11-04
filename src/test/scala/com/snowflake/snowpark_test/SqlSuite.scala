package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.io.File
import scala.reflect.io.Directory

trait SqlSuite extends SNTestBase {
  protected val tableName1: String = randomName()
  protected val tableName2: String = randomName() + "_SQLTEST"

  protected val stageName: String = randomName()
  protected val procName: String = randomName()

  override def beforeAll: Unit = {
    super.beforeAll()
    createTable(tableName1, "num int")
    createStage(stageName)
    runQuery(
      s"""
                        |create or replace procedure $procName()
                        |returns varchar
                        |language javascript
                        |as
                        |$$$$
                        |var rs = snowflake.execute( { sqlText:
                        |`show tables`
                        |} )
                        |return 'Done'
                        |$$$$
                        |""".stripMargin,
      session)
  }

  override def afterAll: Unit = {
    dropTable(tableName1)
    dropStage(stageName)
    runQuery(s"drop procedure $procName()", session)
    val statement = runQueryReturnStatement("SHOW TABLES", session)
    val tablesResult = statement.getResultSet
    while (tablesResult.next()) {
      assert(tablesResult.getString("name") != tableName2)
    }
    statement.close()
    super.afterAll()
  }

  test("non select queries") {
    val result = session.sql(s"show stages like '$stageName'").collect()
    assert(result.length == 1)
    // verify result is not empty
    assert(result(0).toString.contains(stageName))

    val result1 = session.sql(s"show tables like '$tableName1'").collect()
    assert(result1.length == 1)
    // verify result is not empty
    assert(result1(0).toString.contains(tableName1))

    val result2 = session.sql(s"alter session set lock_timeout = 3600").collect()
    assert(result2.length == 1)
    // verify result is not empty
    assert(result2(0).toString.contains("Statement executed successfully"))

    val result3 = session.sql(s"call $procName()").collect()
    assert(result3.length == 1)
    // verify result is not empty
    assert(result3(0).toString.contains("Done"))
  }

  test("put/get should not be performed when preparing") {
    val fileName = "test.xml"
    val path = this.getClass.getResource(s"/$fileName")

    // add spaces to the query
    val putQuery =
      s" put ${TestUtils.escapePath(path.toString).replace("file:/", "file:///")} @$stageName "
    val put = session.sql(putQuery)
    put.schema.printTreeString()
    // should upload nothing
    assert(session.sql(s"ls @$stageName").collect().isEmpty)

    // verify hard code put schema, it will execute the query
    verifySchema(putQuery, put.schema)

    val outputPath = TestUtils.tempDirWithEscape + randomName()
    // add spaces to the query
    val getQuery = s"get @$stageName/$fileName.gz file://$outputPath"
    val get = session.sql(getQuery)
    // should download nothing
    get.schema.printTreeString()
    assert(!new File(s"$outputPath/$fileName.gz").exists())

    try {
      // verify hard code get schema, and execute the query
      verifySchema(getQuery, get.schema)
      // TODO: Below assertion failed on GCP because JDBC bug SNOW-493080
      // Disable this check temporally
      // assert(new File(s"$outputPath/$fileName.gz").exists())
    } finally {
      // remove tmp file
      new Directory(new File(outputPath)).deleteRecursively()
    }
  }

  test("Run sql query with bindings") {
    val df1 = session.sql("select * from values (?),(?),(?)", List(1, 2, 3))
    assert(df1.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    val df2 =
      session.sql(
        "select variance(identifier(?)) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a, b)",
        Seq("a"))
    assert(df2.collect()(0).getDecimal(0).toString == "0.800000")

    val df3 = session
      .sql("select * from values (?),(?),(?) as T(id)", Seq(1, 2, 3))
      .filter(col("id") < 3)
    assert(df3.collect() sameElements Array[Row](Row(1), Row(2)))

    val df4 =
      session.sql("select * from values (?,?),(?,?),(?,?) as T(a, b)", Seq(1, 1, 2, 1, 3, 1))
    val df5 =
      session.sql("select * from values (?,?),(?,?),(?,?) as T(a, b)", List(1, 2, 2, 1, 4, 3))
    val df6 = df4.union(df5).filter(col("a") < 3)
    assert(df6.collect() sameElements Array[Row](Row(1, 1), Row(2, 1), Row(1, 2)))

    val df7 = df4.join(df5, Seq("a", "b"), "inner")
    assert(df7.collect() sameElements Array[Row](Row(2, 1)))

    // Async result
    assert(df1.async.collect().getResult() sameElements Array[Row](Row(1), Row(2), Row(3)))
    assert(
      df6.async.collect().getResult() sameElements Array[Row](Row(1, 1), Row(2, 1), Row(1, 2)))
  }
}

class EagerSqlSuite extends SqlSuite with EagerSession {
  test("Run sql query") {
    val df1 = session.sql("select * from values (1),(2),(3)")
    assert(df1.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    val df2 =
      session.sql("select variance(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b)")

    assert(df2.collect()(0).getDecimal(0).toString == "0.800000")

    val df3 = session.sql("select * from values (1),(2),(3) as T(id)").filter(col("id") < 3)
    assert(df3.collect() sameElements Array[Row](Row(1), Row(2)))

    val df4 = session.sql("select * from values (1,1),(2,1),(3,1) as T(a,b)")
    val df5 = session.sql("select * from values (1,2),(2,2),(3,2) as T(a,b)")
    val df6 = df4.union(df5).filter(col("a") < 3)
    assert(df6.collect() sameElements Array[Row](Row(1, 1), Row(2, 1), Row(1, 2), Row(2, 2)))

    assertThrows[SnowflakeSQLException] {
      session.sql("select * from (1)")
    }

    assertThrows[SnowflakeSQLException] {
      session.sql("select sum(a) over () from values 1.0, 2.0, 3.0 T(a)")
    }
  }

  test("create table") {
    // create table
    val table = session.sql(s"create or replace table $tableName2 (num int)")
    assert(table.schema.nonEmpty)

    // assert the table is not created before collect
    assertThrows[SnowflakeSQLException](session.sql(s"select * from $tableName2").collect())
    table.collect()

    // drop table
    val dropTable = session.sql(s"drop table $tableName2")
    assert(dropTable.schema.nonEmpty)
    dropTable.collect()
    // assert that the table is already dropped
    assertThrows[SnowflakeSQLException](session.sql(s"select * from $tableName2"))

    // test when create/drop table fails
    // throws exception during prepare
    val name = randomName()
    assertThrows[SnowflakeSQLException](session.sql(s"create or replace table $name"))
    assertThrows[SnowflakeSQLException](session.sql(s"drop table $name"))
  }

  test("insert into table") {
    val insert = session.sql(s"insert into $tableName1 values(1),(2),(3)")
    // test expected schema for insert
    val expectedSchema: StructType =
      StructType(StructField("\"number of rows inserted\"", LongType, nullable = false) :: Nil)
    assert(insert.schema == expectedSchema)
    insert.collect()
    val df = session.sql(s"select * from $tableName1")
    assert(df.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    // test for insertion to a non-existing table
    assertThrows[SnowflakeSQLException](session.sql(s"insert into $tableName2 values(1),(2),(3)"))

    // test for insertion with wrong type of data, throws exception when collect
    val insert2 = session.sql(s"insert into $tableName1 values(1.4),('test')")
    assertThrows[SnowflakeSQLException](insert2.collect())
  }

  test("show") {
    val tableSqlResult = session.sql("SHOW TABLES")
    verifySchema("SHOW TABLES", tableSqlResult.schema)
    val collectedResult = tableSqlResult.collect()
    assert(collectedResult.length > 0)

    // test when input is a wrong show command, throws exception when prepare
    assertThrows[SnowflakeSQLException](session.sql("SHOW TABLE"))
  }
}

class LazySqlSuite extends SqlSuite with LazySession {
  test("Run sql query") {
    val df1 = session.sql("select * from values (1),(2),(3)")
    assert(df1.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    val df2 =
      session.sql("select variance(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b)")

    assert(df2.collect()(0).getDecimal(0).toString == "0.800000")

    val df3 = session.sql("select * from values (1),(2),(3) as T(id)").filter(col("id") < 3)
    assert(df3.collect() sameElements Array[Row](Row(1), Row(2)))

    val df4 = session.sql("select * from values (1,1),(2,1),(3,1) as T(a,b)")
    val df5 = session.sql("select * from values (1,2),(2,2),(3,2) as T(a,b)")
    val df6 = df4.union(df5).filter(col("a") < 3)
    assert(df6.collect() sameElements Array[Row](Row(1, 1), Row(2, 1), Row(1, 2), Row(2, 2)))
  }

  test("create table") {
    // create table
    val table = session.sql(s"create or replace table $tableName2 (num int)")
    assert(table.schema.nonEmpty)

    // assert the table is not created before collect
    assertThrows[SnowflakeSQLException](session.sql(s"select * from $tableName2").collect())
    table.collect()

    // drop table
    val dropTable = session.sql(s"drop table $tableName2")
    assert(dropTable.schema.nonEmpty)
    dropTable.collect()
    // assert that the table is already dropped
    session.sql(s"select * from $tableName2") // no error
    assertThrows[SnowflakeSQLException](session.sql(s"select * from $tableName2").collect())

    // test when create/drop table fails
    // throws exception during prepare
    val name = randomName()
    session.sql(s"create or replace table $name") // no error
    assertThrows[SnowflakeSQLException](session.sql(s"create or replace table $name").collect())
    session.sql(s"drop table $name") // no error
    assertThrows[SnowflakeSQLException](session.sql(s"drop table $name").collect())
  }

  test("insert into table") {
    val insert = session.sql(s"insert into $tableName1 values(1),(2),(3)")
    // test expected schema for insert
    val expectedSchema: StructType =
      StructType(StructField("\"number of rows inserted\"", LongType, nullable = false) :: Nil)
    assert(insert.schema == expectedSchema)
    insert.collect()
    val df = session.sql(s"select * from $tableName1")
    assert(df.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    // test for insertion to a non-existing table
    session.sql(s"insert into $tableName2 values(1),(2),(3)") // no error
    assertThrows[SnowflakeSQLException](
      session.sql(s"insert into $tableName2 values(1),(2),(3)").collect())

    // test for insertion with wrong type of data, throws exception when collect
    val insert2 = session.sql(s"insert into $tableName1 values(1.4),('test')")
    assertThrows[SnowflakeSQLException](insert2.collect())
  }

  test("show") {
    val tableSqlResult = session.sql("SHOW TABLES")
    verifySchema("SHOW TABLES", tableSqlResult.schema)
    val collectedResult = tableSqlResult.collect()
    assert(collectedResult.length > 0)

    // test when input is a wrong show command, throws exception when prepare
    session.sql("SHOW TABLE") // no error
    assertThrows[SnowflakeSQLException](session.sql("SHOW TABLE").collect())
  }
}
