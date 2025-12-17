package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.types._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.sql.Time
import java.util.Locale

class TableSuite extends TestData {

  val tableName: String = randomName()
  val tableName2: String = randomName()
  val tableName3: String = randomName()
  val tableName4: String = randomName()
  val tableName5: String = randomName()
  val tableName6: String = randomName()
  val semiStructuredTable: String = randomName()
  val timeTable: String = randomName()
  val quotedName: String = s""""${randomName()}""""
  val tempTableName = randomName()

  override def beforeAll: Unit = {
    super.beforeAll()
    testWithTimezone() {
      val tableFromDifferentSchema = getFullyQualifiedTempSchema + "." + tempTableName
      createTable(tableName, "num int")
      runQuery(s"insert into $tableName values(1),(2),(3)", session)
      createTable(tableName4, "num int")
      runQuery(s"insert into $tableName4 values(1),(2),(3)", session)
      createTable(tableFromDifferentSchema, "str string")
      runQuery(s"insert into $tableFromDifferentSchema values('abc')", session)
      createTable(quotedName, "num int")
      runQuery(s"insert into $quotedName values(1),(2)", session)
      createTable(semiStructuredTable, "a1 array, o1 object, v1 variant, g1 geography")
      runQuery(
        s"insert into $semiStructuredTable select parse_json(a), parse_json(b), " +
          s"parse_json(a), to_geography(c) from values('[1,2]', '{a:1}', 'POINT(-122.35 37.55)')," +
          s"('[1,2,3]', '{b:2}', 'POINT(-12 37)') as T(a,b,c)",
        session)
      createTable(timeTable, "time time")
      runQuery(
        s"insert into $timeTable select to_time(a) from values('09:15:29')," +
          s"('09:15:29.99999999') as T(a)",
        session)
    }
  }

  override def afterAll: Unit = {
    val tableFromDifferentSchema = getFullyQualifiedTempSchema + "." + tempTableName
    dropTable(tableName)
    dropTable(tableName2)
    dropTable(tableName3)
    dropTable(tableName4)
    dropTable(tableName5)
    dropTable(tableName6)
    dropTable(tableFromDifferentSchema)
    dropTable(quotedName)
    dropTable(semiStructuredTable)
    dropTable(timeTable)
    super.afterAll()
  }

  test("Read Snowflake Table") {
    val df = session.table(tableName)

    assert(df.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))
    val df1 = session.read.table(tableName)
    assert(df1.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    val dbSchemaTableName: String =
      session.getCurrentDatabase.get + "." + session.getCurrentSchema.get + "." + tableName

    val df2 = session.table(dbSchemaTableName)
    assert(df2.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    val df3 = session.read.table(dbSchemaTableName)
    assert(df3.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))
  }

  test("Save as Snowflake Table") {
    val df = session.table(tableName)
    assert(df.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    // copy tableName to tableName2, default mode
    df.write.saveAsTable(tableName2)

    val df2 = session.table(tableName2)
    assert(df2.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    // append mode
    df.write.mode(SaveMode.Append).saveAsTable(tableName2)
    val df4 = session.table(tableName2)
    assert(df4.collect() sameElements Array[Row](Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)))

    // ignore mode
    df.write.mode(SaveMode.Ignore).saveAsTable(tableName2)
    val df3 = session.table(tableName2)
    assert(df3.collect() sameElements Array[Row](Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)))

    // override mode
    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val df5 = session.table(tableName2)
    assert(df5.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    // test for SPSaveMode.Append when the original table does not exist
    //  need to create the table before insertion
    df.write.mode(SaveMode.Append).saveAsTable(tableName3)
    val df6 = session.table(tableName3)
    assert(df6.collect() sameElements Array[Row](Row(1), Row(2), Row(3)))

    // ErrorIfExists Mode
    assertThrows[SnowflakeSQLException](
      df.write.mode(SaveMode.ErrorIfExists).saveAsTable(tableName2))
  }

  test("Save as Snowflake Table - String Argument") {
    val df = session.table(tableName4)
    checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

    // copy tableName to tableName2, default mode
    df.write.saveAsTable(tableName5)

    val df2 = session.table(tableName5)
    checkAnswer(df2, Seq(Row(1), Row(2), Row(3)))

    // append mode
    df.write.mode("append").saveAsTable(tableName5)
    val df4 = session.table(tableName5)
    checkAnswer(df4, Seq(Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)))

    // ignore mode
    df.write.mode("IGNORE").saveAsTable(tableName5)
    val df3 = session.table(tableName5)
    checkAnswer(df3, Seq(Row(1), Row(2), Row(3), Row(1), Row(2), Row(3)))

    // override mode
    df.write.mode("OvErWrItE").saveAsTable(tableName5)
    val df5 = session.table(tableName5)
    checkAnswer(df5, Seq(Row(1), Row(2), Row(3)))

    // test for SPSaveMode.Append when the original table does not exist
    //  need to create the table before insertion
    df.write.mode("aPpEnD").saveAsTable(tableName6)
    val df6 = session.table(tableName6)
    checkAnswer(df6, Seq(Row(1), Row(2), Row(3)))

    // ErrorIfExists Mode
    assertThrows[SnowflakeSQLException](df.write.mode("erRorIFexists").saveAsTable(tableName5))
  }

  test("multipart identifier") {
    val name1 = tableName
    val name2 = session.getCurrentSchema.get + "." + name1
    val name3 = session.getCurrentDatabase.get + "." + name2

    val expected = Array[Row](Row(1), Row(2), Row(3))
    assert(session.table(name1).collect() sameElements expected)
    assert(session.table(name2).collect() sameElements expected)
    assert(session.table(name3).collect() sameElements expected)

    val name4 = tableName2
    val name5 = session.getCurrentSchema.get + "." + name4
    val name6 = session.getCurrentDatabase.get + "." + name5

    session.table(name1).write.mode(SaveMode.Overwrite).saveAsTable(name4)
    assert(session.table(name4).collect() sameElements expected)

    session.table(name1).write.mode(SaveMode.Overwrite).saveAsTable(name5)
    // name4, name5, name6 are same table
    assert(session.table(name5).collect() sameElements expected)

    session.table(name1).write.mode(SaveMode.Overwrite).saveAsTable(name6)
    assert(session.table(name6).collect() sameElements expected)
  }

  test("write table to different schema") {
    val name1 = tableName
    val name2 = getFullyQualifiedTempSchema + "." + name1

    session.table(name1).write.saveAsTable(name2)

    checkAnswer(session.table(name2), Seq(Row(1), Row(2), Row(3)))

    dropTable(name2)
  }

  test("read from different schema") {
    val tableFromDifferentSchema = getFullyQualifiedTempSchema + "." + tempTableName
    checkAnswer(session.table(tableFromDifferentSchema), Seq(Row("abc")))
  }

  test("quotes, upper and lower case name") {
    checkAnswer(session.table(quotedName), Seq(Row(1), Row(2)))
    // scalastyle:off caselocale
    checkAnswer(session.table(tableName.toLowerCase), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(session.table(tableName.toUpperCase), Seq(Row(1), Row(2), Row(3)))
    // scalastyle:on caselocale
  }

  // Contains 'alter session', which is not supported by owner's right java sp
  test("table with semi structured types", JavaStoredProcExcludeOwner) {
    val df = session.table(semiStructuredTable)
    val types = df.schema.map(_.dataType)
    assert(types.length == 4)
    assert(
      types sameElements Seq(
        ArrayType(StringType),
        MapType(StringType, StringType),
        VariantType,
        GeographyType))
    checkAnswer(
      df,
      Seq(
        Row(
          "[\n  1,\n  2\n]",
          "{\n  \"a\": 1\n}",
          "[\n  1,\n  2\n]",
          Geography.fromGeoJSON(
            "{\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n}")),
        Row(
          "[\n  1,\n  2,\n  3\n]",
          "{\n  \"b\": 2\n}",
          "[\n  1,\n  2,\n  3\n]",
          Geography.fromGeoJSON(
            "{\n  \"coordinates\": [\n    -12,\n    37\n  ],\n  \"type\": \"Point\"\n}"))))
  }

  // Contains 'alter session', which is not supported by owner's right java sp
  test("test row with geography", JavaStoredProcExcludeOwner) {
    val df2 = session.table(semiStructuredTable).select(col("g1"))
    assert(
      df2.collect()(0).getString(0) ==
        "{\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n}")
    assertThrows[ClassCastException](df2.collect()(0).getBinary(0))
    assert(
      getShowString(df2, 10) ==
        """----------------------
          ||"G1"                |
          |----------------------
          ||{                   |
          ||  "coordinates": [  |
          ||    -122.35,        |
          ||    37.55           |
          ||  ],                |
          ||  "type": "Point"   |
          ||}                   |
          ||{                   |
          ||  "coordinates": [  |
          ||    -12,            |
          ||    37              |
          ||  ],                |
          ||  "type": "Point"   |
          ||}                   |
          |----------------------
        |""".stripMargin)

    testWithAlteredSessionParameter(
      {
        assertThrows[SnowparkClientException](df2.collect())
      },
      "GEOGRAPHY_OUTPUT_FORMAT",
      "'WKT'")
  }

  test("table with time type") {
    testWithTimezone() {
      val df2 = session.table(timeTable)
      // Java time has accuracy of 1ms. Snowflake time has accuracy of 1ns. Lost accuracy here.
      checkAnswer(df2, Seq(Row(Time.valueOf("09:15:29")), Row(Time.valueOf("09:15:29"))))
    }
  }

  // getDatabaseFromProperties will read local files, which is not supported in Java SP yet.
  test("Consistent table name behaviors", JavaStoredProcExclude) {
    val tableName = randomName()
    val db = getDatabaseFromProperties
    val sc = getSchemaFromProperties

    val list = new java.util.ArrayList[String](3)
    list.add(db)
    list.add(sc)
    list.add(tableName)

    import session.implicits._
    val df = Seq(1, 2, 3).toDF("a")

    try {
      // saveAsTable
      df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
      // all session.table
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(Seq(db, sc, tableName)), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(Seq(sc, tableName)), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(Seq(tableName)), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(s"$db.$sc.$tableName"), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).saveAsTable(Seq(sc, tableName))
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).saveAsTable(Seq(db, sc, tableName))
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).saveAsTable(s"$db.$sc.$tableName")
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).saveAsTable(list)
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

    } finally {
      dropTable(tableName)
    }
  }

  test("saveAsTable() in Append mode with and without table exists") {
    val df = session.table(tableName)
    dropTable(tableName2)

    // copy tableName to tableName2 with Append mode
    df.write.mode(SaveMode.Append).saveAsTable(tableName2)
    assert(session.table(tableName2).count() == 3)
    df.write.mode(SaveMode.Append).saveAsTable(tableName2)
    assert(session.table(tableName2).count() == 6)
  }

  test("saveAsTable() table types") {
    val df = session.table(tableName)

    val tempTable = randomName()
    val temporaryTable = randomName()
    val transientTable = randomName()

    def getType(tableName: String): String =
      session
        .sql(s"show tables like '$tableName'")
        .select(""""kind"""")
        .collect()
        .head
        .getString(0)
        .toUpperCase(Locale.ROOT)

    try {
      df.write.mode(SaveMode.Overwrite).option("tableType", "temp").saveAsTable(tempTable)
      assert(getType(tempTable).equals("TEMPORARY"))

      df.write.mode(SaveMode.Overwrite).option("tableType", "temporary").saveAsTable(temporaryTable)
      assert(getType(temporaryTable).equals("TEMPORARY"))

      df.write.mode(SaveMode.Overwrite).option("tableType", "transient").saveAsTable(transientTable)
      assert(getType(transientTable).equals("TRANSIENT"))
    } finally {
      dropTable(tempTable)
      dropTable(temporaryTable)
      dropTable(transientTable)
    }
  }
}
