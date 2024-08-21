package com.snowflake.snowpark_test

import com.snowflake.snowpark.internal.analyzer.quoteName
import com.snowflake.snowpark.types.LongType
import com.snowflake.snowpark.{JavaStoredProcExclude, Row, TestData, functions}

class ViewSuite extends TestData {

  lazy private val viewName1: String = randomName()
  lazy private val viewName2: String = s""""test @#%^${randomName()}""""

  override def afterAll: Unit = {
    dropView(viewName1)
    dropView(viewName2)
    super.afterAll
  }

  test("create view") {
    integer1.createOrReplaceView(viewName1)
    checkAnswer(session.sql(s"select * from $viewName1"), Seq(Row(1), Row(2), Row(3)), sort = false)

    // test replace
    double1.createOrReplaceView(viewName1)
    checkAnswer(
      session.sql(s"select * from $viewName1"),
      Seq(Row(1.111), Row(2.222), Row(3.333)),
      sort = false)
  }

  test("view name with special character") {
    columnNameHasSpecialCharacter.createOrReplaceView(viewName2)
    checkAnswer(
      session.sql(s"select * from ${quoteName(viewName2)}"),
      Seq(Row(1, 2), Row(3, 4)),
      sort = false)
  }

  test("only works on select") {
    assertThrows[IllegalArgumentException](
      session.sql("show tables").createOrReplaceView(viewName1))
  }

  // getDatabaseFromProperties will read local files, which is not supported in Java SP yet.
  test("Consistent view name behaviors", JavaStoredProcExclude) {
    val viewName = randomName()
    val db = getDatabaseFromProperties
    val sc = getSchemaFromProperties

    val list = new java.util.ArrayList[String](3)
    list.add(db)
    list.add(sc)
    list.add(viewName)

    import session.implicits._
    val df = Seq(1, 2, 3).toDF("a")

    try {
      // create view
      df.createOrReplaceView(viewName)
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceView(Seq(db, sc, viewName))
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceView(Seq(sc, viewName))
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceView(Seq(viewName))
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceView(s"$db.$sc.$viewName")
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceView(list)
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      // create temp view
      df.createOrReplaceTempView(viewName)
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceTempView(Seq(db, sc, viewName))
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceTempView(Seq(sc, viewName))
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceTempView(Seq(viewName))
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceTempView(s"$db.$sc.$viewName")
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

      df.createOrReplaceTempView(list)
      checkAnswer(session.table(viewName), Seq(Row(1), Row(2), Row(3)))
      dropView(viewName)

    } finally {
      dropView(viewName)
    }
  }

  test("create temp view on functions") {
    import functions._
    val tableName = randomName()
    val viewName = randomViewName()
    try {
      createTable(tableName, "id int, val int")
      val t = session.table(tableName)
      val a = t.groupBy(col("id")).agg(sqlExpr("max(val)"))
      a.createOrReplaceTempView(viewName) // should not report error
      val schema = session.table(viewName).schema
      assert(schema.size == 2)
      assert(schema("ID").dataType == LongType)
      assert(schema("\"MAX(VAL)\"").dataType == LongType)

      val a2 = t.groupBy(col("id")).agg(sum(col("val")))
      a2.createOrReplaceTempView(viewName)
      val schema1 = session.table(viewName).schema
      assert(schema1.size == 2)
      assert(schema1("ID").dataType == LongType)
      assert(schema1("\"SUM(VAL)\"").dataType == LongType)

      val a3 = t.groupBy(col("id")).agg(sum(col("val")) + 1)
      a3.createOrReplaceTempView(viewName)
      val schema2 = session.table(viewName).schema
      assert(schema2.size == 2)
      assert(schema2("ID").dataType == LongType)
      assert(schema2("\"ADD(SUM(VAL), LITERAL())\"").dataType == LongType)

    } finally {
      dropTable(tableName)
      dropView(viewName)
    }
  }
}
