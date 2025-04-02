package com.snowflake.snowpark
import com.snowflake.snowpark.functions._

class SimplifierSuite extends TestData {
  import session.implicits._

  test("union + union") {
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((2, 2)).toDF("a", "b")
    val df3 = Seq((3, 2)).toDF("a", "b")
    val df4 = Seq((4, 2)).toDF("a", "b")

    val result1 = df1.union(df2).union(df3.union(df4))
    checkAnswer(result1, Seq(Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)))
    val query1 = result1.snowflakePlan.queries.last.sql
    assert(query1.split("UNION \\( SELECT").length == 4)

    val result2 = df1.union(df2).union(df3)
    checkAnswer(result2, Seq(Row(1, 2), Row(2, 2), Row(3, 2)))
    val query2 = result2.snowflakePlan.queries.last.sql
    assert(query2.split("UNION \\( SELECT").length == 3)

    // mix union and union all

    val result3 = df1.union(df2).union(df3.unionAll(df4))
    checkAnswer(result3, Seq(Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)))
    val query3 = result3.snowflakePlan.queries.last.sql
    assert(query3.split("UNION \\( SELECT").length == 2)
    assert(query3.split("UNION \\(\\( SELECT").length == 2)
    assert(query3.split("UNION ALL \\( SELECT").length == 2)
  }

  test("union all + union all") {
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((2, 2)).toDF("a", "b")
    val df3 = Seq((3, 2)).toDF("a", "b")
    val df4 = Seq((4, 2)).toDF("a", "b")

    val result1 = df1.unionAll(df2).unionAll(df3.unionAll(df4))
    checkAnswer(result1, Seq(Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)), sort = false)
    val query1 = result1.snowflakePlan.queries.last.sql
    assert(query1.split("UNION ALL \\( SELECT").length == 4)

    val result2 = df1.unionAll(df2).unionAll(df3)
    checkAnswer(result2, Seq(Row(1, 2), Row(2, 2), Row(3, 2)), sort = false)
    val query2 = result2.snowflakePlan.queries.last.sql
    assert(query2.split("UNION ALL \\( SELECT").length == 3)

    // mix union and union all

    val result3 = df1.unionAll(df2).unionAll(df3.union(df4))
    checkAnswer(result3, Seq(Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)), sort = false)
    val query3 = result3.snowflakePlan.queries.last.sql
    assert(query3.split("UNION ALL \\( SELECT").length == 2)
    assert(query3.split("UNION ALL \\(\\( SELECT").length == 2)
    assert(query3.split("UNION \\( SELECT").length == 2)
  }

  test("Filter + Filter") {
    val df = Seq((2, 10, 100), (2, 11, 100), (0, 21, 2), (6, 10, 100), (4, 1, 100), (3, 3, 5))
      .toDF("a", "b", "c")
    val result = df
      .filter(df("a") < 5)
      .filter(df("a") > 1)
      .filter(df("b") =!= 10)
      .filter(df("c") === 100)

    checkAnswer(result, Seq(Row(2, 11, 100), Row(4, 1, 100)), sort = false)
    val query = result.snowflakePlan.queries.last.sql
    assert(query.split("WHERE").length == 2) // contains only one WHERE
    assert(
      query.contains(
        "WHERE ((\"A\" < 5 :: int) AND " +
          "((\"A\" > 1 :: int) AND ((\"B\" <> 10 :: int) AND " +
          "(\"C\" = 100 :: int))))"))

    val result1 = df
      .filter((df("a") === 2) or (df("a") === 0))
      .filter((df("b") === 10) or (df("b") === 20))
    result1.show()
    checkAnswer(result1, Seq(Row(2, 10, 100)))
    val query1 = result1.snowflakePlan.queries.last.sql
    assert(
      query1.contains("WHERE (((\"A\" = 2 :: int) OR (\"A\" = 0 :: int))" +
        " AND ((\"B\" = 10 :: int) OR (\"B\" = 20 :: int)))"))
    assert(query1.split("WHERE").length == 2)
  }

  test("Filter + Filter on Join") {
    val df1 = Seq(1, 2, 3).toDF("a")
    val df2 = Seq(3, 4, 5).toDF("b")
    val result = df1
      .filter(df1("a") < 3)
      .filter(df1("a") > 1)
      .join(df2.filter(df2("b") > 3).filter(df2("b") < 5))
      .filter(df1("a") === 2)
      .filter(df2("b") === 4)
    checkAnswer(result, Seq(Row(2, 4)))
    val query = result.snowflakePlan.queries.last.sql
    assert(query.split("WHERE").length == 4)
    assert(query.contains("WHERE ((\"A\" < 3 :: int) AND (\"A\" > 1 :: int))"))
    assert(query.contains("WHERE ((\"B\" > 3 :: int) AND (\"B\" < 5 :: int))"))
    assert(query.contains("WHERE ((\"A\" = 2 :: int) AND (\"B\" = 4 :: int))"))
  }

  test("Sort + Limit") {
    val df = Seq(5, 1, 2, 6, 7, 8, 3).toDF("a")
    val result = df.sort(df("a")).limit(3)
    checkAnswer(result, Seq(Row(1), Row(2), Row(3)), sort = false)
    val query = result.snowflakePlan.queries.last.sql
    assert(query.contains("ORDER BY \"A\" ASC NULLS FIRST LIMIT 3"))
  }

  test("withColumns") {
    val df = session.sql("SELECT 1 AS a")
    var newDf = df
    (0 until 10).foreach(x => newDf = newDf.withColumn(s"c$x", lit(x)))
    val query1 = newDf.snowflakePlan.queries.last

    // should only contain two "SELECT"
    assert(query1.countString("SELECT") == 2)

    (0 until 10).foreach(x => newDf = newDf.withColumn(s"c$x", lit(x + 10)))

    val query2 = newDf.snowflakePlan.queries.last
    // invoke a new project if replacing existing columns
    assert(query2.countString("SELECT") == 11)
    assert(newDf.output.length == 11)

    (0 until 10).foreach(x => newDf = newDf.withColumn(s"d$x", lit(x + 20)))

    val query3 = newDf.snowflakePlan.queries.last
    // no replacing
    assert(query3.countString("SELECT") == 12)
    assert(newDf.output.length == 21)

    checkAnswer(
      newDf,
      Seq(Row(1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)))
  }

  test("withColumns 2") {
    val df = session.sql("select 0 as c0, 1 as c1, 2 as c2")
    checkAnswer(df.withColumn("c1", lit(3)).withColumn("c2", lit(4)), Seq(Row(0, 3, 4)))
  }

  test("withColumn - replace column") {
    import session.implicits._
    val df = Seq((1, 2)).toDF("a", "b")
    val result = df.withColumn("a", functions.col("a") + 10)
    result.schema.printTreeString()
    checkAnswer(result, Seq(Row(2, 11)))
  }

  test("withColumn - invoke replaced column") {
    val df = session.sql("select 1 as a")
    checkAnswer(df.withColumn("b", col("a") + 1), Seq(Row(1, 2)))
    checkAnswer(df.withColumn("a", lit(10)).withColumn("b", col("a") + 1), Seq(Row(10, 11)))
  }

  test("withColumn - invoke existing column") {
    val df = session.sql("SELECT 1 as a")
    val query1 = df.withColumn("b", df("a") + 1).snowflakePlan.queries.last
    assert(query1.countString("SELECT") == 3)

    val query2 = df
      .withColumn("b", lit(2))
      .withColumn("c", col("b").as("c"))
      .snowflakePlan
      .queries
      .last
    assert(query2.countString("SELECT") == 3)

    // without invoke
    val query3 = df
      .withColumn("b", lit(2))
      .withColumn("c", lit(3))
      .snowflakePlan
      .queries
      .last
    assert(query3.countString("SELECT") == 2)

    // can't analyze
    val query4 = df
      .withColumn("b", lit(2))
      .withColumn("c", sqlExpr("1"))
      .snowflakePlan
      .queries
      .last
    assert(query4.countString("SELECT") == 3)
  }

  test("withColumn - invoke and replace") {
    val df = session.sql("SELECT 1 as a")
    val df1 = df
      .withColumn("b", lit(2))
    val df2 = df1
      .withColumn("b", df1("b") + 1)
      .withColumn("c", lit(5))
      .withColumn("d", lit(6))
    checkAnswer(df2, Seq(Row(1, 3, 5, 6)))
    val query = df2.snowflakePlan.queries.last
    assert(query.countString("SELECT") == 4)
  }

  test("Project + Filter") {
    val df = Seq((1, 2, 3), (3, 4, 5), (0, 1, 2)).toDF("a", "b", "c")
    val result = df.select(df("a")).filter(df("a") > 2)
    checkAnswer(result, Seq(Row(3)))
    val query = result.snowflakePlan.queries.last
    assert(query.countString("SELECT") == 4)

    val result1 =
      df.select(df("a"), df("b")).where(df("a") < 4).select(df("b")).filter(df("b") > 3)
    checkAnswer(result1, Seq(Row(4)))
    val query1 = result1.snowflakePlan.queries.last
    assert(query1.countString("SELECT") == 5)
  }

  test("ProjectAndFilter + Filter") {
    val df = Seq((1, 2, 3), (3, 4, 5), (0, 1, 2)).toDF("a", "b", "c")
    val result = df.select(df("a"), df("b")).where(df("a") > 0).filter(df("b") < 4)
    checkAnswer(result, Seq(Row(1, 2)))
    val query = result.snowflakePlan.queries.last
    assert(query.countString("SELECT") == 4)
    assert(query.countString("WHERE ((\"A\" > 0 :: int) AND (\"B\" < 4 :: int))") == 1)
  }

  test("Project + Filter - can't analyze") {
    val df = Seq((1, 2, 3), (3, 4, 5), (0, 1, 2)).toDF("a", "b", "c")
    // select unresolved attribute
    val result = df.select("a").filter(df("a") > 2)
    checkAnswer(result, Seq(Row(3)))
    val query = result.snowflakePlan.queries.last
    assert(query.countString("SELECT") == 5)
  }

  test("Project + Filter - new columns") {
    val df = Seq((1, 2, 3), (3, 4, 5), (0, 1, 2)).toDF("a", "b", "c")
    // select unresolved attribute
    val result = df.select(df("a"), lit(-1).as("b")).filter(df("b") < 0)
    checkAnswer(result, Seq(Row(1, -1), Row(3, -1), Row(0, -1)), sort = false)
    val query = result.snowflakePlan.queries.last
    assert(query.countString("SELECT") == 5)

    val result1 = df.select(df("a"), lit(-1).as("B")).filter(df("b") < 0)
    checkAnswer(result1, Seq(Row(1, -1), Row(3, -1), Row(0, -1)), sort = false)
    val query1 = result1.snowflakePlan.queries.last
    assert(query1.countString("SELECT") == 5)

    val result2 = df.select(df("a"), lit(-1).as("b")).filter(df("B") < 0)
    checkAnswer(result2, Seq(Row(1, -1), Row(3, -1), Row(0, -1)), sort = false)
    val query2 = result2.snowflakePlan.queries.last
    assert(query2.countString("SELECT") == 5)

    val result3 = df.select(df("a"), df("b")).filter(col("b") < 2)
    checkAnswer(result3, Seq(Row(0, 1)), sort = false)
    val query3 = result3.snowflakePlan.queries.last
    assert(query3.countString("SELECT") == 5)

    val result4 = df.select(df("a"), col("b")).filter(df("b") < 2)
    checkAnswer(result4, Seq(Row(0, 1)), sort = false)
    val query4 = result4.snowflakePlan.queries.last
    assert(query4.countString("SELECT") == 5)
  }

  test("dropColumns") {
    val df = Seq((1, 2, 3, 4)).toDF("a", "b", "c", "d")
    val result = df.drop("a").drop("B").drop("\"C\"")
    checkAnswer(result, Seq(Row(4)))
    val query = result.snowflakePlan.queries.last
    assert(query.countString("SELECT") == 4)

    val result1 = df.drop("a").drop("B").drop("\"c\"") // "c" doesn't exist
    checkAnswer(result1, Seq(Row(3, 4)))
    val query1 = result1.snowflakePlan.queries.last
    assert(query1.countString("SELECT") == 4)

    // drop all
    val result2 = df.drop("a").drop("B").drop(col("c"), df("d"))
    assertThrows[SnowparkClientException](result2.collect())

    // drop renamed column
    val df1 = df.select(df("a"), df("b").as("newCol"))
    checkAnswer(df1, Seq(Row(1, 2)))
    val result3 = df1.drop(df("b"))
    checkAnswer(result3, Seq(Row(1)))
  }
}
