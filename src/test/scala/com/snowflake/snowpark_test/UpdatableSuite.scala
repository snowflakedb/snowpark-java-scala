package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark._

class UpdatableSuite extends TestData {

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

  test("update rows in table") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val updatable = session.table(tableName)

    assert(updatable.update(Map(col("b") -> lit(0)), col("a") === 1) == UpdateResult(2, 0))
    checkAnswer(updatable, Seq(Row(1, 0), Row(1, 0), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(updatable.update(Map("b" -> lit(0)), col("a") === 1) == UpdateResult(2, 0))
    checkAnswer(updatable, Seq(Row(1, 0), Row(1, 0), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(updatable.update(Map(col("a") -> lit(1), col("b") -> lit(0))) == UpdateResult(6, 0))
    checkAnswer(updatable, Seq(Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(updatable.update(Map("b" -> (col("a") + col("b")))) == UpdateResult(6, 0))
    checkAnswer(updatable, Seq(Row(1, 2), Row(1, 3), Row(2, 3), Row(2, 4), Row(3, 4), Row(3, 5)))
  }

  test("delete rows from table") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val updatable = session.table(tableName)

    assert(updatable.delete(col("a") === 1 && col("b") === 2) == DeleteResult(1))
    checkAnswer(updatable, Seq(Row(1, 1), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    checkAnswer(updatable, Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    assert(updatable.delete() == DeleteResult(6))
    checkAnswer(updatable, Seq())
  }

  test("update with join") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val t1 = session.table(tableName)
    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val t2 = session.table(tableName2)
    assert(t2.update(Map(col("n") -> lit(0)), t1("a") === t2("n"), t1) == UpdateResult(3, 3))
    checkAnswer(
      t2,
      Seq(Row(0, "A"), Row(0, "B"), Row(0, "C"), Row(4, "D"), Row(5, "E"), Row(6, "F")))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    assert(t2.update(Map("n" -> lit(0)), t1("a") === t2("n"), t1) == UpdateResult(3, 3))
    checkAnswer(
      t2,
      Seq(Row(0, "A"), Row(0, "B"), Row(0, "C"), Row(4, "D"), Row(5, "E"), Row(6, "F")))

    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    import session.implicits._
    val sd = Seq("A", "B", "D", "E").toDF("c")
    assert(t2.update(Map("n" -> lit(0)), t2("L") === sd("c"), sd) == UpdateResult(4, 0))
    checkAnswer(
      t2,
      Seq(Row(0, "A"), Row(0, "B"), Row(0, "D"), Row(0, "E"), Row(3, "C"), Row(6, "F")))
  }

  test("update with join involving ambiguous columns") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val t1 = session.table(tableName)
    testData3.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val t2 = session.table(tableName2)
    assert(t1.update(Map(col("a") -> lit(0)), t1("a") === t2("a"), t2) == UpdateResult(4, 0))
    checkAnswer(t1, Seq(Row(0, 1), Row(0, 2), Row(0, 1), Row(0, 2), Row(3, 1), Row(3, 2)))

    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName3)
    val up = session.table(tableName3)
    import session.implicits._
    val sd = Seq("A", "B", "D", "E").toDF("L")
    assert(up.update(Map("n" -> lit(0)), up("L") === sd("L"), sd) == UpdateResult(4, 0))
    checkAnswer(
      up,
      Seq(Row(0, "A"), Row(0, "B"), Row(0, "D"), Row(0, "E"), Row(3, "C"), Row(6, "F")))
  }

  test("update with join with aggregated source data") {
    import session.implicits._
    val tmp = Seq((0, 10)).toDF("k", "v")
    tmp.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val src = Seq((0, 11), (0, 12), (0, 13)).toDF("k", "v")

    val target = session.table(tableName)
    val b = src.groupBy(col("k")).agg(min(col("v")).as("v"))
    assert(
      target.update(Map(target("v") -> b("v")), target("k") === b("k"), b)
        == UpdateResult(1, 0))
    checkAnswer(target, Seq(Row(0, 11)))
  }

  test("delete with join") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val t1 = session.table(tableName)
    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val t2 = session.table(tableName2)
    assert(t2.delete(t1("a") === t2("n"), t1) == DeleteResult(3))
    checkAnswer(t2, Seq(Row(4, "D"), Row(5, "E"), Row(6, "F")))

    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    import session.implicits._
    val sd = Seq("A", "B", "D", "E").toDF("c")
    assert(t2.delete(t2("L") === sd("c"), sd) == DeleteResult(4))
    checkAnswer(t2, Seq(Row(3, "C"), Row(6, "F")))
  }

  test("delete with join involving ambiguous columns") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val t1 = session.table(tableName)
    testData3.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val t2 = session.table(tableName2)
    assert(t1.delete(t1("a") === t2("a"), t2) == DeleteResult(4))
    checkAnswer(t1, Seq(Row(3, 1), Row(3, 2)))

    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName3)
    val up = session.table(tableName3)
    import session.implicits._
    val sd = Seq("A", "B", "D", "E").toDF("L")
    assert(up.delete(up("L") === sd("L"), sd) == DeleteResult(4))
    checkAnswer(up, Seq(Row(3, "C"), Row(6, "F")))
  }

  test("delete with join with aggregated source data") {
    import session.implicits._
    val tmp = Seq((0, 1), (0, 2), (0, 3)).toDF("k", "v")
    tmp.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val src = Seq((0, 1), (0, 2), (0, 3)).toDF("k", "v")

    val target = session.table(tableName)
    val b = src.groupBy(col("k")).agg(mean(col("v")).as("v"))
    assert(target.delete(target("v") === b("v"), b) == DeleteResult(1))
    checkAnswer(target, Seq(Row(0, 1), Row(0, 3)))
  }

  test("merge with update clause only") {
    import session.implicits._
    val targetDF = Seq((10, "old"), (10, "too_old"), (11, "old")).toDF("id", "desc")
    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val target = session.table(tableName)
    val source = Seq((10, "new")).toDF("id", "desc")

    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenMatched
        .update(Map(target("desc") -> source("desc")))
        .collect() == MergeResult(0, 2, 0))
    checkAnswer(target, Seq(Row(10, "new"), Row(10, "new"), Row(11, "old")))

    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenMatched
        .update(Map("desc" -> source("desc")))
        .collect() == MergeResult(0, 2, 0))
    checkAnswer(target, Seq(Row(10, "new"), Row(10, "new"), Row(11, "old")))

    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenMatched(target("desc") === lit("old"))
        .update(Map(target("desc") -> source("desc")))
        .collect() == MergeResult(0, 1, 0))
    checkAnswer(target, Seq(Row(10, "new"), Row(10, "too_old"), Row(11, "old")))
  }

  test("merge with delete clause only") {
    import session.implicits._
    val targetDF = Seq((10, "old"), (10, "too_old"), (11, "old")).toDF("id", "desc")
    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val target = session.table(tableName)
    val source = Seq((10, "new")).toDF("id", "desc")

    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenMatched
        .delete()
        .collect() == MergeResult(0, 0, 2))
    checkAnswer(target, Seq(Row(11, "old")))

    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenMatched(target("desc") === lit("old"))
        .delete()
        .collect() == MergeResult(0, 0, 1))
    checkAnswer(target, Seq(Row(10, "too_old"), Row(11, "old")))
  }

  test("merge with insert clause only") {
    import session.implicits._
    val targetDF = Seq((10, "old"), (11, "new")).toDF("id", "desc")
    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val target = session.table(tableName)
    val source = Seq((12, "old"), (12, "new")).toDF("id", "desc")

    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenNotMatched
        .insert(Map(target("id") -> source("id"), target("desc") -> source("desc")))
        .collect() == MergeResult(2, 0, 0))
    checkAnswer(target, Seq(Row(10, "old"), Row(11, "new"), Row(12, "new"), Row(12, "old")))

    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenNotMatched
        .insert(Seq(source("id"), source("desc")))
        .collect() == MergeResult(2, 0, 0))
    checkAnswer(target, Seq(Row(10, "old"), Row(11, "new"), Row(12, "new"), Row(12, "old")))

    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenNotMatched
        .insert(Map("id" -> source("id"), "desc" -> source("desc")))
        .collect() == MergeResult(2, 0, 0))
    checkAnswer(target, Seq(Row(10, "old"), Row(11, "new"), Row(12, "new"), Row(12, "old")))

    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenNotMatched(source("desc") === lit("new"))
        .insert(Map(target("id") -> source("id"), target("desc") -> source("desc")))
        .collect() == MergeResult(1, 0, 0))
    checkAnswer(target, Seq(Row(10, "old"), Row(11, "new"), Row(12, "new")))
  }

  test("merge with matched and not matched clauses") {
    import session.implicits._
    val targetDF = Seq((10, "old"), (10, "too_old"), (11, "old")).toDF("id", "desc")
    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val target = session.table(tableName)
    val source = Seq((10, "new"), (12, "new"), (13, "old")).toDF("id", "desc")

    assert(
      target
        .merge(source, target("id") === source("id"))
        .whenMatched(target("desc") === lit("too_old"))
        .delete()
        .whenMatched
        .update(Map(target("desc") -> source("desc")))
        .whenNotMatched(source("desc") === lit("old"))
        .insert(Map(target("id") -> source("id"), target("desc") -> lit("new")))
        .whenNotMatched
        .insert(Map(target("id") -> source("id"), target("desc") -> source("desc")))
        .collect() == MergeResult(2, 1, 1))
    checkAnswer(target, Seq(Row(10, "new"), Row(11, "old"), Row(12, "new"), Row(13, "new")))
  }

  test("merge with aggregated source") {
    import session.implicits._
    val targetDF = Seq((0, 10)).toDF("k", "v")
    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val target = session.table(tableName)
    val sourceDF = Seq((0, 10), (0, 11), (0, 12)).toDF("k", "v")
    val source = sourceDF.groupBy(col("k")).agg(max(col("v")).as("v"))

    assert(
      target
        .merge(source, target("k") === source("k"))
        .whenMatched
        .update(Map(target("v") -> source("v")))
        .whenNotMatched
        .insert(Map(target("k") -> source("k"), target("v") -> source("v")))
        .collect() == MergeResult(0, 1, 0))
    checkAnswer(target, Seq(Row(0, 12)))
  }

  test("merge with multiple clause conditions") {
    import session.implicits._
    val targetDF = Seq((0, 10), (1, 11), (2, 12), (3, 13)).toDF("k", "v")
    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val target = session.table(tableName)
    val source = Seq((0, 20), (1, 21), (2, 22), (3, 23), (4, 24), (5, 25), (6, 26), (7, 27))
      .toDF("k", "v")

    assert(
      target
        .merge(source, target("k") === source("k"))
        .whenMatched(source("v") < lit(21))
        .delete()
        .whenMatched(source("v") > lit(22))
        .update(Map("v" -> (source("v") - lit(20))))
        .whenMatched(source("v") =!= lit(21))
        .delete()
        .whenMatched
        .update(Map(target("v") -> source("v")))
        .whenNotMatched(source("v") < lit(25))
        .insert(Seq(source("k"), source("v") - lit(20)))
        .whenNotMatched(source("v") > lit(26))
        .insert(Map("k" -> source("k")))
        .whenNotMatched(source("v") =!= lit(25))
        .insert(Map(target("v") -> source("v")))
        .whenNotMatched
        .insert(Map("k" -> source("k"), "v" -> source("v")))
        .collect() == MergeResult(4, 2, 2))
    checkAnswer(
      target,
      Seq(Row(1, 21), Row(3, 3), Row(4, 4), Row(5, 25), Row(7, null), Row(null, 26)))
  }

  test("clone") {
    import session.implicits._
    val df = Seq(1, 2).toDF("a")
    val tableName = randomName()
    try {
      df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
      val updatable = session.table(tableName)
      assert(updatable.isInstanceOf[Updatable])
      val cloned = updatable.clone
      assert(cloned.isInstanceOf[Updatable])

      cloned.delete(col("a") === 1).rowsDeleted
      checkAnswer(session.table(tableName), Seq(Row(2)))
    } finally {
      dropTable(tableName)
    }
  }
}
