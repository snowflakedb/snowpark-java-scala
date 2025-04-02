package com.snowflake.snowpark_test

import com.snowflake.snowpark.types._
import com.snowflake.snowpark._

trait ComplexDataFrameSuite extends SNTestBase {
  import session.implicits._
  val tableName: String = randomName()
  val tmpStageName: String = randomStageName()
  private val userSchema: StructType = StructType(
    Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", DoubleType)))

  override def beforeAll: Unit = {
    super.beforeAll()
    createTable(tableName, "num int")
    runQuery(s"insert into $tableName values(1),(2),(3)", session)
    runQuery(s"CREATE TEMPORARY STAGE $tmpStageName", session)
    uploadFileToStage(tmpStageName, testFileCsv, compress = false)
  }

  override def afterAll: Unit = {
    runQuery(s"DROP STAGE IF EXISTS $tmpStageName", session)
    dropTable(tableName)
    super.afterAll()
  }

  test("Combination of multiple operators") {
    val df1 = Seq(1, 2).toDF("a")
    val df2 = Seq(1, 2).map(i => (i, s"test$i")).toDF("a", "b")

    checkAnswer(df1.join(df2, "a").except(df2), Seq.empty)

    checkAnswer(df1.join(df2, "a").intersect(df2), Seq(Row(1, "test1"), Row(2, "test2")))

    checkAnswer(df1.join(df2, "a"), df2.filter($"a" < 2).union(df2.filter($"a" >= 2)))

    checkAnswer(
      df1.join(df2, "a").union(df2.filter($"a" < 2).union(df2.filter($"a" >= 2))),
      Seq(Row(1, "test1"), Row(2, "test2")))

    checkAnswer(
      df1.join(df2, "a").unionAll(df2.filter($"a" < 2).unionAll(df2.filter($"a" >= 2))),
      Seq(Row(1, "test1"), Row(1, "test1"), Row(2, "test2"), Row(2, "test2")))
  }

  test("Combination of multiple operators with filters") {
    val df1 = (1 to 10).toDF("a")
    val df2 = (1 to 10).map(i => (i, s"test$i")).toDF("a", "b")

    checkAnswer(df1.filter($"a" < 6).join(df2, "a").intersect(df2.filter($"a" > 5)), Seq.empty)
    checkAnswer(df1.filter($"a" < 6).join(df2, Seq("a"), "left_semi"), df1.filter($"a" < 6))
    checkAnswer(df1.filter($"a" < 6).join(df2, Seq("a"), "left_anti"), Seq.empty)

    checkAnswer(
      df1.filter($"a" < 6).join(df2, "a").union(df2.filter($"a" > 5)),
      (1 to 10).map(i => Row(i, s"test$i")))
  }

  test("join on top of unions") {
    val df1 = (1 to 5).toDF("a")
    val df2 = (6 to 10).toDF("a")
    val df3 = (1 to 5).map(i => (i, s"test$i")).toDF("a", "b")
    val df4 = (6 to 10).map(i => (i, s"test$i")).toDF("a", "b")
    checkAnswer(
      df1.union(df2).join(df3.union(df4), "a").sort(functions.col("a")),
      (1 to 10).map(i => Row(i, s"test$i")))
  }

  test("Combination of multiple data sources") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val df1 = session.read.schema(userSchema).csv(testFileOnStage)
    val df2 = session.table(tableName)
    checkAnswer(
      df2.join(df1, df1("a") === df2("num")),
      Seq(Row(1, 1, "one", 1.2), Row(2, 2, "two", 2.2)))

    checkAnswer(
      df2.filter($"num" === 1).join(df1.select("a", "b"), df1("a") === df2("num")),
      Seq(Row(1, 1, "one")))
  }
}

class EagerComplexDataFrameSuite extends ComplexDataFrameSuite with EagerSession

class LazyComplexDataFrameSuite extends ComplexDataFrameSuite with LazySession
