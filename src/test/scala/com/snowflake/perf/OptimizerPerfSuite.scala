package com.snowflake.perf

import com.snowflake.snowpark.{DataFrame, PerfTest, TestUtils}
import com.snowflake.snowpark.functions._

@PerfTest
class OptimizerPerfSuite extends PerfBase {
  import session.implicits._

  lazy val table150: String = TestUtils.randomTableName()

  lazy val table250: String = TestUtils.randomTableName()

  override def beforeAll: Unit = {
    super.beforeAll

    // init table150
    val table150Size = 150
    val columns = (0 to table150Size).map(index => s"col$index int").mkString(",")
    session.sql(s"create or replace temp table $table150($columns)").collect()
    val values = (0 to table150Size).mkString(",")
    session.sql(s"insert into $table150 values($values)").collect()

    val table250Size = 250
    val columns1 = (0 to table250Size).map(index => s"col$index int").mkString(",")
    session.sql(s"create or replace temp table $table250($columns1)").collect()
    val values1 = (0 to table250Size).mkString(",")
    session.sql(s"insert into $table250 values($values1)").collect()
  }

  override def afterAll: Unit = {
    dropTable(table150)
    dropTable(table250)
    super.afterAll
  }

  perfTest("schema query") {
    var df = Seq((1, 2), (3, 4)).toDF("\"col %\"", "\"col *\"")
    (1 to 400).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
    (1 to 400).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
    (1 to 400).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
    (1 to 400).foreach(x => {
      df = df.select(col("*")).filter(col("\"col %\"") > 1)
    })
    df.schema
  }

  perfTest("withColumn 150 * 200") {
    var df1: DataFrame = session.table(table150)
    (0 to 200).foreach(index => {
      df1 = df1.withColumn(s"n$index", lit(-1))
    })
    df1.collect()
  }

  perfTest("dropColumn 250 * 200") {
    var df1: DataFrame = session.table(table250)
    (0 to 200).foreach(index => {
      df1 = df1.drop(s"col$index")
    })
    df1.collect()
  }

  perfTest("union 150 * 100") {
    val df = session.table(table150)
    var df1: DataFrame = df
    (0 to 100).foreach(index => {
      df1 = df1.unionAll(df)
    })
    df1.collect()
  }

  perfTest("filter 150 * 200") {
    val df = session.table(table150)
    var df1: DataFrame = df
    (0 to 100).foreach(x => {
      df1 = df1.where(df("col1") > 100)
    })
    df1.collect()
  }
}
