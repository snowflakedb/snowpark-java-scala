package com.snowflake.snowpark_test

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark_java.udaf.JavaUDAF1

class UDAFSuite extends TestData {
  import session.implicits._

  val tableName: String = randomName()
  val tmpStageName: String = randomStageName()

  // Test UDAF: Custom SUM
  class TestSumUDAF extends JavaUDAF1[Double, Double, Double] {
    override def initialize(): Double = 0.0
    override def accumulate(state: Double, value: Double): Double = {
      if (value != null) state + value else state
    }
    override def merge(state1: Double, state2: Double): Double = state1 + state2
    override def finish(state: Double): Double = state
  }

  // Test UDAF: Custom AVG with complex state
  class TestAvgUDAF extends JavaUDAF1[TestAvgUDAF.AvgState, Double, Double] {
    override def initialize(): TestAvgUDAF.AvgState = new TestAvgUDAF.AvgState()
    override def accumulate(state: TestAvgUDAF.AvgState, value: Double): TestAvgUDAF.AvgState = {
      if (value != null) {
        new TestAvgUDAF.AvgState(state.sum + value, state.count + 1)
      } else {
        state
      }
    }
    override def merge(s1: TestAvgUDAF.AvgState, s2: TestAvgUDAF.AvgState): TestAvgUDAF.AvgState = {
      new TestAvgUDAF.AvgState(s1.sum + s2.sum, s1.count + s2.count)
    }
    override def finish(state: TestAvgUDAF.AvgState): Double = {
      if (state.count > 0) state.sum / state.count else null.asInstanceOf[Double]
    }
  }

  object TestAvgUDAF {
    class AvgState(val sum: Double = 0.0, val count: Long = 0) extends java.io.Serializable
  }

  override def beforeAll: Unit = {
    super.beforeAll()
    createTable(tableName, "category varchar, amount double")
    runQuery(
      s"""insert into $tableName values
         |('Electronics', 100.0),
         |('Electronics', 200.0),
         |('Electronics', 150.0),
         |('Clothing', 50.0),
         |('Clothing', 75.0),
         |('Clothing', null)
         |""".stripMargin,
      session)
    createStage(tmpStageName, isTemporary = true)
  }

  override def afterAll: Unit = {
    dropTable(tableName)
    dropStage(tmpStageName)
    super.afterAll()
  }

  test("UDAF - register temporary anonymous UDAF") {
    val df = session.table(tableName)
    // Note: registerTemporary returns a Column representing the UDAF
    // For anonymous UDAFs, you need to use the returned column directly
    // or register with a name and use callUDAF
    val udafName = randomFunctionName()
    session.udaf.registerTemporary(udafName, new TestSumUDAF())

    val result = df
      .groupBy("category")
      .agg(callUDAF(udafName, col("amount")).as("total"))
      .collect()

    assert(result.length == 2)
    println(s"Anonymous UDAF Result: ${result.mkString(", ")}")
  }

  test("UDAF - register temporary named UDAF") {
    val df = session.table(tableName)
    session.udaf.registerTemporary("my_avg", new TestAvgUDAF())

    val result = df
      .groupBy("category")
      .agg(callUDAF("my_avg", col("amount")).as("avg_amount"))
      .collect()

    assert(result.length == 2)
    // Electronics: (100 + 200 + 150) / 3 = 150.0
    // Clothing: (50 + 75) / 2 = 62.5
    println(s"Named UDAF Result: ${result.mkString(", ")}")
  }

  test("UDAF - register permanent UDAF") {
    val udafName = randomFunctionName()
    session.udaf.registerPermanent(udafName, new TestSumUDAF(), tmpStageName)

    val df = session.table(tableName)
    val result = df
      .groupBy("category")
      .agg(callUDAF(udafName, col("amount")).as("total"))
      .collect()

    assert(result.length == 2)
    println(s"Permanent UDAF Result: ${result.mkString(", ")}")

    // Cleanup
    session.sql(s"DROP FUNCTION IF EXISTS $udafName(DOUBLE)")
  }

  test("UDAF - multiple aggregations") {
    val df = session.table(tableName)
    session.udaf.registerTemporary("my_sum", new TestSumUDAF())
    session.udaf.registerTemporary("my_avg2", new TestAvgUDAF())

    val result = df
      .groupBy("category")
      .agg(
        callUDAF("my_sum", col("amount")).as("total"),
        callUDAF("my_avg2", col("amount")).as("average"))
      .collect()

    assert(result.length == 2)
    println(s"Multiple Aggregations: ${result.mkString(", ")}")
  }

  test("UDAF - aggregate without groupBy") {
    val df = session.table(tableName)
    val udafName = randomFunctionName()
    session.udaf.registerTemporary(udafName, new TestSumUDAF())

    val result = df.agg(callUDAF(udafName, col("amount")).as("grand_total")).collect()

    assert(result.length == 1)
    // Total: 100 + 200 + 150 + 50 + 75 = 575.0
    println(s"Grand Total: ${result.head.getDouble(0)}")
  }
}
