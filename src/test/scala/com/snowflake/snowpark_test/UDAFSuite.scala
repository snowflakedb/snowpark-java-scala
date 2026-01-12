package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.TestData
import com.snowflake.snowpark.udaf._
import com.snowflake.snowpark_java.test.{MyAvgUDAF, MySumUDAF}

// ==================== Scala UDAF Definitions (outside test class to avoid serialization issues) ====================

// Scala UDAF that computes sum of integers
class ScalaSumUDAF extends UDAF1[Long, Long, Int] {
  override def outputType(): DataType = LongType

  override def initialize(): Long = 0L

  override def accumulate(state: Long, input: Int): Long = state + input

  override def merge(state1: Long, state2: Long): Long = state1 + state2

  override def terminate(state: Long): Long = state
}

// Note: UDAF0 is not supported by Snowflake server (cannot create UDAF with 0 arguments)

// State class for average - must have no-arg constructor for Kryo deserialization
class ScalaAvgState extends Serializable {
  var sum: Long = 0L
  var count: Long = 0L
}

// Scala UDAF that computes average using a custom class for state
class ScalaAvgUDAF extends UDAF1[ScalaAvgState, Double, Int] {
  override def outputType(): DataType = DoubleType

  override def initialize(): ScalaAvgState = new ScalaAvgState()

  override def accumulate(state: ScalaAvgState, input: Int): ScalaAvgState = {
    state.sum += input
    state.count += 1
    state
  }

  override def merge(state1: ScalaAvgState, state2: ScalaAvgState): ScalaAvgState = {
    state1.sum += state2.sum
    state1.count += state2.count
    state1
  }

  override def terminate(state: ScalaAvgState): Double = {
    if (state.count == 0) 0.0 else state.sum.toDouble / state.count
  }
}

// Scala UDAF with 2 input arguments
class ScalaSumProductUDAF extends UDAF2[Long, Long, Int, Int] {
  override def outputType(): DataType = LongType

  override def initialize(): Long = 0L

  override def accumulate(state: Long, a: Int, b: Int): Long = state + (a * b)

  override def merge(state1: Long, state2: Long): Long = state1 + state2

  override def terminate(state: Long): Long = state
}

// Custom state class for UDAF - demonstrates using a custom class instead of primitives/arrays
// IMPORTANT: Kryo requires a no-arg constructor for deserialization, so we use var fields
// initialized to default values instead of a case class with constructor parameters
class AvgState extends Serializable {
  var sum: Long = 0L
  var count: Long = 0L
}

// Scala UDAF that computes average using a custom class for state
class ScalaCustomClassAvgUDAF extends UDAF1[AvgState, Double, Int] {
  override def outputType(): DataType = DoubleType

  override def initialize(): AvgState = new AvgState()

  override def accumulate(state: AvgState, input: Int): AvgState = {
    state.sum += input
    state.count += 1
    state
  }

  override def merge(state1: AvgState, state2: AvgState): AvgState = {
    state1.sum += state2.sum
    state1.count += state2.count
    state1
  }

  override def terminate(state: AvgState): Double = {
    if (state.count == 0) 0.0 else state.sum.toDouble / state.count
  }
}

class UDAFSuite extends TestData {
  import session.implicits._

  // ==================== Java UDAF Tests ====================

  test("testTemporaryJavaUDAF") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new MySumUDAF()
    val mySum = session.udaf.registerTemporary("my_sum", udaf)

    val df = session.createDataFrame(Seq(1, 2, 3)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 6)
  }

  test("testPermanentJavaUDAF") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val stageName = randomName()
    try {
      session.sql(s"CREATE STAGE $stageName").collect()
      val udaf = new MySumUDAF()
      val mySum = session.udaf.registerPermanent(randomName(), udaf, s"@$stageName")

      val df = session.createDataFrame(Seq(1, 2, 3)).toDF("a")
      val result = df.select(mySum(col("a"))).collect()
      assert(result(0).getLong(0) == 6)
    } finally {
      session.sql(s"DROP STAGE IF EXISTS $stageName").collect()
    }
  }

  test("testJavaUDAF with group by") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new MySumUDAF()
    val mySum = session.udaf.registerTemporary("java_sum_group", udaf)

    val df = session.createDataFrame(Seq(("a", 1), ("a", 2), ("b", 3), ("b", 4))).toDF("grp", "val")
    val result = df.groupBy(col("grp")).agg(mySum(col("val"))).sort(col("grp")).collect()

    assert(result.length == 2)
    assert(result(0).getString(0) == "a")
    assert(result(0).getLong(1) == 3) // 1 + 2
    assert(result(1).getString(0) == "b")
    assert(result(1).getLong(1) == 7) // 3 + 4
  }

  test("testJavaUDAF with custom state class") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new MyAvgUDAF()
    val myAvg = session.udaf.registerTemporary("java_avg", udaf)

    val df = session.createDataFrame(Seq(10, 20, 30)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    // (10 + 20 + 30) / 3 = 60 / 3 = 20.0
    assert(result(0).getDouble(0) == 20.0)
  }

  // ==================== Scala UDAF Tests ====================

  test("testTemporaryScalaUDAF") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new ScalaSumUDAF()
    val mySum = session.udaf.registerTemporary("scala_sum", udaf)

    val df = session.createDataFrame(Seq(1, 2, 3)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 6)
  }

  test("testAnonymousTemporaryScalaUDAF") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new ScalaSumUDAF()
    val mySum = session.udaf.registerTemporary(udaf)

    val df = session.createDataFrame(Seq(10, 20, 30)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 60)
  }

  test("testPermanentScalaUDAF") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val stageName = randomName()
    try {
      session.sql(s"CREATE STAGE $stageName").collect()
      val udaf = new ScalaSumUDAF()
      val mySum = session.udaf.registerPermanent(randomName(), udaf, s"@$stageName")

      val df = session.createDataFrame(Seq(1, 2, 3, 4)).toDF("a")
      val result = df.select(mySum(col("a"))).collect()
      assert(result(0).getLong(0) == 10)
    } finally {
      session.sql(s"DROP STAGE IF EXISTS $stageName").collect()
    }
  }

  // Note: UDAF0 test removed - Snowflake doesn't support UDAFs with 0 arguments

  test("testScalaUDAF1 - average") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new ScalaAvgUDAF()
    val myAvg = session.udaf.registerTemporary("scala_avg", udaf)

    val df = session.createDataFrame(Seq(10, 20, 30)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getDouble(0) == 20.0)
  }

  test("testScalaUDAF with custom class state") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new ScalaCustomClassAvgUDAF()
    val myAvg = session.udaf.registerTemporary("scala_custom_class_avg", udaf)

    val df = session.createDataFrame(Seq(10, 20, 30, 40)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    // (10 + 20 + 30 + 40) / 4 = 100 / 4 = 25.0
    assert(result(0).getDouble(0) == 25.0)
  }

  test("testScalaUDAF2 - sum of products") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new ScalaSumProductUDAF()
    val mySumProduct = session.udaf.registerTemporary("scala_sum_product", udaf)

    val df = session.createDataFrame(Seq((1, 2), (3, 4), (5, 6))).toDF("a", "b")
    // 1*2 + 3*4 + 5*6 = 2 + 12 + 30 = 44
    val result = df.select(mySumProduct(col("a"), col("b"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 44)
  }

  test("testScalaUDAF with group by") {
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
    val udaf = new ScalaSumUDAF()
    val mySum = session.udaf.registerTemporary("scala_sum_group", udaf)

    val df = session.createDataFrame(Seq(("a", 1), ("a", 2), ("b", 3), ("b", 4))).toDF("grp", "val")
    val result = df.groupBy(col("grp")).agg(mySum(col("val"))).sort(col("grp")).collect()

    assert(result.length == 2)
    assert(result(0).getString(0) == "a")
    assert(result(0).getLong(1) == 3) // 1 + 2
    assert(result(1).getString(0) == "b")
    assert(result(1).getLong(1) == 7) // 3 + 4
  }
}
