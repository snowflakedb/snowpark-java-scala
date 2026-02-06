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

// State class for average - regular class with no-arg constructor for Kryo deserialization
class ScalaAvgState extends Serializable {
  var sum: Long = 0L
  var count: Long = 0L
}

// Case class state - works because Kryo uses Objenesis to instantiate without no-arg constructor
// Objenesis must be available in the server runtime (added to UdfResources dependencies)
case class CaseClassAvgState(var sum: Long = 0L, var count: Long = 0L)

// Scala UDAF using case class for state - demonstrates that case classes work with Kryo/Objenesis
class ScalaCaseClassAvgUDAF extends UDAF1[CaseClassAvgState, Double, Int] {
  override def outputType(): DataType = DoubleType

  override def initialize(): CaseClassAvgState = CaseClassAvgState()

  override def accumulate(state: CaseClassAvgState, input: Int): CaseClassAvgState = {
    state.sum += input
    state.count += 1
    state
  }

  override def merge(state1: CaseClassAvgState, state2: CaseClassAvgState): CaseClassAvgState = {
    state1.sum += state2.sum
    state1.count += state2.count
    state1
  }

  override def terminate(state: CaseClassAvgState): Double = {
    if (state.count == 0) 0.0 else state.sum.toDouble / state.count
  }
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
// Note: Kryo can use Objenesis to instantiate classes without no-arg constructors (e.g., case classes)
// Both regular classes with no-arg constructors and case classes are supported.
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    session.sql("ALTER SESSION SET ENABLE_JAVA_UDAF = TRUE").collect()
  }

  override def afterAll(): Unit = {
    session.sql("ALTER SESSION UNSET ENABLE_JAVA_UDAF").collect()
    super.afterAll()
  }

  // ==================== Java UDAF Tests ====================

  test("testTemporaryJavaUDAF") {
    val udaf = new MySumUDAF()
    val mySum = session.udaf.registerTemporary("my_sum", udaf)

    val df = session.createDataFrame(Seq(1, 2, 3)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 6)
  }

  test("testPermanentJavaUDAF") {
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
    val udaf = new ScalaSumUDAF()
    val mySum = session.udaf.registerTemporary("scala_sum", udaf)

    val df = session.createDataFrame(Seq(1, 2, 3)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 6)
  }

  test("testAnonymousTemporaryScalaUDAF") {
    val udaf = new ScalaSumUDAF()
    val mySum = session.udaf.registerTemporary(udaf)

    val df = session.createDataFrame(Seq(10, 20, 30)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 60)
  }

  test("testPermanentScalaUDAF") {
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

  test("testScalaUDAF1 - average") {
    val udaf = new ScalaAvgUDAF()
    val myAvg = session.udaf.registerTemporary("scala_avg", udaf)

    val df = session.createDataFrame(Seq(10, 20, 30)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getDouble(0) == 20.0)
  }

  test("testScalaUDAF with custom class state") {
    val udaf = new ScalaCustomClassAvgUDAF()
    val myAvg = session.udaf.registerTemporary("scala_custom_class_avg", udaf)

    val df = session.createDataFrame(Seq(10, 20, 30, 40)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    // (10 + 20 + 30 + 40) / 4 = 100 / 4 = 25.0
    assert(result(0).getDouble(0) == 25.0)
  }

  test("testScalaUDAF2 - sum of products") {
    val udaf = new ScalaSumProductUDAF()
    val mySumProduct = session.udaf.registerTemporary("scala_sum_product", udaf)

    val df = session.createDataFrame(Seq((1, 2), (3, 4), (5, 6))).toDF("a", "b")
    // 1*2 + 3*4 + 5*6 = 2 + 12 + 30 = 44
    val result = df.select(mySumProduct(col("a"), col("b"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 44)
  }

  test("testScalaUDAF with group by") {
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

  test("testScalaUDAF with case class state") {
    val udaf = new ScalaCaseClassAvgUDAF()
    val myAvg = session.udaf.registerTemporary("scala_case_class_avg", udaf)

    val df = session.createDataFrame(Seq(10, 20, 30, 40, 50)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    // (10 + 20 + 30 + 40 + 50) / 5 = 150 / 5 = 30.0
    assert(result(0).getDouble(0) == 30.0)
  }

  // ==================== functions.udaf() convenience method tests ====================

  test("test functions.udaf() with ScalaSumUDAF") {
    val mySum = udaf(new ScalaSumUDAF())

    val df = session.createDataFrame(Seq(1, 2, 3, 4, 5)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    // 1 + 2 + 3 + 4 + 5 = 15
    assert(result(0).getLong(0) == 15)
  }

  test("test functions.udaf() with ScalaAvgUDAF") {
    val myAvg = udaf(new ScalaAvgUDAF())

    val df = session.createDataFrame(Seq(10, 20, 30, 40)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    // (10 + 20 + 30 + 40) / 4 = 100 / 4 = 25.0
    assert(result(0).getDouble(0) == 25.0)
  }

  test("test functions.udaf() with UDAF2") {
    val mySumProduct = udaf(new ScalaSumProductUDAF())

    val df = session.createDataFrame(Seq((2, 3), (4, 5), (6, 7))).toDF("a", "b")
    // 2*3 + 4*5 + 6*7 = 6 + 20 + 42 = 68
    val result = df.select(mySumProduct(col("a"), col("b"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 68)
  }

  test("test functions.udaf() with group by") {
    val mySum = udaf(new ScalaSumUDAF())

    val df =
      session.createDataFrame(Seq(("x", 10), ("x", 20), ("y", 30), ("y", 40))).toDF("grp", "val")
    val result = df.groupBy(col("grp")).agg(mySum(col("val"))).sort(col("grp")).collect()

    assert(result.length == 2)
    assert(result(0).getString(0) == "x")
    assert(result(0).getLong(1) == 30) // 10 + 20
    assert(result(1).getString(0) == "y")
    assert(result(1).getLong(1) == 70) // 30 + 40
  }

  test("test functions.udaf() with case class state") {
    val myAvg = udaf(new ScalaCaseClassAvgUDAF())

    val df = session.createDataFrame(Seq(100, 200, 300)).toDF("a")
    val result = df.select(myAvg(col("a"))).collect()
    assert(result.length == 1)
    // (100 + 200 + 300) / 3 = 600 / 3 = 200.0
    assert(result(0).getDouble(0) == 200.0)
  }

  test("test functions.udaf() with single row") {
    val mySum = udaf(new ScalaSumUDAF())

    val df = session.createDataFrame(Seq(42)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 42)
  }

  test("test functions.udaf() with multiple UDAFs in same query") {
    val mySum = udaf(new ScalaSumUDAF())
    val myAvg = udaf(new ScalaAvgUDAF())

    val df = session.createDataFrame(Seq(10, 20, 30)).toDF("a")
    val result = df.select(mySum(col("a")), myAvg(col("a"))).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 60) // sum: 10 + 20 + 30 = 60
    assert(result(0).getDouble(1) == 20.0) // avg: 60 / 3 = 20.0
  }

  test("test functions.udaf() with filter") {
    val mySum = udaf(new ScalaSumUDAF())

    val df = session.createDataFrame(Seq(1, 2, 3, 4, 5, 6)).toDF("a")
    // Filter for even numbers only
    val result = df.filter(col("a") % lit(2) === lit(0)).select(mySum(col("a"))).collect()
    assert(result.length == 1)
    // 2 + 4 + 6 = 12
    assert(result(0).getLong(0) == 12)
  }

  test("test functions.udaf() reuse same UDAF instance") {
    val mySum = udaf(new ScalaSumUDAF())

    val df1 = session.createDataFrame(Seq(1, 2, 3)).toDF("a")
    val df2 = session.createDataFrame(Seq(10, 20, 30)).toDF("b")

    val result1 = df1.select(mySum(col("a"))).collect()
    val result2 = df2.select(mySum(col("b"))).collect()

    assert(result1(0).getLong(0) == 6) // 1 + 2 + 3
    assert(result2(0).getLong(0) == 60) // 10 + 20 + 30
  }

  test("test functions.udaf() with multiple groups") {
    val mySum = udaf(new ScalaSumUDAF())

    val df = session
      .createDataFrame(
        Seq(
          ("a", 1),
          ("b", 2),
          ("c", 3),
          ("a", 4),
          ("b", 5),
          ("c", 6),
          ("a", 7),
          ("b", 8),
          ("c", 9)))
      .toDF("grp", "val")

    val result = df.groupBy(col("grp")).agg(mySum(col("val"))).sort(col("grp")).collect()

    assert(result.length == 3)
    assert(result(0).getString(0) == "a")
    assert(result(0).getLong(1) == 12) // 1 + 4 + 7
    assert(result(1).getString(0) == "b")
    assert(result(1).getLong(1) == 15) // 2 + 5 + 8
    assert(result(2).getString(0) == "c")
    assert(result(2).getLong(1) == 18) // 3 + 6 + 9
  }

  test("test functions.udaf() with large numbers") {
    val mySum = udaf(new ScalaSumUDAF())

    val df = session.createDataFrame(Seq(Int.MaxValue - 1, 1, 1)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    // Int.MaxValue - 1 + 1 + 1 = Int.MaxValue + 1 = 2147483648L
    assert(result(0).getLong(0) == Int.MaxValue.toLong + 1)
  }

  test("test functions.udaf() with negative numbers") {
    val mySum = udaf(new ScalaSumUDAF())

    val df = session.createDataFrame(Seq(-10, 20, -30, 40)).toDF("a")
    val result = df.select(mySum(col("a"))).collect()
    assert(result.length == 1)
    // -10 + 20 - 30 + 40 = 20
    assert(result(0).getLong(0) == 20)
  }

  test("test functions.udaf() chained with other DataFrame operations") {
    val mySum = udaf(new ScalaSumUDAF())

    val df = session
      .createDataFrame(Seq(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("c", 5)))
      .toDF("grp", "val")
      .filter(col("grp") =!= lit("c")) // exclude group "c"
      .groupBy(col("grp"))
      .agg(mySum(col("val")).as("total"))
      .filter(col("total") > lit(5)) // only groups with total > 5

    val result = df.collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "b")
    assert(result(0).getLong(1) == 7) // 3 + 4
  }

  test("test functions.udaf() with UDAF2 and group by") {
    val mySumProduct = udaf(new ScalaSumProductUDAF())

    val df = session
      .createDataFrame(Seq(("x", 1, 2), ("x", 3, 4), ("y", 5, 6), ("y", 7, 8)))
      .toDF("grp", "a", "b")

    val result =
      df.groupBy(col("grp")).agg(mySumProduct(col("a"), col("b"))).sort(col("grp")).collect()

    assert(result.length == 2)
    assert(result(0).getString(0) == "x")
    assert(result(0).getLong(1) == 14) // 1*2 + 3*4 = 2 + 12 = 14
    assert(result(1).getString(0) == "y")
    assert(result(1).getLong(1) == 86) // 5*6 + 7*8 = 30 + 56 = 86
  }
}
