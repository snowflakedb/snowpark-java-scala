package com.snowflake.perf

import com.snowflake.snowpark.{SNTestBase, Session, TestUtils}
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.{DoubleType, StringType, StructField, StructType}

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.util.Random

trait PerfBase extends SNTestBase {
  // disabled by default
  // to enable perf test use maven flag `-DargLine="-DPERF_TEST=true"`
  lazy val isPerfTest: Boolean =
    System.getProperty("PERF_TEST") match {
      case null => false
      case value if value.toLowerCase() == "true" => true
      case _ => false
    }

  lazy val snowhouseProfile: String = "snowhouse.properties"
  lazy val snowhouseSession: Session = Session.builder.configFile(snowhouseProfile).create

  lazy val resultFileName: String =
    s"perf_test_result_${Random.alphanumeric.take(5).mkString}.csv"

  lazy val tmpStageName: String = randomStageName()

  val perfTestResultTable: String = "SNOWPARK_PERF_TEST_RESULT"

  // name of test, time consumption of test
  def writeResult(name: String, time: Double): Unit = {
    val data = s"$name,$time\n"
    Files.write(
      Paths.get(resultFileName),
      data.getBytes,
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND)
  }

  override def beforeAll: Unit = {
    super.beforeAll
    import session.implicits._
    // run a query to start warehouse
    Seq(1, 2).toDF("a").filter(col("a") > 1).collect()
  }

  override def afterAll: Unit = {
    if (isPerfTest) {
      TestUtils.createStage(tmpStageName, isTemporary = true, snowhouseSession)
      snowhouseSession.file.put(s"file://$resultFileName", s"@$tmpStageName")
      val fileSchema = StructType(
        StructField("TEST_NAME", StringType),
        StructField("TIME_CONSUMPTION", DoubleType))
      snowhouseSession.read
        .schema(fileSchema)
        .csv(s"@$tmpStageName")
        .copyInto(
          perfTestResultTable,
          Seq(lit("scala"), current_timestamp(), col("$1"), col("$2")))

      Files.delete(Paths.get(resultFileName))
      snowhouseSession.close()
    }
    super.afterAll
  }

  def timer(func: => Unit): Double = {
    val start = System.currentTimeMillis()
    func
    val end = System.currentTimeMillis()
    (end - start) / 1000.0
  }

  // use `perfTest` to replace `test` function in all perf tests
  def perfTest(testName: String)(func: => Unit): Unit = {
    if (isPerfTest) {
      test(testName) {
        try {
          writeResult(testName, timer(func))
          succeed
        } catch {
          case ex: Exception =>
            writeResult(testName, -1.0) // -1.0 if failed
            throw ex
        }
      }
    } else {
      ignore(testName) {
        func
        succeed
      }
    }
  }
}
