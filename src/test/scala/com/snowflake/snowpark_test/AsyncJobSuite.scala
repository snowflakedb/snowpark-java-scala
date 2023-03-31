package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.scalatest.{BeforeAndAfterEach, Tag}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Success}

object TestScalaSP {
  def asyncBasic(session: com.snowflake.snowpark.Session, count: Int, seconds: Int): String = {
    val asyncJobs = new scala.collection.mutable.ArrayBuffer[AsyncJob]()
    var index = 0
    // Kick-off many async jobs
    while (index < count) {
      val df = session.sql(
        s"select count(*) from table(generator(timeLimit => ${seconds + index}))")
      asyncJobs.append(df.async.collect())
      index += 1
    }
    // wait for async jobs done, and get result
    asyncJobs.map { asyncJob =>
      asyncJob.getRows()(0).getLong(0)
    }.mkString("[ ", ", ", " ]")
  }
}

class AsyncJobSuite extends TestData with BeforeAndAfterEach {
  val tmpStageName: String = randomStageName()
  val targetStageName = randomStageName()

  val tableName: String = randomName()
  val tableName1: String = randomName()
  val tableName2: String = randomName()
  private val userSchema: StructType = StructType(
    Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", DoubleType)))

  // session to verify permanent udf
  //  lazy private val newSession = Session.builder.configFile(defaultProfile).create
  override def beforeAll(): Unit = {
    super.beforeAll()
    // create temporary stage to store the file
    runQuery(s"CREATE TEMPORARY STAGE $tmpStageName", session)
    //    runQuery(s"CREATE TEMPORARY STAGE $tmpStageName", newSession)
    // Create temp target stage for writing DF to file test.
    runQuery(s"CREATE TEMPORARY STAGE $targetStageName", session)
    // upload the file to stage
    uploadFileToStage(tmpStageName, testFileCsv, compress = false)
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session, Some(tmpStageName))
      // In stored procs mode, there is only one session
      //      TestUtils.addDepsToClassPath(newSession, Some(tmpStageName))
    }
  }

  override def afterAll(): Unit = {
    // drop the temporary stages
    runQuery(s"DROP STAGE IF EXISTS $tmpStageName", session)
    //    runQuery(s"DROP STAGE IF EXISTS $tmpStageName", newSession)
    dropTable(tableName)
    dropTable(tableName1)
    dropTable(tableName2)

    super.afterAll()
  }

  test("test 10 jobs") {
    println(TestScalaSP.asyncBasic(session, 10, 5))
  }

  test("async DataFrame collect(): common case") {
    val df = session.range(5)
    val asyncJob = df.async.collect()

    // getResult()
    val res = asyncJob.getResult()
    assert(res.isInstanceOf[Array[Row]])
    checkResult(res, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

    // getIterator()
    val itr = asyncJob.getIterator()
    assert(itr.isInstanceOf[Iterator[Row]])
    checkResultIterator(itr, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

    // getRows()
    val rows = asyncJob.getRows()
    assert(rows.isInstanceOf[Array[Row]])
    checkResult(rows, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
  }

  test("async DataFrame collect(): empty ResultSet") {
    val df = session.range(5).filter(col("ID") > 100)
    val asyncJob = df.async.collect()

    // getResult()
    val res = asyncJob.getResult()
    assert(res.isInstanceOf[Array[Row]])
    assert(res.isEmpty)

    // getIterator()
    val itr = asyncJob.getIterator()
    assert(itr.isInstanceOf[Iterator[Row]])
    assert(!itr.hasNext)

    // getRows()
    val rows = asyncJob.getRows()
    assert(rows.isInstanceOf[Array[Row]])
    assert(rows.isEmpty)
  }

  test("async DataFrame collect(): negative") {
    val df = session.sql("select to_number('not_a_number')")
    val asyncJob = df.async.collect()

    // getResult() raises exception
    val expectedMessage = "Numeric value 'not_a_number' is not recognized"
    val ex1 = intercept[SnowflakeSQLException] {
      asyncJob.getResult()
    }
    assert(ex1.getMessage.contains(expectedMessage))

    // getIterator() raises exception
    val ex2 = intercept[SnowflakeSQLException] {
      asyncJob.getIterator()
    }
    assert(ex2.getMessage.contains(expectedMessage))

    // getRows() raises exception
    val ex3 = intercept[SnowflakeSQLException] {
      asyncJob.getRows()
    }
    assert(ex3.getMessage.contains(expectedMessage))
  }

  test("async DataFrame collect(): with timeout") {
    val asyncJob = session.sql("select SYSTEM$WAIT(30)").async.collect()

    try {
      // getResult() raises exception
      val ex1 = intercept[SnowparkClientException] {
        asyncJob.getResult(3)
      }
      assert(ex1.errorCode.equals("0318"))
      assert(
        ex1.getMessage.matches(".*The query with the ID .* is still running and " +
          "has the current status RUNNING. The function call has been running for 2 seconds,.*"))

      // getIterator() raises exception
      val ex2 = intercept[SnowparkClientException] {
        asyncJob.getIterator(3)
      }
      assert(ex2.errorCode.equals("0318"))

      // getRows() raises exception
      val ex3 = intercept[SnowparkClientException] {
        asyncJob.getRows(3)
      }
      assert(ex3.errorCode.equals("0318"))
    } finally {
      asyncJob.cancel()
    }
  }

  test("async DataFrame collect(): cooperate with Future and succeed") {
    val asyncJob = session.sql("select SYSTEM$WAIT(3)").async.collect()

    val asyncFuture = Future { asyncJob.getResult() }

    @volatile var futureDone = false
    var futureResult: Option[Boolean] = None
    asyncFuture onComplete {
      case Success(rows) =>
        checkResult(rows, Seq(Row("waited 3 seconds")))
        futureResult = Some(true)
        futureDone = true
      case Failure(_) =>
        futureResult = Some(false)
        futureDone = true
        fail("The future should be successful")
    }

    // The main thread waits for onComplete() callback to be done.
    var count = 0
    while (!futureDone && count < 60) {
      Thread.sleep(1000)
      count = count + 1
    }

    assert(futureResult.get)
  }

  test("async DataFrame collect(): cooperate with Future but fail") {
    val asyncJob = session.sql("select to_number('not_a_number')").async.collect()
    val asyncFuture = Future { asyncJob.getResult() }

    @volatile var futureDone = false
    var futureResult: Option[Boolean] = None
    asyncFuture onComplete {
      case Success(_) =>
        futureResult = Some(true)
        futureDone = true
        fail("The future should fail")
      case Failure(t) =>
        futureResult = Some(false)
        futureDone = true
        assert(t.getMessage.contains("Numeric value 'not_a_number' is not recognized"))
    }

    // The main thread waits for onComplete() callback to be done.
    var count = 0
    while (!futureDone && count < 60) {
      Thread.sleep(1000)
      count = count + 1
    }

    assert(!futureResult.get)
  }

  test("test AsyncJob isRunning() and cancel()") {
    // TODO just use the first session
    // val session2 = Session.builder.configFile(defaultProfile).create
    val session2 = session
    try {
      // positive test
      val asyncJob = session.sql("select SYSTEM$WAIT(3)").async.collect()
      while (!asyncJob.isDone()) { Thread.sleep(1000) }
      assert(asyncJob.isDone())

      // negative test
      val queryID = session.sql("select SYSTEM$WAIT(100)").async.collect().getQueryId()
      val asyncJob2 = session2.createAsyncJob(queryID)
      assert(!asyncJob2.isDone())
      asyncJob2.cancel()
      val start = System.currentTimeMillis()
      while (!asyncJob2.isDone()) { Thread.sleep(1000) }
      assert((System.currentTimeMillis() - start) / 1000 < 60)
      assert(asyncJob2.isDone())

      val ex1 = intercept[SnowflakeSQLException] {
        asyncJob2.getRows()
      }
      assert(ex1.getMessage.contains("SQL execution canceled"))
    } finally {
      // session2.close()
    }
  }

  test("async DataFrame toLocalIterator(): common case") {
    val df = session.range(5)
    val asyncJob = df.async.toLocalIterator()

    // getResult()
    val res = asyncJob.getResult()
    assert(res.isInstanceOf[Iterator[Row]])
    checkResultIterator(res, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

    // getIterator()
    val itr = asyncJob.getIterator()
    assert(itr.isInstanceOf[Iterator[Row]])
    checkResultIterator(itr, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

    // getRows()
    val rows = asyncJob.getRows()
    assert(rows.isInstanceOf[Array[Row]])
    checkResult(rows, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
  }

  test("async DataFrame toLocalIterator(): empty ResultSet") {
    val df = session.range(5).filter(col("ID") > 100)
    val asyncJob = df.async.toLocalIterator()

    // getResult()
    val res = asyncJob.getResult()
    assert(res.isInstanceOf[Iterator[Row]])
    assert(!res.hasNext)

    // getIterator()
    val itr = asyncJob.getIterator()
    assert(itr.isInstanceOf[Iterator[Row]])
    assert(!itr.hasNext)

    // getRows()
    val rows = asyncJob.getRows()
    assert(rows.isInstanceOf[Array[Row]])
    assert(rows.isEmpty)
  }

  test("async DataFrame toLocalIterator(): negative") {
    val df = session.sql("select to_number('not_a_number')")
    val asyncJob = df.async.toLocalIterator()

    // getResult() raises exception
    val expectedMessage = "Numeric value 'not_a_number' is not recognized"
    val ex1 = intercept[SnowflakeSQLException] {
      asyncJob.getResult()
    }
    assert(ex1.getMessage.contains(expectedMessage))

    // getIterator() raises exception
    val ex2 = intercept[SnowflakeSQLException] {
      asyncJob.getIterator()
    }
    assert(ex2.getMessage.contains(expectedMessage))

    // getRows() raises exception
    val ex3 = intercept[SnowflakeSQLException] {
      asyncJob.getRows()
    }
    assert(ex3.getMessage.contains(expectedMessage))
  }

  test("async DataFrame count()") {
    // getResult() positive
    var asyncJob = session.range(5).async.count()
    val res = asyncJob.getResult()
    assert(res.isInstanceOf[Long])
    assert(res == 5)

    // getResult() negative
    asyncJob = session.sql("select to_number('not_a_number') as C1 where C1 > 0").async.count()
    val expectedMessage = "Numeric value 'not_a_number' is not recognized"
    val ex1 = intercept[SnowflakeSQLException] {
      asyncJob.getResult()
    }
    assert(ex1.getMessage.contains(expectedMessage))
  }

  test("session.createAsyncJob(queryID)") {
    // TODO just use the first session
    // val session2 = Session.builder.configFile(defaultProfile).create
    val session2 = session
    try {
      // positive test
      val queryID = session.range(5).async.collect().getQueryId()
      val asyncJob = session2.createAsyncJob(queryID)
      val itr = asyncJob.getIterator()
      assert(itr.isInstanceOf[Iterator[Row]])
      checkResultIterator(itr, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
      val rows = asyncJob.getRows()
      assert(rows.isInstanceOf[Array[Row]])
      checkResult(rows, Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

      // negative test
      val queryID2 = session.sql("select to_number('not_a_number')").async.collect().getQueryId()
      val asyncJob2 = session2.createAsyncJob(queryID2)
      val expectedMessage = "Numeric value 'not_a_number' is not recognized"
      val ex1 = intercept[SnowflakeSQLException] {
        // getIterator() raises exception
        asyncJob2.getIterator()
      }
      assert(ex1.getMessage.contains(expectedMessage))
      val ex2 = intercept[SnowflakeSQLException] {
        // getRows() raises exception
        asyncJob2.getRows()
      }
      assert(ex2.getMessage.contains(expectedMessage))

      // use invalid query ID
      val asyncJob3 = session2.createAsyncJob("negative_test_invalid_query_id")
      val ex3 = intercept[SnowflakeSQLException] {
        asyncJob3.getRows()
      }
      assert(ex3.getMessage.contains("No response or invalid response from GET request."))
    } finally {
      // session2.close()
    }
  }

  // This function is copied from DataFrameReader.testReadFile
  def testReadFile(testName: String, testTags: Tag*)(
      thunk: (() => DataFrameReader) => Unit): Unit = {
    // test select
    test(testName + " - SELECT", testTags: _*) {
      thunk(() => session.read)
    }

    // test copy
    test(testName + " - COPY", testTags: _*) {
      thunk(() => session.read.option("PURGE", false))
    }
  }
  // Run DataFrameReaderSuite.testReadFile("read csv test") asynchronously
  // Copy DataFrameReader.testReadFile("read csv test")
  // And modify to call collect with Async mode
  testReadFile("read csv test (async)")(reader => {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val df1 = reader().schema(userSchema).csv(testFileOnStage).async.collect().getResult()

    assert(df1.length == 2)
    assert(df1(0).length == 3)
    assert(df1 sameElements Array[Row](Row(1, "one", 1.2), Row(2, "two", 2.2)))

    assertThrows[SnowparkClientException](session.read.csv(testFileOnStage))

    // if user give a incorrect schema that there are type error
    // the system will throw SnowflakeSQLException during execution
    val incorrectSchema: StructType = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))
    val df2 = reader().schema(incorrectSchema).csv(testFileOnStage)
    assertThrows[SnowflakeSQLException](df2.async.collect().getResult())
  })

  //  // Run PermanentUDFSuite.test("mix temporary and permanent UDF") asynchronously
  //  test("mix temporary and permanent UDF (async)", JavaStoredProcExclude) {
  //    import functions.callUDF
  //    val udf = (x: Int) => x + 1
  //    val tempFuncName = randomName()
  //    val permFuncName = randomName()
  //    val stageName1 = randomName()
  //    try {
  //      runQuery(s"create stage $stageName1", session)
  //      session.udf.registerTemporary(tempFuncName, udf)
  //      session.udf.registerPermanent(permFuncName, udf, stageName1)
  //      val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
  //      checkResult(
  //        df.select(callUDF(tempFuncName, df("a"))).async.collect().getResult(),
  //        Seq(Row(2), Row(3)))
  //      checkResult(
  //        df.select(callUDF(permFuncName, df("a"))).async.collect().getResult(),
  //        Seq(Row(2), Row(3)))
  //
  //      // another session
  //      val df2 = newSession.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
  //      checkResult(
  //        df2.select(callUDF(permFuncName, df("a"))).async.collect().getResult(),
  //        Seq(Row(2), Row(3)))
  //      assertThrows[SnowflakeSQLException](
  //        df2.select(callUDF(tempFuncName, df("a"))).async.collect().getResult())
  //
  //    } finally {
  //      runQuery(s"drop function if exists $tempFuncName(INT)", session)
  //      runQuery(s"drop function if exists $permFuncName(INT)", session)
  //      runQuery(s"drop stage if exists $stageName1", session)
  //    }
  //  }

  // Run DataFrameSuite.test("cacheResult") in asynchronously
  test("cacheResult (async)") {
    val tableName = randomName()
    runQuery(s"create table $tableName (num int)", session)
    runQuery(s"insert into $tableName values(1),(2)", session)

    val df = session.table(tableName)
    checkResult(df.async.collect().getResult(), Seq(Row(1), Row(2)))

    // update df
    runQuery(s"insert into $tableName values(3)", session)
    checkResult(df.async.collect().getResult(), Seq(Row(1), Row(2), Row(3)))

    val df1 = df.cacheResult()
    runQuery(s"insert into $tableName values(4)", session)
    checkResult(df1.async.collect().getResult(), Seq(Row(1), Row(2), Row(3)))
    checkResult(df.async.collect().getResult(), Seq(Row(1), Row(2), Row(3), Row(4)))

    val df2 = df1.where(col("num") > 2)
    checkResult(df2.async.collect().getResult(), Seq(Row(3))) // no change on df based on cached df

    val df3 = df.where(col("num") > 2)
    checkResult(df3.async.collect().getResult(), Seq(Row(3), Row(4)))

    val df4 = df1.cacheResult()
    checkResult(df4.async.collect().getResult(), Seq(Row(1), Row(2), Row(3)))

    // drop original table, the make sure original query will not be invoked
    runQuery(s"drop table $tableName", session)
    checkResult(df1.async.collect().getResult(), Seq(Row(1), Row(2), Row(3)))
    checkResult(df2.async.collect().getResult(), Seq(Row(3)))
  }

  // Copy TableSuite.test("Consistent table name behaviors") and
  // update it to run saveAsTable() in async mode
  test("Consistent table name behaviors (async)", JavaStoredProcExclude) {
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
      val asyncJob1 = df.write.mode(SaveMode.Overwrite).async.saveAsTable(tableName)
      val res = asyncJob1.getResult()
      assert(res.isInstanceOf[Unit])
      val rows = asyncJob1.getRows()
      assert(
        rows.length == 1 && rows.head.length == 1 &&
          rows.head.getString(0).contains(s"Table $tableName successfully created"))
      // all session.table
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(Seq(db, sc, tableName)), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(Seq(sc, tableName)), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(Seq(tableName)), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(session.table(s"$db.$sc.$tableName"), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).async.saveAsTable(Seq(sc, tableName)).getResult()
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).async.saveAsTable(Seq(db, sc, tableName)).getResult()
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).async.saveAsTable(s"$db.$sc.$tableName").getResult()
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)

      df.write.mode(SaveMode.Overwrite).async.saveAsTable(list).getResult()
      checkAnswer(session.table(tableName), Seq(Row(1), Row(2), Row(3)))
      dropTable(tableName)
    } finally {
      dropTable(tableName)
    }
  }

  test("read stage file and saveAsTable in async") {
    val tableName = randomName()
    try {
      val testFileOnStage = s"@$tmpStageName/$testFileCsv"
      val df1 = session.read.schema(userSchema).csv(testFileOnStage)
      // Write with OverWrite
      df1.write.mode("OVERWRITE").async.saveAsTable(tableName).getResult()
      checkAnswer(session.table(tableName), Seq(Row(1, "one", 1.2), Row(2, "two", 2.2)))
      // Write with Append
      val df2 = session.read.schema(userSchema).option("PURGE", false).csv(testFileOnStage)
      df2.write.mode("APPEND").async.saveAsTable(tableName).getResult()
      checkAnswer(
        session.table(tableName),
        Seq(Row(1, "one", 1.2), Row(2, "two", 2.2), Row(1, "one", 1.2), Row(2, "two", 2.2)),
        false)
    } finally {
      dropTable(tableName)
    }
  }

  // Copy UpdatableSuite.test("update rows in table") and
  // modify it to run update() in async mode
  test("update rows in table (async)") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    val updatable = session.table(tableName1)

    val asyncJob1 = updatable.async.update(Map(col("b") -> lit(0)), col("a") === 1)
    assert(asyncJob1.getResult() == UpdateResult(2, 0))
    checkAnswer(updatable, Seq(Row(1, 0), Row(1, 0), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    val asyncJob2 = updatable.async.update(Map("b" -> lit(0)), col("a") === 1)
    assert(asyncJob2.getResult() == UpdateResult(2, 0))
    checkAnswer(updatable, Seq(Row(1, 0), Row(1, 0), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    val asyncJob3 = updatable.async.update(Map(col("a") -> lit(1), col("b") -> lit(0)))
    assert(asyncJob3.getResult() == UpdateResult(6, 0))
    checkAnswer(updatable, Seq(Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0), Row(1, 0)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    val asyncJob4 = updatable.async.update(Map("b" -> (col("a") + col("b"))))
    assert(asyncJob4.getResult() == UpdateResult(6, 0))
    checkAnswer(updatable, Seq(Row(1, 2), Row(1, 3), Row(2, 3), Row(2, 4), Row(3, 4), Row(3, 5)))
  }

  // Copy UpdatableSuite.test("update with join") and
  // modify it to run update() in async mode
  test("update with join (async)") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    val t1 = session.table(tableName1)
    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val t2 = session.table(tableName2)
    val asyncJob1 = t2.async.update(Map(col("n") -> lit(0)), t1("a") === t2("n"), t1)
    assert(asyncJob1.getResult() == UpdateResult(3, 3))
    checkAnswer(
      t2,
      Seq(Row(0, "A"), Row(0, "B"), Row(0, "C"), Row(4, "D"), Row(5, "E"), Row(6, "F")),
      sort = false)

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val asyncJob2 = t2.async.update(Map("n" -> lit(0)), t1("a") === t2("n"), t1)
    assert(asyncJob2.getResult() == UpdateResult(3, 3))
    checkAnswer(
      t2,
      Seq(Row(0, "A"), Row(0, "B"), Row(0, "C"), Row(4, "D"), Row(5, "E"), Row(6, "F")),
      sort = false)

    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    import session.implicits._
    val sd = Seq("A", "B", "D", "E").toDF("c")
    val asyncJob3 = t2.async.update(Map("n" -> lit(0)), t2("L") === sd("c"), sd)
    assert(asyncJob3.getResult() == UpdateResult(4, 0))
    checkAnswer(
      t2,
      Seq(Row(0, "A"), Row(0, "B"), Row(0, "D"), Row(0, "E"), Row(3, "C"), Row(6, "F")))
  }

  // Copy UpdatableSuite.test("delete rows from table") and
  // modify it to run delete() in async mode
  test("delete rows from table (async)") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    val updatable = session.table(tableName1)

    val asyncJob1 = updatable.async.delete(col("a") === 1 && col("b") === 2)
    assert(asyncJob1.getResult() == DeleteResult(1))
    checkAnswer(updatable, Seq(Row(1, 1), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    checkAnswer(updatable, Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    val asyncJob2 = updatable.async.delete()
    assert(asyncJob2.getResult() == DeleteResult(6))
    checkAnswer(updatable, Seq())
  }

  // Copy UpdatableSuite.test("delete with join") and
  // modify it to run delete() in async mode
  test("delete with join (async)") {
    testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName1)
    val t1 = session.table(tableName1)
    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    val t2 = session.table(tableName2)
    val asyncJob1 = t2.async.delete(t1("a") === t2("n"), t1)
    assert(asyncJob1.getResult() == DeleteResult(3))
    checkAnswer(t2, Seq(Row(4, "D"), Row(5, "E"), Row(6, "F")), sort = false)

    upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
    import session.implicits._
    val sd = Seq("A", "B", "D", "E").toDF("c")
    val asyncJob2 = t2.async.delete(t2("L") === sd("c"), sd)
    assert(asyncJob2.getResult() == DeleteResult(4))
    checkAnswer(t2, Seq(Row(3, "C"), Row(6, "F")))
  }

  // Copy UpdatableSuite.test("merge with multiple clause conditions") and
  // modify it to run it in async mode
  test("merge with multiple clause conditions") {
    import session.implicits._
    val targetDF = Seq((0, 10), (1, 11), (2, 12), (3, 13)).toDF("k", "v")
    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val target = session.table(tableName)
    val source = Seq((0, 20), (1, 21), (2, 22), (3, 23), (4, 24), (5, 25), (6, 26), (7, 27))
      .toDF("k", "v")

    val mergeBuilder = target
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
    assert(mergeBuilder.async.collect().getResult() == MergeResult(4, 2, 2))
    checkAnswer(
      target,
      Seq(Row(1, 21), Row(3, 3), Row(4, 4), Row(5, 25), Row(7, null), Row(null, 26)))
  }

  //  // Async executes Merge and get result in another session.
  //  test("merge async in one session and get the result in another", JavaStoredProcExclude) {
  //    import session.implicits._
  //    val targetDF = Seq((0, 10), (1, 11), (2, 12), (3, 13)).toDF("k", "v")
  //    targetDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  //    val target = session.table(tableName)
  //    val source = Seq((0, 20), (1, 21), (2, 22), (3, 23), (4, 24), (5, 25), (6, 26), (7, 27))
  //      .toDF("k", "v")
  //
  //    val mergeBuilder = target
  //      .merge(source, target("k") === source("k"))
  //      .whenMatched(source("v") < lit(21))
  //      .delete()
  //      .whenMatched(source("v") > lit(22))
  //      .update(Map("v" -> (source("v") - lit(20))))
  //      .whenMatched(source("v") =!= lit(21))
  //      .delete()
  //      .whenMatched
  //      .update(Map(target("v") -> source("v")))
  //      .whenNotMatched(source("v") < lit(25))
  //      .insert(Seq(source("k"), source("v") - lit(20)))
  //      .whenNotMatched(source("v") > lit(26))
  //      .insert(Map("k" -> source("k")))
  //      .whenNotMatched(source("v") =!= lit(25))
  //      .insert(Map(target("v") -> source("v")))
  //      .whenNotMatched
  //      .insert(Map("k" -> source("k"), "v" -> source("v")))
  //    val queryID = mergeBuilder.async.collect().getQueryId()
  //
  //    // get MergeResult and check result in another session.
  //    val mergeResultRows = newSession.createAsyncJob(queryID).getRows()
  //    checkResult(mergeResultRows, Seq(Row(4, 2, 2)))
  //    checkAnswer(
  //      newSession.table(tableName),
  //      Seq(Row(1, 21), Row(3, 3), Row(4, 4), Row(5, 25), Row(7, null), Row(null, 26)))
  //  }

  private def runCSvTestAsync(
      df: DataFrame,
      stageLocation: String,
      options: Map[String, Any],
      expectedWriteResult: Array[Row],
      outputFileExtension: String,
      saveMode: Option[SaveMode] = None) = {
    // Execute COPY INTO location and check result
    val writer = df.write.options(options)
    saveMode.foreach(writer.mode)
    val writeResult = writer.async.csv(stageLocation).getResult()
    checkResult(writeResult.rows, expectedWriteResult)

    // Check files are written to stage location
    val resultFiles = session.sql(s"ls $stageLocation").collect()
    assert(resultFiles.length == 1 && resultFiles(0).getString(0).endsWith(outputFileExtension))
  }

  private def runJsonTestAsync(
      df: DataFrame,
      stageLocation: String,
      options: Map[String, Any],
      expectedWriteResult: Array[Row],
      outputFileExtension: String,
      saveMode: Option[SaveMode] = None) = {
    // Execute COPY INTO location and check result
    val writer = df.write.options(options)
    saveMode.foreach(writer.mode)
    val writeResult = writer.async.json(stageLocation).getResult()
    checkResult(writeResult.rows, expectedWriteResult)

    // Check files are written to stage location
    val resultFiles = session.sql(s"ls $stageLocation").collect()
    assert(resultFiles.length == 1 && resultFiles(0).getString(0).endsWith(outputFileExtension))
  }

  private def runParquetTest(
      df: DataFrame,
      stageLocation: String,
      options: Map[String, Any],
      expectedNumberOfRow: Int,
      outputFileExtension: String,
      saveMode: Option[SaveMode] = None) = {
    // Execute COPY INTO location and check result
    val writer = df.write.options(options)
    saveMode.foreach(writer.mode)
    val writeResult = writer.async.parquet(stageLocation).getResult()
    // only check the number of row since file size often changed along with
    // server side updates.
    assert(writeResult.rows.head.getInt(0) == expectedNumberOfRow)

    // Check files are written to stage location
    val resultFiles = session.sql(s"ls $stageLocation").collect()
    assert(resultFiles.length == 1 && resultFiles(0).getString(0).endsWith(outputFileExtension))
  }

  // Copy DataFrameWriterSuiter.test("write CSV files: save mode and file format options") and
  // modify it to run it in async mode
  test("async: write CSV files: save mode and file format options") {
    createTable(tableName, "c1 int, c2 double, c3 string")
    runQuery(
      s"insert into $tableName values (1,1.1,'one'),(2,2.2,'two'),(null,null,null)",
      session)
    val schema = StructType(
      Seq(
        StructField("c1", IntegerType),
        StructField("c2", DoubleType),
        StructField("c3", StringType)))
    val df = session.table(tableName)
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // without any options
    runCSvTestAsync(df, path, Map.empty, Array(Row(3, 32, 46)), ".csv.gz")
    checkAnswer(
      session.read.schema(schema).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // by default, the mode is ErrorIfExist
    val ex = intercept[SnowflakeSQLException] {
      runCSvTestAsync(df, path, Map.empty, Array(Row(3, 32, 46)), ".csv.gz")
    }
    assert(ex.getMessage.contains("Files already existing at the unload destination"))

    // Test overwrite mode
    runCSvTestAsync(
      df,
      path,
      Map.empty,
      Array(Row(3, 32, 46)),
      ".csv.gz",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.schema(schema).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // test some file format options and values
    session.sql(s"remove $path").collect()
    val options1 = Map(
      "FIELD_DELIMITER" -> "'aa'",
      "RECORD_DELIMITER" -> "bbbb",
      "COMPRESSION" -> "NONE",
      "FILE_EXTENSION" -> "mycsv")
    runCSvTestAsync(df, path, options1, Array(Row(3, 47, 47)), ".mycsv")
    checkAnswer(
      session.read.schema(schema).options(options1).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // Test file format name only
    val fileFormatName = randomTableName()
    session
      .sql(
        s"CREATE OR REPLACE TEMPORARY FILE FORMAT $fileFormatName " +
          s"TYPE = CSV FIELD_DELIMITER = 'aa' RECORD_DELIMITER = 'bbbb' " +
          s"COMPRESSION = 'NONE' FILE_EXTENSION = 'mycsv'")
      .collect()
    runCSvTestAsync(
      df,
      path,
      Map("FORMAT_NAME" -> fileFormatName),
      Array(Row(3, 47, 47)),
      ".mycsv",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.schema(schema).options(options1).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))

    // Test file format name and some extra format options
    val fileFormatName2 = randomTableName()
    session
      .sql(
        s"CREATE OR REPLACE TEMPORARY FILE FORMAT $fileFormatName2 " +
          s"TYPE = CSV FIELD_DELIMITER = 'aa' RECORD_DELIMITER = 'bbbb'")
      .collect()
    val formatNameAndOptions =
      Map("FORMAT_NAME" -> fileFormatName2, "COMPRESSION" -> "NONE", "FILE_EXTENSION" -> "mycsv")
    runCSvTestAsync(
      df,
      path,
      formatNameAndOptions,
      Array(Row(3, 47, 47)),
      ".mycsv",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.schema(schema).options(options1).csv(path),
      Seq(Row(1, 1.1, "one"), Row(2, 2.2, "two"), Row(null, null, null)))
  }

  // Copy DataFrameWriterSuiter.test("write JSON files: file format options"") and
  // modify it to run it in async mode
  test("async: write JSON files: file format options") {
    createTable(tableName, "c1 int, c2 string")
    runQuery(s"insert into $tableName values (1,'one'),(2,'two')", session)
    val df = session.table(tableName).select(array_construct(col("c1"), col("c2")))
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // write with default
    runJsonTestAsync(df, path, Map.empty, Array(Row(2, 20, 40)), ".json.gz")
    checkAnswer(
      session.read.json(path),
      Seq(Row("[\n  1,\n  \"one\"\n]"), Row("[\n  2,\n  \"two\"\n]")))

    // write one column and overwrite
    val df2 = session.table(tableName).select(to_variant(col("c2")))
    runJsonTestAsync(
      df2,
      path,
      Map.empty,
      Array(Row(2, 12, 32)),
      ".json.gz",
      Some(SaveMode.Overwrite))
    checkAnswer(session.read.json(path), Seq(Row("\"one\""), Row("\"two\"")))

    // write with format_name
    val formatName = randomTableName()
    session.sql(s"CREATE or REPLACE TEMPORARY  FILE  FORMAT $formatName TYPE  = JSON").collect()
    val df3 = session.table(tableName).select(to_variant(col("c1")))
    runJsonTestAsync(
      df3,
      path,
      Map("FORMAT_NAME" -> formatName),
      Array(Row(2, 4, 24)),
      ".json.gz",
      Some(SaveMode.Overwrite))
    session.read.json(path).show()
    checkAnswer(session.read.json(path), Seq(Row("1"), Row("2")))

    // write with format_name format and some extra option
    session.sql(s"rm $path").collect()
    val df4 = session.table(tableName).select(to_variant(col("c1")))
    runJsonTestAsync(
      df4,
      path,
      Map(
        "FORMAT_NAME" -> formatName,
        "FILE_EXTENSION" -> "myjson.json",
        "COMPRESSION" -> "NONE"),
      Array(Row(2, 4, 4)),
      ".myjson.json",
      Some(SaveMode.Overwrite))
    session.read.json(path).show()
    checkAnswer(session.read.json(path), Seq(Row("1"), Row("2")))
  }

  // Copy DataFrameWriterSuiter.test("write PARQUET files: file format options") and
  // modify it to run it in async mode
  test("async: write PARQUET files: file format options") {
    createTable(tableName, "c1 int, c2 string")
    runQuery(s"insert into $tableName values (1,'one'),(2,'two')", session)
    val df = session.table(tableName)
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    // write by default
    runParquetTest(df, path, Map.empty, 2, ".parquet")
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))

    // write with overwrite
    runParquetTest(df, path, Map.empty, 2, ".snappy.parquet", Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))

    // write with format_name
    val formatName = randomTableName()
    session
      .sql(s"CREATE or REPLACE TEMPORARY  FILE  FORMAT $formatName TYPE  = PARQUET")
      .collect()
    runParquetTest(
      df,
      path,
      Map("FORMAT_NAME" -> formatName),
      2,
      ".snappy.parquet",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))

    // write with format_name format and some extra option
    session.sql(s"rm $path").collect()
    val df4 = session.table(tableName).select(to_variant(col("c1")))
    runParquetTest(
      df,
      path,
      Map("FORMAT_NAME" -> formatName, "COMPRESSION" -> "LZO"),
      2,
      ".lzo.parquet",
      Some(SaveMode.Overwrite))
    checkAnswer(
      session.read.parquet(path),
      Seq(
        Row("{\n  \"_COL_0\": 1,\n  \"_COL_1\": \"one\"\n}"),
        Row("{\n  \"_COL_0\": 2,\n  \"_COL_1\": \"two\"\n}")))
  }

  // Copy DataFrameWriterSuiter.test("Catch COPY INTO LOCATION output schema change") and
  // modify it to run it in async mode
  test("async: Catch COPY INTO LOCATION output schema change") {
    // This test case catches the future potential COPY INTO LOCATION output schema change
    // If it is failed, we need report with COPY team to provide a workaround for customer.
    // NOTE: The server side change may affect old SnowPark versions. This test case
    // will be run in jenkins test for preprod, so we can catch it before deployed to prod.
    val df1 = session.sql("select 123, 'abc'")
    val path = s"@$targetStageName/p_${Random.nextInt().abs}"

    val copySchema = df1.write.async.csv(path).getResult().schema
    assert(copySchema.size == 3)
    val field0 = copySchema.fields(0)
    assert(field0.name.equals("\"rows_unloaded\"") && field0.dataType == LongType)
    val field1 = copySchema.fields(1)
    assert(field1.name.equals("\"input_bytes\"") && field1.dataType == LongType)
    val field2 = copySchema.fields(2)
    assert(field2.name.equals("\"output_bytes\"") && field2.dataType == LongType)
  }
}
