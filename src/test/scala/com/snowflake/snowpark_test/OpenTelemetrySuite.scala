package com.snowflake.snowpark_test

import com.snowflake.snowpark.{OpenTelemetryEnabled, SaveMode, UpdateResult}
import com.snowflake.snowpark.internal.OpenTelemetry
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import java.util

class OpenTelemetrySuite extends OpenTelemetryEnabled {
  test("line number - collect") {
    session.sql("select 1").collect()
    checkSpan("snow.snowpark.DataFrame", "collect", "")
  }

  test("line number - randomSplit") {
    session.sql("select * from values(1),(2),(3) as t(num)").randomSplit(Array(0.5, 0.5))
    checkSpan("snow.snowpark.DataFrame", "randomSplit", "")
  }

  test("line number - first") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.first()
    checkSpan("snow.snowpark.DataFrame", "first", "")
    df.first(2)
    checkSpan("snow.snowpark.DataFrame", "first", "")
  }

  test("line number - cacheResult") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.cacheResult()
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "")
  }

  test("line number - toLocalIterator") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.toLocalIterator
    checkSpan("snow.snowpark.DataFrame", "toLocalIterator", "")
  }

  test("line number - count") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.count()
    checkSpan("snow.snowpark.DataFrame", "count", "")
  }

  test("line number - show") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.show()
    checkSpan("snow.snowpark.DataFrame", "show", "")
    df.show(1)
    checkSpan("snow.snowpark.DataFrame", "show", "")
    df.show(1, 10)
    checkSpan("snow.snowpark.DataFrame", "show", "")
  }

  test("line number - createOrReplaceView") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val name = randomName()
    try {
      df.createOrReplaceView(name)
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceView", "")
    } finally {
      dropView(name)
    }
    try {
      df.createOrReplaceView(Seq(name))
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceView", "")
    } finally {
      dropView(name)
    }

    try {
      val list: java.util.List[String] = new util.ArrayList[String](1)
      list.add(name)
      df.createOrReplaceView(list)
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceView", "")
    } finally {
      dropView(name)
    }
  }

  test("line number - createOrReplaceTempView") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val name = randomName()
    try {
      df.createOrReplaceTempView(name)
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceTempView", "")
    } finally {
      dropView(name)
    }
    try {
      df.createOrReplaceTempView(Seq(name))
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceTempView", "")
    } finally {
      dropView(name)
    }

    try {
      val list: java.util.List[String] = new util.ArrayList[String](1)
      list.add(name)
      df.createOrReplaceTempView(list)
      checkSpan("snow.snowpark.DataFrame", "createOrReplaceTempView", "")
    } finally {
      dropView(name)
    }
  }

  test("line number - HasCachedResult") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val cached = df.cacheResult()
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "")
    cached.cacheResult()
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "")
  }

  test("line number - DataFrameAsyncActor") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.async.count()
    checkSpan("snow.snowpark.DataFrameAsyncActor", "count", "")
    df.async.collect()
    checkSpan("snow.snowpark.DataFrameAsyncActor", "collect", "")
    df.async.toLocalIterator()
    checkSpan("snow.snowpark.DataFrameAsyncActor", "toLocalIterator", "")
  }

  test("line number - DataFrameStatFunctions - corr") {
    import session.implicits._
    val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    df.stat.corr("a", "b")
    checkSpan("snow.snowpark.DataFrameStatFunctions", "corr", "")
  }

  test("line number - DataFrameStatFunctions - cov") {
    import session.implicits._
    val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    df.stat.cov("a", "b")
    checkSpan("snow.snowpark.DataFrameStatFunctions", "cov", "")
  }

  test("line number - DataFrameStatFunctions - approxQuantile") {
    import session.implicits._
    val df = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 0).toDF("a")
    df.stat.approxQuantile("a", Array(0, 0.1, 0.4, 0.6, 1))
    checkSpan("snow.snowpark.DataFrameStatFunctions", "approxQuantile", "")
  }

  test("line number - DataFrameStatFunctions - approxQuantile 2") {
    import session.implicits._
    val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    df.stat.approxQuantile(Array("a", "b"), Array(0, 0.1, 0.6))
    checkSpan("snow.snowpark.DataFrameStatFunctions", "approxQuantile", "")
  }

  test("line number - DataFrameStatFunctions - crosstab") {
    import session.implicits._
    val df = Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3)).toDF("key", "value")
    df.stat.crosstab("key", "value")
    checkSpan("snow.snowpark.DataFrameStatFunctions", "crosstab", "")
  }

  test("line number - DataFrameWriter - csv") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.write.csv(s"@$stageName1/csv1")
    checkSpan("snow.snowpark.DataFrameWriter", "csv", "")
  }

  test("line number - DataFrameWriter - json") {
    import session.implicits._
    val df = Seq((1, 1.1, "a"), (2, 2.2, "b")).toDF("a", "b", "c")
    val df2 = df.select(array_construct(df.schema.names.map(df(_)): _*))
    df2.write.option("compression", "none").json(s"@$stageName1/json1")
    checkSpan("snow.snowpark.DataFrameWriter", "json", "")
  }

  test("line number - DataFrameWriter - parquet") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.write.parquet(s"@$stageName1/parquet1")
    checkSpan("snow.snowpark.DataFrameWriter", "parquet", "")
  }

  test("line number - DataFrameWriter - saveAsTable") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val tableName = randomName()
    try {
      df.write.saveAsTable(tableName)
      checkSpan("snow.snowpark.DataFrameWriter", "saveAsTable", "")
    } finally {
      dropTable(tableName)
    }
    try {
      df.write.saveAsTable(Seq(tableName))
      checkSpan("snow.snowpark.DataFrameWriter", "saveAsTable", "")
    } finally {
      dropTable(tableName)
    }
    try {
      val list = new util.ArrayList[String](1)
      list.add(tableName)
      df.write.saveAsTable(tableName)
      checkSpan("snow.snowpark.DataFrameWriter", "saveAsTable", "")
    } finally {
      dropTable(tableName)
    }
  }

  test("line number - DataFrameWriterAsyncActor - saveAsTable") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val tableName = randomName()
    try {
      df.write.async.saveAsTable(tableName).getResult()
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "saveAsTable", "")
    } finally {
      dropTable(tableName)
    }
    try {
      df.write.async.saveAsTable(Seq(tableName)).getResult()
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "saveAsTable", "")
    } finally {
      dropTable(tableName)
    }
    try {
      val list = new util.ArrayList[String](1)
      list.add(tableName)
      df.write.async.saveAsTable(tableName).getResult()
      checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "saveAsTable", "")
    } finally {
      dropTable(tableName)
    }
  }

  test("line number - DataFrameWriterAsyncActor - csv") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.write.async.csv(s"@$stageName1/csv2").getResult()
    checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "csv", "")
  }

  test("line number - DataFrameWriterAsyncActor - json") {
    import session.implicits._
    val df = Seq((1, 1.1, "a"), (2, 2.2, "b")).toDF("a", "b", "c")
    val df2 = df.select(array_construct(df.schema.names.map(df(_)): _*))
    df2.write.option("compression", "none").async.json(s"@$stageName1/json2")
    checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "json", "")
  }

  test("line number - DataFrameWriterAsyncActor - parquet") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.write.async.parquet(s"@$stageName1/parquet2")
    checkSpan("snow.snowpark.DataFrameWriterAsyncActor", "parquet", "")
  }

  test("line number - CopyableDataFrame") {
    val stageName = randomName()
    val tableName = randomName()
    val userSchema: StructType = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", DoubleType)))
    try {
      createStage(stageName)
      uploadFileToStage(stageName, testFileCsv, compress = false)
      createTable(tableName, "a Int, b String, c Double")
      val testFileOnStage = s"@$stageName/$testFileCsv"
      testSpanExporter.reset()
      val df = session.read.schema(userSchema).csv(testFileOnStage)
      df.copyInto(tableName)
      checkSpan("snow.snowpark.CopyableDataFrame", "copyInto", "")
      df.copyInto(tableName, Seq(col("$1"), col("$2"), col("$3")))
      checkSpan("snow.snowpark.CopyableDataFrame", "copyInto", "")
      df.copyInto(tableName, Seq(col("$1"), col("$2"), col("$3")), Map("FORCE" -> "TRUE"))
      checkSpan("snow.snowpark.CopyableDataFrame", "copyInto", "")
      df.copyInto(tableName, Seq("a", "b", "c"), Seq(col("$1"), col("$2"), col("$3")), Map.empty)
      checkSpan("snow.snowpark.CopyableDataFrame", "copyInto", "")
      df.clone()
      checkSpan("snow.snowpark.CopyableDataFrame", "clone", "")
    } finally {
      dropStage(stageName)
      dropTable(tableName)
    }
  }

  test("line number - CopyableDataFrameAsyncActor") {
    val stageName = randomName()
    val tableName = randomName()
    val userSchema: StructType = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", DoubleType)))
    try {
      createStage(stageName)
      uploadFileToStage(stageName, testFileCsv, compress = false)
      createTable(tableName, "a Int, b String, c Double")
      val testFileOnStage = s"@$stageName/$testFileCsv"
      testSpanExporter.reset()
      val df = session.read.schema(userSchema).csv(testFileOnStage)
      df.async.copyInto(tableName).getResult()
      checkSpan("snow.snowpark.CopyableDataFrameAsyncActor", "copyInto", "")
      df.async.copyInto(tableName, Seq(col("$1"), col("$2"), col("$3"))).getResult()
      checkSpan("snow.snowpark.CopyableDataFrameAsyncActor", "copyInto", "")
      val seq1 = Seq(col("$1"), col("$2"), col("$3"))
      df.async.copyInto(tableName, seq1, Map("FORCE" -> "TRUE")).getResult()
      checkSpan("snow.snowpark.CopyableDataFrameAsyncActor", "copyInto", "")
      df.async.copyInto(tableName, Seq("a", "b", "c"), seq1, Map.empty).getResult()
      checkSpan("snow.snowpark.CopyableDataFrameAsyncActor", "copyInto", "")
    } finally {
      dropStage(stageName)
      dropTable(tableName)
    }
  }

  test("line number - updatable") {
    val tableName = randomName()
    val tableName2 = randomName()
    try {
      testData2.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
      val updatable = session.table(tableName)
      upperCaseData.write.mode(SaveMode.Overwrite).saveAsTable(tableName2)
      val t2 = session.table(tableName2)
      testSpanExporter.reset()
      updatable.update(Map(col("a") -> lit(1), col("b") -> lit(0)))
      checkSpan("snow.snowpark.Updatable", "update", "")
      updatable.update(Map("b" -> (col("a") + col("b"))))
      checkSpan("snow.snowpark.Updatable", "update", "")
      updatable.update(Map(col("b") -> lit(0)), col("a") === 1)
      checkSpan("snow.snowpark.Updatable", "update", "")
      updatable.update(Map("b" -> lit(0)), col("a") === 1)
      checkSpan("snow.snowpark.Updatable", "update", "")
      t2.update(Map(col("n") -> lit(0)), updatable("a") === t2("n"), updatable)
      checkSpan("snow.snowpark.Updatable", "update", "")
      t2.update(Map("n" -> lit(0)), updatable("a") === t2("n"), updatable)
      checkSpan("snow.snowpark.Updatable", "update", "")
    } finally {
      dropTable(tableName)
      dropTable(tableName2)
    }
  }

  test("OpenTelemetry.emit") {
    OpenTelemetry.emit("ClassA", "functionB", "fileC", 123, "chainD")
    checkSpan("snow.snowpark.ClassA", "functionB", "fileC", 123, "chainD")
  }

  test("report error") {
    val error = new Exception("test")
    OpenTelemetry.reportError("ClassA1", "functionB1", error)
    checkSpanError("snow.snowpark.ClassA1", "functionB1", error)
  }

  override def beforeAll: Unit = {
    super.beforeAll
    createStage(stageName1)
  }

  override def afterAll: Unit = {
    dropStage(stageName1)
    super.afterAll
  }

  private val stageName1 = randomName()

  def checkSpan(className: String, funcName: String, methodChain: String): Unit = {
    val stack = Thread.currentThread().getStackTrace
    val file = stack(2) // this file
    checkSpan(
      className,
      funcName,
      "OpenTelemetrySuite.scala",
      file.getLineNumber - 1,
      methodChain)
  }
}
