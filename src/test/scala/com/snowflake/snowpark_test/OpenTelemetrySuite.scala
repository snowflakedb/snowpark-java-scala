package com.snowflake.snowpark_test

import com.snowflake.snowpark.OpenTelemetryEnabled
import com.snowflake.snowpark.internal.OpenTelemetry
import com.snowflake.snowpark.functions._
import java.util

class OpenTelemetrySuite extends OpenTelemetryEnabled {
  // do not add test before line number tests
  // it verifies code line numbers
  test("line number - collect") {
    session.sql("select 1").collect()
    checkSpan("snow.snowpark.DataFrame", "collect", "OpenTelemetrySuite.scala", 12, "")
  }

  test("line number - randomSplit") {
    session.sql("select * from values(1),(2),(3) as t(num)").randomSplit(Array(0.5, 0.5))
    checkSpan("snow.snowpark.DataFrame", "randomSplit", "OpenTelemetrySuite.scala", 17, "")
  }

  test("line number - first") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.first()
    checkSpan("snow.snowpark.DataFrame", "first", "OpenTelemetrySuite.scala", 23, "")
    df.first(2)
    checkSpan("snow.snowpark.DataFrame", "first", "OpenTelemetrySuite.scala", 25, "")
  }

  test("line number - cacheResult") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.cacheResult()
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "OpenTelemetrySuite.scala", 31, "")
  }

  test("line number - toLocalIterator") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.toLocalIterator
    checkSpan("snow.snowpark.DataFrame", "toLocalIterator", "OpenTelemetrySuite.scala", 37, "")
  }

  test("line number - count") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.count()
    checkSpan("snow.snowpark.DataFrame", "count", "OpenTelemetrySuite.scala", 43, "")
  }

  test("line number - show") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.show()
    checkSpan("snow.snowpark.DataFrame", "show", "OpenTelemetrySuite.scala", 49, "")
    df.show(1)
    checkSpan("snow.snowpark.DataFrame", "show", "OpenTelemetrySuite.scala", 51, "")
    df.show(1, 10)
    checkSpan("snow.snowpark.DataFrame", "show", "OpenTelemetrySuite.scala", 53, "")
  }

  test("line number - createOrReplaceView") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val name = randomName()
    try {
      df.createOrReplaceView(name)
      checkSpan(
        "snow.snowpark.DataFrame",
        "createOrReplaceView",
        "OpenTelemetrySuite.scala",
        61,
        "")
    } finally {
      dropView(name)
    }
    try {
      df.createOrReplaceView(Seq(name))
      checkSpan(
        "snow.snowpark.DataFrame",
        "createOrReplaceView",
        "OpenTelemetrySuite.scala",
        72,
        "")
    } finally {
      dropView(name)
    }

    try {
      val list: java.util.List[String] = new util.ArrayList[String](1)
      list.add(name)
      df.createOrReplaceView(list)
      checkSpan(
        "snow.snowpark.DataFrame",
        "createOrReplaceView",
        "OpenTelemetrySuite.scala",
        86,
        "")
    } finally {
      dropView(name)
    }
  }

  test("line number - createOrReplaceTempView") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val name = randomName()
    try {
      df.createOrReplaceTempView(name)
      checkSpan(
        "snow.snowpark.DataFrame",
        "createOrReplaceTempView",
        "OpenTelemetrySuite.scala",
        102,
        "")
    } finally {
      dropView(name)
    }
    try {
      df.createOrReplaceTempView(Seq(name))
      checkSpan(
        "snow.snowpark.DataFrame",
        "createOrReplaceTempView",
        "OpenTelemetrySuite.scala",
        113,
        "")
    } finally {
      dropView(name)
    }

    try {
      val list: java.util.List[String] = new util.ArrayList[String](1)
      list.add(name)
      df.createOrReplaceTempView(list)
      checkSpan(
        "snow.snowpark.DataFrame",
        "createOrReplaceTempView",
        "OpenTelemetrySuite.scala",
        127,
        "")
    } finally {
      dropView(name)
    }
  }

  test("line number - HasCachedResult") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    val cached = df.cacheResult()
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "OpenTelemetrySuite.scala", 141, "")
    cached.cacheResult()
    checkSpan("snow.snowpark.DataFrame", "cacheResult", "OpenTelemetrySuite.scala", 143, "")
  }

  test("line number - DataFrameAsyncActor") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.async.count()
    checkSpan("snow.snowpark.DataFrameAsyncActor", "count", "OpenTelemetrySuite.scala", 149, "")
    df.async.collect()
    checkSpan("snow.snowpark.DataFrameAsyncActor", "collect", "OpenTelemetrySuite.scala", 151, "")
    df.async.toLocalIterator()
    checkSpan(
      "snow.snowpark.DataFrameAsyncActor",
      "toLocalIterator",
      "OpenTelemetrySuite.scala",
      153,
      "")
  }

  test("line number - DataFrameStatFunctions - corr") {
    import session.implicits._
    val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    df.stat.corr("a", "b")
    checkSpan("snow.snowpark.DataFrameStatFunctions", "corr", "OpenTelemetrySuite.scala", 165, "")
  }

  test("line number - DataFrameStatFunctions - cov") {
    import session.implicits._
    val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    df.stat.cov("a", "b")
    checkSpan("snow.snowpark.DataFrameStatFunctions", "cov", "OpenTelemetrySuite.scala", 172, "")
  }

  test("line number - DataFrameStatFunctions - approxQuantile") {
    import session.implicits._
    val df = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 0).toDF("a")
    df.stat.approxQuantile("a", Array(0, 0.1, 0.4, 0.6, 1))
    checkSpan(
      "snow.snowpark.DataFrameStatFunctions",
      "approxQuantile",
      "OpenTelemetrySuite.scala",
      179,
      "")
  }

  test("line number - DataFrameStatFunctions - approxQuantile 2") {
    import session.implicits._
    val df = Seq((0.1, 0.5), (0.2, 0.6), (0.3, 0.7)).toDF("a", "b")
    df.stat.approxQuantile(Array("a", "b"), Array(0, 0.1, 0.6))
    checkSpan(
      "snow.snowpark.DataFrameStatFunctions",
      "approxQuantile",
      "OpenTelemetrySuite.scala",
      191,
      "")
  }

  test("line number - DataFrameStatFunctions - crosstab") {
    import session.implicits._
    val df = Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3)).toDF("key", "value")
    df.stat.crosstab("key", "value")
    checkSpan(
      "snow.snowpark.DataFrameStatFunctions",
      "crosstab",
      "OpenTelemetrySuite.scala",
      203,
      "")
  }

  test("line number - DataFrameWriter - csv") {
    val df = session.sql("select * from values(1),(2),(3) as t(num)")
    df.write.csv(s"@$stageName1/csv1")
    checkSpan("snow.snowpark.DataFrameWriter", "csv", "OpenTelemetrySuite.scala", 214, "")
  }

  test("line number - DataFrameWriter - json") {
    import session.implicits._
    val df = Seq((1, 1.1, "a"), (2, 2.2, "b")).toDF("a", "b", "c")
    val df2 = df.select(array_construct(df.schema.names.map(df(_)): _*))
    df2.write.option("compression", "none").json(s"@$stageName1/json1")
    checkSpan("snow.snowpark.DataFrameWriter", "json", "OpenTelemetrySuite.scala", 222, "")
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
}
