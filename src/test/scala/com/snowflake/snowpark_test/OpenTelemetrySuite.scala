package com.snowflake.snowpark_test

import com.snowflake.snowpark.OpenTelemetryEnabled
import com.snowflake.snowpark.internal.OpenTelemetry

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

  test("OpenTelemetry.emit") {
    OpenTelemetry.emit("ClassA", "functionB", "fileC", 123, "chainD")
    checkSpan("snow.snowpark.ClassA", "functionB", "fileC", 123, "chainD")
  }

  test("report error") {
    val error = new Exception("test")
    OpenTelemetry.reportError("ClassA1", "functionB1", error)
    checkSpanError("snow.snowpark.ClassA1", "functionB1", error)
  }
}
