package com.snowflake.snowpark

import com.snowflake.snowpark.functions._

class MethodChainSuite extends SNTestBase {
  private val df1 = session.sql("select * from values(1,2,3) as T(a, b, c)")

  private def checkMethodChain(df: DataFrame, methodNames: String*): Unit = {
    val methodChain = df.methodChain
    assert(methodChain == methodNames)
  }

  test("new dataframe") {
    checkMethodChain(df1)
  }

  test("clone") {
    checkMethodChain(df1.clone, "clone")
  }

  test("toDF") {
    checkMethodChain(
      df1
        .toDF("a1", "b1", "c1")
        .toDF(Seq("a2", "b2", "c2"))
        .toDF(Array("a3", "b3", "c3")),
      "toDF",
      "toDF",
      "toDF")
  }

  test("sort") {
    checkMethodChain(
      df1
        .sort(col("a"))
        .sort(Seq(col("a")))
        .sort(Array(col("a"))),
      "sort",
      "sort",
      "sort")
  }

  test("alias") {
    checkMethodChain(df1.alias("a"), "alias")
  }

  test("select") {
    checkMethodChain(
      df1
        .select(col("a"))
        .select(Seq(col("a")))
        .select(Array(col("a")))
        .select("a")
        .select(Seq("a"))
        .select(Array("a")),
      "select",
      "select",
      "select",
      "select",
      "select",
      "select")
  }

  test("drop") {
    checkMethodChain(df1.drop("a").drop(Seq("b")), "drop", "drop")
    checkMethodChain(df1.drop(Array("a")).drop(col("b")), "drop", "drop")
    checkMethodChain(df1.drop(Seq(col("a"))).drop(Array(col("b"))), "drop", "drop")
  }

  test("filter") {
    checkMethodChain(df1.filter(col("a") < 0), "filter")
  }

  test("where") {
    checkMethodChain(df1.where(col("a") < 0), "where")
  }

  test("agg") {
    checkMethodChain(df1.agg("a" -> "max"), "agg")
    checkMethodChain(df1.agg(Seq("a" -> "max")), "agg")
    checkMethodChain(df1.agg(max(col("a"))), "agg")
    checkMethodChain(df1.agg(Seq(max(col("a")))), "agg")
    checkMethodChain(df1.agg(Array(max(col("a")))), "agg")
  }

  test("rollup.agg") {
    checkMethodChain(df1.rollup(col("a")).agg(col("a") -> "max"), "rollup.agg")
    checkMethodChain(df1.rollup(Seq(col("a"))).agg(Seq(col("a") -> "max")), "rollup.agg")
    checkMethodChain(df1.rollup(Array(col("a"))).agg(max(col("a"))), "rollup.agg")
    checkMethodChain(df1.rollup("a").agg(Seq(max(col("a")))), "rollup.agg")
    checkMethodChain(df1.rollup(Seq("a")).agg(Array(max(col("a")))), "rollup.agg")
    checkMethodChain(df1.rollup(Array("a")).agg(Map(col("a") -> "max")), "rollup.agg")
  }
}
