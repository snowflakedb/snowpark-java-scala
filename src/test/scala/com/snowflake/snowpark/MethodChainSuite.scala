package com.snowflake.snowpark

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.internal.analyzer.Literal

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

  test("groupBy") {
    checkMethodChain(df1.groupBy(col("a")).avg(col("a")), "groupBy.avg")
    checkMethodChain(df1.groupBy().mean(col("a")), "groupBy.mean")
    checkMethodChain(df1.groupBy(Seq(col("a"))).sum(col("a")), "groupBy.sum")
    checkMethodChain(df1.groupBy(Array(col("a"))).median(col("a")), "groupBy.median")
    checkMethodChain(df1.groupBy("a").min(col("a")), "groupBy.min")
    checkMethodChain(df1.groupBy(Seq("a")).max(col("a")), "groupBy.max")
    checkMethodChain(df1.groupBy(Array("a")).any_value(col("a")), "groupBy.any_value")
  }

  test("groupByGroupingSets") {
    checkMethodChain(
      df1.groupByGroupingSets(GroupingSets(Set(col("a")))).count(),
      "groupByGroupingSets.count")
    checkMethodChain(
      df1
        .groupByGroupingSets(Seq(GroupingSets(Set(col("a")))))
        .builtin("count")(col("a")),
      "groupByGroupingSets.builtin")
  }
}
