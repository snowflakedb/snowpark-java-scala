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
}
