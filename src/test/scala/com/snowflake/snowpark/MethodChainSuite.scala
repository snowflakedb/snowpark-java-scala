package com.snowflake.snowpark

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.udtf.UDTF1

import scala.collection.mutable

class MethodChainSuite extends TestData {
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
      "toDF"
    )
  }

  test("sort") {
    checkMethodChain(
      df1
        .sort(col("a"))
        .sort(Seq(col("a")))
        .sort(Array(col("a"))),
      "sort",
      "sort",
      "sort"
    )
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
      "select"
    )
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
      "groupByGroupingSets.count"
    )
    checkMethodChain(
      df1
        .groupByGroupingSets(Seq(GroupingSets(Set(col("a")))))
        .builtin("count")(col("a")),
      "groupByGroupingSets.builtin"
    )
  }

  test("cube") {
    checkMethodChain(df1.cube(col("a")).count(), "cube.count")
    checkMethodChain(df1.cube(Seq(col("a"))).count(), "cube.count")
    checkMethodChain(df1.cube(Array(col("a"))).count(), "cube.count")
    checkMethodChain(df1.cube("a").count(), "cube.count")
    checkMethodChain(df1.cube(Seq("a")).count(), "cube.count")
  }

  test("distinct") {
    checkMethodChain(df1.distinct(), "distinct")
  }

  test("dropDuplicates") {
    checkMethodChain(df1.dropDuplicates(), "dropDuplicates")
  }

  test("pivot") {
    checkMethodChain(df1.pivot(col("a"), Seq(1, 2, 3)).count(), "pivot.count")
    checkMethodChain(df1.pivot("a", Seq(1, 2, 3)).count(), "pivot.count")
  }

  test("limit") {
    checkMethodChain(df1.limit(1), "limit")
  }

  test("union") {
    checkMethodChain(df1.union(df1.clone), "union")
    checkMethodChain(df1.unionAll(df1.clone), "unionAll")
    checkMethodChain(df1.unionByName(df1.clone), "unionByName")
    checkMethodChain(df1.unionAllByName(df1.clone), "unionAllByName")
  }

  test("intersect") {
    checkMethodChain(df1.intersect(df1.clone), "intersect")
  }

  test("except") {
    checkMethodChain(df1.except(df1.clone), "except")
  }

  test("join") {
    val df2 = df1.clone
    checkMethodChain(df1.join(df2), "join")
    checkMethodChain(df1.join(df2, "a"), "join")
    checkMethodChain(df1.join(df2, Seq("a", "b")), "join")
    checkMethodChain(df1.join(df2, Seq("a", "b"), "inner"), "join")
    checkMethodChain(df1.join(df2, df1("a") === df2("a")), "join")
    checkMethodChain(df1.join(df2, df1("a") === df2("a"), "inner"), "join")
  }

  test("join 2") {
    import com.snowflake.snowpark.tableFunctions._
    val df2 = session.sql("select * from values('1,2,3') as T(a)")
    checkMethodChain(df2.join(split_to_table, df2("a"), lit(",")), "join")
    checkMethodChain(df2.join(split_to_table, Seq(df2("a"), lit(","))), "join")
    checkMethodChain(df2.join(split_to_table, Seq(df2("a"), lit(","))), "join")

    val TableFunc1 = new UDTF1[String] {

      private val freq = new mutable.HashMap[String, Int]

      override def process(colValue: String): Iterable[Row] = {
        val curValue = freq.getOrElse(colValue, 0)
        freq.put(colValue, curValue + 1)
        mutable.Iterable.empty
      }

      override def outputSchema(): StructType =
        StructType(StructField("FREQUENCIES", StringType))

      override def endPartition(): Iterable[Row] = {
        Seq(Row(freq.toString()))
      }
    }
    import session.implicits._
    val df = Seq(("a", "b"), ("a", "c"), ("a", "b"), ("d", "e")).toDF("a", "b")
    val tf = session.udtf.registerTemporary(TableFunc1)
    checkMethodChain(
      df.join(tf, Seq(df("b")), Seq(df("a")), Seq(df("b"))),
      "select",
      "toDF",
      "join"
    )
    checkMethodChain(
      df.join(tf, Map("arg1" -> df("b")), Seq(df("a")), Seq(df("b"))),
      "select",
      "toDF",
      "join"
    )
    checkMethodChain(
      df.join(tf(Map("arg1" -> df("b"))), Seq(df("a")), Seq(df("b"))),
      "select",
      "toDF",
      "join"
    )

    val df3 = session.sql("select * from values('[1,2,3]') as T(a)")
    checkMethodChain(df3.join(flatten, Map("input" -> parse_json(df("a")))), "join")

    checkMethodChain(df3.join(flatten(parse_json(df("a")))), "join")
  }

  test("join 3") {
    checkMethodChain(df1.crossJoin(df1.clone), "crossJoin")
    checkMethodChain(df1.naturalJoin(df1.clone), "naturalJoin")
    checkMethodChain(df1.naturalJoin(df1.clone, "left"), "naturalJoin")
  }

  test("withColumn") {
    checkMethodChain(df1.withColumn("a1", lit(1)), "withColumn")
    checkMethodChain(df1.withColumns(Seq("a1"), Seq(lit(1))), "withColumns")
  }

  test("rename") {
    checkMethodChain(df1.rename("a1", col("a")), "rename")
  }

  test("sample") {
    checkMethodChain(df1.sample(1), "sample")
    checkMethodChain(df1.sample(0.5), "sample")
  }

  test("na") {
    checkMethodChain(double3.na.drop(1, Seq("a")), "na", "drop")
    checkMethodChain(
      nullData3.na.fill(Map("flo" -> 12.3, "int" -> 11, "boo" -> false, "str" -> "f")),
      "na",
      "fill"
    )
    checkMethodChain(nullData3.na.replace("flo", Map(2 -> 300, 1 -> 200)), "na", "replace")
  }

  test("stat") {
    checkMethodChain(df1.stat.sampleBy(col("a"), Map(1 -> 0.0, 2 -> 1.0)), "stat", "sampleBy")
    checkMethodChain(df1.stat.sampleBy("a", Map(1 -> 0.0, 2 -> 1.0)), "stat", "sampleBy")
  }

  test("flatten") {
    val table1 = session.sql("select parse_json(value) as value from values('[1,2]') as T(value)")
    checkMethodChain(table1.flatten(table1("value")), "flatten")
    checkMethodChain(
      table1.flatten(table1("value"), "", outer = false, recursive = false, "both"),
      "flatten"
    )
  }
}
