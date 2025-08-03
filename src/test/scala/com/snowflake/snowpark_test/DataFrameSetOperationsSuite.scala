package com.snowflake.snowpark_test

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.IntegerType
import com.snowflake.snowpark.{Column, Row, SnowparkClientException, TestData}
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.sql.{Date, Timestamp}

class DataFrameSetOperationsSuite extends TestData {
  import session.implicits._

  test("Union with filters") {
    def check(newCol: Column, filter: Column, result: Seq[Row]): Unit = {
      val df1 = session.createDataFrame(Seq((1, 1))).toDF("a", "b").withColumn("c", newCol)

      val df2 = df1.union(df1).withColumn("d", lit(100)).filter(filter)
      checkAnswer(df2, result)
    }

    check(lit(null).cast(IntegerType), $"c".is_null, Seq(Row(1, 1, null, 100)))
    check(lit(null).cast(IntegerType), $"c".is_not_null, Seq())
    check(lit(2).cast(IntegerType), $"c".is_null, Seq())
    check(lit(2).cast(IntegerType), $"c".is_not_null, Seq(Row(1, 1, 2, 100)))
    check(lit(2).cast(IntegerType), $"c" === 2, Seq(Row(1, 1, 2, 100)))
    check(lit(2).cast(IntegerType), $"c" =!= 2, Seq())
  }

  test("Union All with filters") {
    def check(newCol: Column, filter: Column, result: Seq[Row]): Unit = {
      val df1 = session.createDataFrame(Seq((1, 1))).toDF("a", "b").withColumn("c", newCol)

      val df2 = df1.unionAll(df1).withColumn("d", lit(100)).filter(filter)
      checkAnswer(df2, result)
    }

    check(
      lit(null).cast(IntegerType),
      $"c".is_null,
      Seq(Row(1, 1, null, 100), Row(1, 1, null, 100)))
    check(lit(null).cast(IntegerType), $"c".is_not_null, Seq())
    check(lit(2).cast(IntegerType), $"c".is_null, Seq())
    check(lit(2).cast(IntegerType), $"c".is_not_null, Seq(Row(1, 1, 2, 100), Row(1, 1, 2, 100)))
    check(lit(2).cast(IntegerType), $"c" === 2, Seq(Row(1, 1, 2, 100), Row(1, 1, 2, 100)))
    check(lit(2).cast(IntegerType), $"c" =!= 2, Seq())
  }

  test("except") {
    checkAnswer(
      lowerCaseData.except(upperCaseData),
      Row(1, "a") ::
        Row(2, "b") ::
        Row(3, "c") ::
        Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.except(lowerCaseData), Nil)
    checkAnswer(upperCaseData.except(upperCaseData), Nil)

    // check null equality
    checkAnswer(nullInts.except(nullInts.filter(lit(0) === 1)), nullInts.collect())
    checkAnswer(nullInts.except(nullInts), Nil)

    // check if values are de-duplicated
    checkAnswer(allNulls.except(allNulls.filter(lit(0) === 1)), Row(null) :: Nil)
    checkAnswer(allNulls.except(allNulls), Nil)

    // check if values are de-duplicated
    val df = Seq(("id1", 1), ("id1", 1), ("id", 1), ("id1", 2)).toDF("id", "value")
    checkAnswer(
      df.except(df.filter(lit(0) === 1)),
      Row("id", 1) ::
        Row("id1", 1) ::
        Row("id1", 2) :: Nil)

    // check if the empty set on the left side works
    checkAnswer(allNulls.filter(lit(0) === 1).except(allNulls), Nil)
  }

  test("except between two projects without references used in filter") {
    val df = Seq((1, 2, 4), (1, 3, 5), (2, 2, 3), (2, 4, 5)).toDF("a", "b", "c")
    val df1 = df.filter($"a" === 1)
    val df2 = df.filter($"a" === 2)
    checkAnswer(df1.select("b").except(df2.select("b")), Row(3) :: Nil)
    checkAnswer(df1.select("b").except(df2.select("c")), Row(2) :: Nil)
  }

  test("test union/unionAll/unionByName/unionAllByName in one case") {
    val df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    val df2 = Seq((3, 1, 2)).toDF("c", "a", "b")
    val df3 = Seq((1, 2, 3)).toDF("b", "c", "a")

    checkAnswer(df1.union(df2), Row(1, 2, 3) :: Row(3, 1, 2) :: Nil)
    checkAnswer(df1.unionAll(df2), Row(1, 2, 3) :: Row(3, 1, 2) :: Nil)
    checkAnswer(df1.unionByName(df2), Row(1, 2, 3) :: Nil)
    checkAnswer(df1.unionAllByName(df2), Row(1, 2, 3) :: Row(1, 2, 3) :: Nil)

    checkAnswer(df1.union(df3), Row(1, 2, 3) :: Nil)
    checkAnswer(df1.unionAll(df3), Row(1, 2, 3) :: Row(1, 2, 3) :: Nil)
    checkAnswer(df1.unionByName(df3), Row(1, 2, 3) :: Row(3, 1, 2) :: Nil)
    checkAnswer(df1.unionAllByName(df3), Row(1, 2, 3) :: Row(3, 1, 2) :: Nil)
  }

  /*
   * This test is same as the one in base class with a minor change to remove
   * the RDD code that checks the answer.  Running the same query again
   * tests the same thing.
   */
  test("nondeterministic expressions should not be pushed down") {
    val df1 = (1 to 20).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 10).map(Tuple1.apply).toDF("i")

    // Checks that the random filter is not pushed down and
    // so will return the same result when run again

    val union = df1.union(df2).filter($"i" < random(7))
    assert(union.collect() sameElements union.collect())

    val intersect = df1.intersect(df2).filter($"i" < random(7))
    assert(intersect.collect() sameElements intersect.collect())

    val except = df1.except(df2).filter($"i" < random(7))
    assert(except.collect() sameElements except.collect())
  }
  /*
   * From Snowpark 0.8.0, union() is not an alias of unionAll() any more
   */
  test("SN: union all") {
    val unionDF = testData4
      .union(testData4)
      .union(testData4)
      .union(testData4)
      .union(testData4)

    checkAnswer(unionDF.agg(min($"key"), sum($"key")), Row(1, 5050) :: Nil)

    val unionAllDF = testData4
      .unionAll(testData4)
      .unionAll(testData4)
      .unionAll(testData4)
      .unionAll(testData4)

    checkAnswer(unionAllDF.agg(min($"key"), sum($"key")), Row(1, 25250) :: Nil)
  }
  /*
   * This test is same as the test in base class with two modifications
   * 1. Removed the check that asserts that the analyzed plan is an instance of Union
   * 2. Updated the assertion on exception thrown and error messages
   */
  test("SN: union by name") {
    var df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    var df2 = Seq((3, 1, 2)).toDF("c", "a", "b")
    val df3 = Seq((2, 3, 1)).toDF("b", "c", "a")
    val unionDf = df1.unionByName(df2.unionByName(df3))
    checkAnswer(unionDf, Row(1, 2, 3) :: Nil)

    // Check failure cases
    df1 = Seq((1, 2)).toDF("a", "c")
    df2 = Seq((3, 4, 5)).toDF("a", "b", "c")
    intercept[SnowflakeSQLException] {
      df1.unionByName(df2).collect()
    }

    df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    df2 = Seq((4, 5, 6)).toDF("a", "c", "d")
    intercept[SnowparkClientException] {
      df1.unionByName(df2)
    }
  }

  test("SN: unionAll by name") {
    var df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    var df2 = Seq((3, 1, 2)).toDF("c", "a", "b")
    val df3 = Seq((2, 3, 1)).toDF("b", "c", "a")
    val unionDf = df1.unionAllByName(df2.unionAllByName(df3))
    checkAnswer(unionDf, Row(1, 2, 3) :: Row(1, 2, 3) :: Row(1, 2, 3) :: Nil)

    // Check failure cases
    df1 = Seq((1, 2)).toDF("a", "c")
    df2 = Seq((3, 4, 5)).toDF("a", "b", "c")
    intercept[SnowflakeSQLException] {
      df1.unionAllByName(df2).collect()
    }

    df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    df2 = Seq((4, 5, 6)).toDF("a", "c", "d")
    intercept[SnowparkClientException] {
      df1.unionAllByName(df2)
    }
  }

  test("SN: union by quoted name") {
    var df1 = Seq((1, 2, 3)).toDF(""""a"""", "a", "c")
    var df2 = Seq((3, 1, 2)).toDF("c", """"a"""", "a")
    val df3 = Seq((2, 3, 1)).toDF("a", "c", """"a"""")
    val unionDf = df1.unionByName(df2.unionByName(df3))
    checkAnswer(unionDf, Row(1, 2, 3) :: Nil)

    df1 = Seq((1, 2, 3)).toDF(""""a"""", "b", "c")
    df2 = Seq((4, 5, 6)).toDF("a", "c", "b")
    intercept[SnowparkClientException] {
      df1.unionByName(df2)
    }
  }

  test("SN: unionAll by quoted name") {
    var df1 = Seq((1, 2, 3)).toDF(""""a"""", "a", "c")
    var df2 = Seq((3, 1, 2)).toDF("c", """"a"""", "a")
    val df3 = Seq((2, 3, 1)).toDF("a", "c", """"a"""")
    val unionDf = df1.unionAllByName(df2.unionAllByName(df3))
    checkAnswer(unionDf, Row(1, 2, 3) :: Row(1, 2, 3) :: Row(1, 2, 3) :: Nil)

    df1 = Seq((1, 2, 3)).toDF(""""a"""", "b", "c")
    df2 = Seq((4, 5, 6)).toDF("a", "c", "b")
    intercept[SnowparkClientException] {
      df1.unionAllByName(df2)
    }
  }

  /*
   * This test is the same as the test in base class with one modification:
   * If the left query's column type is NOT nullable but right query's column type is nullable
   * snowflake returns nullable=true for the result of the intersect query.
   */
  test("SN: intersect - nullability") {
    val nonNullableInts = Seq(Tuple1(1), Tuple1(3)).toDF("a")
    assert(nonNullableInts.schema.forall(!_.nullable))

    assert(nullInts.schema.forall(_.nullable))

    val df1 = nonNullableInts.intersect(nullInts)
    checkAnswer(df1, Row(1) :: Row(3) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.intersect(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(3) :: Nil)
    assert(df2.schema.forall(_.nullable))

    val df3 = nullInts.intersect(nullInts)
    checkAnswer(df3, Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.intersect(nonNullableInts)
    checkAnswer(df4, Row(1) :: Row(3) :: Nil)
    assert(df4.schema.forall(!_.nullable))
  }

  test("SN - Performing set operations that combine non-scala native types") {
    val dates = Seq(
      (new Date(0), BigDecimal.valueOf(1), new Timestamp(2)),
      (new Date(3), BigDecimal.valueOf(4), new Timestamp(5))).toDF("date", "decimal", "timestamp")

    val widenTypedRows =
      Seq((new Timestamp(2), 10.5d, (new Timestamp(10)).toString))
        .toDF("date", "decimal", "timestamp")

    dates.union(widenTypedRows).collect()
    dates.except(widenTypedRows).collect()
    dates.intersect(widenTypedRows).collect()
  }

  /*
   * This test is same as the one in base class with a minor change to remove SQL conf
   * settings for case sensitivity since Snowpark does not support it.
   */
  test("SN: union by name - check name duplication") {
    val c0 = "ab"
    val c1 = "AB"
    var df1 = Seq((1, 1)).toDF(c0, c1)
    var df2 = Seq((1, 1)).toDF("c0", "c1")
    intercept[SnowparkClientException] {
      df1.unionByName(df2)
    }
    df1 = Seq((1, 1)).toDF("c0", "c1")
    df2 = Seq((1, 1)).toDF(c0, c1)
    intercept[SnowparkClientException] {
      df1.unionByName(df2)
    }
  }

  test("SN: unionAll by name - check name duplication") {
    val c0 = "ab"
    val c1 = "AB"
    var df1 = Seq((1, 1)).toDF(c0, c1)
    var df2 = Seq((1, 1)).toDF("c0", "c1")
    intercept[SnowparkClientException] {
      df1.unionAllByName(df2)
    }
    df1 = Seq((1, 1)).toDF("c0", "c1")
    df2 = Seq((1, 1)).toDF(c0, c1)
    intercept[SnowparkClientException] {
      df1.unionAllByName(df2)
    }
  }

  test("intersect") {
    checkAnswer(
      lowerCaseData.intersect(lowerCaseData),
      Row(1, "a") ::
        Row(2, "b") ::
        Row(3, "c") ::
        Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.intersect(upperCaseData), Nil)

    // check null equality
    checkAnswer(
      nullInts.intersect(nullInts),
      Row(1) ::
        Row(2) ::
        Row(3) ::
        Row(null) :: Nil)

    // check if values are de-duplicated
    checkAnswer(allNulls.intersect(allNulls), Row(null) :: Nil)

    // check if values are de-duplicated
    val df = Seq(("id1", 1), ("id1", 1), ("id", 1), ("id1", 2)).toDF("id", "value")
    checkAnswer(
      df.intersect(df),
      Row("id", 1) ::
        Row("id1", 1) ::
        Row("id1", 2) :: Nil)
  }

  test("Project should not be pushed down through Intersect or Except") {
    val df1 = (1 to 100).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 30).map(Tuple1.apply).toDF("i")
    val intersect = df1.intersect(df2)
    val except = df1.except(df2)
    assert(intersect.count() === 30)
    assert(except.count() === 70)
  }
  test("except - nullability") {
    val nonNullableInts = Seq(Tuple1(11), Tuple1(3)).toDF("a")
    assert(nonNullableInts.schema.forall(!_.nullable))

    val df1 = nonNullableInts.except(nullInts)
    checkAnswer(df1, Row(11) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.except(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(2) :: Row(null) :: Nil)
    assert(df2.schema.forall(_.nullable))

    val df3 = nullInts.except(nullInts)
    checkAnswer(df3, Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.except(nonNullableInts)
    checkAnswer(df4, Nil)
    assert(df4.schema.forall(!_.nullable))
  }

  test("except distinct - SQL compliance") {
    val df_left = Seq(1, 2, 2, 3, 3, 4).toDF("id")
    val df_right = Seq(1, 3).toDF("id")

    checkAnswer(df_left.except(df_right), Row(2) :: Row(4) :: Nil)
  }

  test("mix set operator") {
    val df1 = Seq(1).toDF("a")
    val df2 = Seq(2).toDF("a")
    val df3 = Seq(3).toDF("a")

    checkAnswer(df1.union(df2).intersect(df2.union(df3)), df2.collect())

    checkAnswer(df1.union(df2).intersect(df2.union(df3)).union(df3), df2.union(df3).collect())

    checkAnswer(df1.union(df2).except(df2.union(df3).intersect(df1.union(df2))), df1.collect())
  }

  test("create view from Union result") {
    val viewName = randomName()
    try {
      val dfSeq1 = session
        .createDataFrame(
          Seq(
            (1, "one|TEST1", true),
            (2, "hello1", false),
            (3, "TEST1", false),
            (4, "abc1", false),
            (5, null, false)))
        .toDF("Number", "Word", "flag")

      val viewDF = session
        .createDataFrame(
          Seq(
            (1, "one|TEST2", true),
            (2, "hello2", false),
            (3, "TEST2", false),
            (4, "abc2", false),
            (5, null, false)))
        .toDF("Number", "Word", "flag")
        .union(dfSeq1)
      viewDF.createOrReplaceView(viewName)

      checkAnswer(viewDF, session.table(viewName))
    } finally {
      dropView(viewName)
    }
  }

}
