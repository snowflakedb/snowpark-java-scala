package com.snowflake.snowpark_test

import java.math.RoundingMode
import java.sql.{Date, Timestamp}
import java.util.TimeZone

import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark._
import net.snowflake.client.jdbc.SnowflakeSQLException

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
class ColumnSuite extends TestData {
  import session.implicits._

  test("column names with space") {
    val c1 = """"name with space""""
    val c2 = """"name.with.dot""""
    val df = Seq((1, "a")).toDF(c1, c2)
    checkAnswer(df.select($""""name with space""""), Row(1) :: Nil)
    checkAnswer(df.select(col(c1)), Row(1) :: Nil)
    checkAnswer(df.select(c1), Row(1) :: Nil)
    checkAnswer(df.select(df(c1)), Row(1) :: Nil)

    checkAnswer(df.select($""""name.with.dot""""), Row("a") :: Nil)
    checkAnswer(df.select(col(c2)), Row("a") :: Nil)
    checkAnswer(df.select(c2), Row("a") :: Nil)
    checkAnswer(df.select(df(c2)), Row("a") :: Nil)

  }

  test("alias and case insensitive name") {
    val df = Seq((1, 2)).toDF("a", "int")
    assert(df.select(df("a").as("b")).schema.head.name === "B")
    assert(df.select(df("a").alias("b")).schema.head.name === "B")
    assert(df.select(df("a").name("b")).schema.head.name === "B")
  }

  test("alias and case sensitive name") {
    val df = Seq((1, 2)).toDF("a", "int")
    assert(df.select(df("a").as(s""""b"""")).schema.head.name === "\"b\"")
    assert(df.select(df("a").alias(s""""b"""")).schema.head.name === "\"b\"")
    assert(df.select(df("a").name(s""""b"""")).schema.head.name === "\"b\"")
  }

  test("unary operators") {
    assert(testData1.select(-testData1("NUM")).collect() sameElements Array[Row](Row(-1), Row(-2)))
    assert(
      testData1.select(!testData1("BOOL")).collect() sameElements Array[Row](Row(false), Row(true)))
  }

  test("alias") {
    assert(testData1.select(testData1("NUM")).schema.head.name == "NUM")
    assert(testData1.select(testData1("NUM").as("NUMBER")).schema.head.name == "NUMBER")
    assert(testData1.select(testData1("NUM").as("NUMBER")).schema.head.name != "\"NUM\"")
    assert(testData1.select(testData1("NUM").alias("NUMBER")).schema.head.name == "NUMBER")
  }

  test("equal and not equal") {
    val expectedEqual = Array[Row](Row(1, true, "a"))
    assert(testData1.where(testData1("BOOL") === true).collect() sameElements expectedEqual)
    assert(
      testData1.where(testData1("BOOL") equal_to lit(true)).collect() sameElements expectedEqual)
    assert(testData1.where(testData1("BOOL") equal_to true).collect() sameElements expectedEqual)

    val expectedNotEqual = Array[Row](Row(2, false, "b"))
    assert(testData1.where(testData1("BOOL") =!= true).collect() sameElements expectedNotEqual)
    assert(
      testData1
        .where(testData1("BOOL") not_equal lit(true))
        .collect() sameElements expectedNotEqual)
    assert(
      testData1.where(testData1("BOOL") not_equal true).collect() sameElements expectedNotEqual)
  }

  test("gt and lt") {
    val expectedGt = Array[Row](Row(2, false, "b"))
    assert(testData1.where(testData1("NUM") > 1).collect() sameElements expectedGt)
    assert(testData1.where(testData1("NUM") gt lit(1)).collect() sameElements expectedGt)
    assert(testData1.where(testData1("NUM") gt 1).collect() sameElements expectedGt)

    val expectedLt = Array[Row](Row(1, true, "a"))
    assert(testData1.where(testData1("NUM") < 2).collect() sameElements expectedLt)
    assert(testData1.where(testData1("NUM") lt lit(2)).collect() sameElements expectedLt)
    assert(testData1.where(testData1("NUM") lt 2).collect() sameElements expectedLt)
  }

  test("leq and geq") {
    val expectedGeq = Array[Row](Row(2, false, "b"))
    assert(testData1.where(testData1("NUM") >= 2).collect() sameElements expectedGeq)
    assert(testData1.where(testData1("NUM") geq lit(2)).collect() sameElements expectedGeq)
    assert(testData1.where(testData1("NUM") geq 2).collect() sameElements expectedGeq)

    val expectedLeq = Array[Row](Row(1, true, "a"))
    assert(testData1.where(testData1("NUM") <= 1).collect() sameElements expectedLeq)
    assert(testData1.where(testData1("NUM") leq lit(1)).collect() sameElements expectedLeq)
    assert(testData1.where(testData1("NUM") leq 1).collect() sameElements expectedLeq)

    assert(
      testData1.where(testData1("NUM").between(lit(0), lit(1))).collect() sameElements Array[Row](
        Row(1, true, "a")))
  }

  test("null safe operators") {
    val df = session.sql("select * from values(null, 1),(2, 2),(null, null) as T(a,b)")

    val expectedEqualNull1 = Array[Row](Row(false), Row(true), Row(true))
    assert(df.select(df("A") <=> df("B")).collect() sameElements expectedEqualNull1)
    assert(df.select(df("A").equal_null(df("B"))).collect() sameElements expectedEqualNull1)

    val expectedEqualNull2 = Array[Row](Row(true), Row(false), Row(true))
    assert(df.select(df("A").equal_null(null)).collect() sameElements expectedEqualNull2)
  }

  test("NaN and Null") {
    val df = session.sql("select * from values(1.1,1),(null,2),('NaN' :: Float,3) as T(a, b)")

    assert(df.where(df("A").equal_nan).collect() sameElements Array[Row](Row(Double.NaN, 3)))
    assert(df.where(df("A").is_null).collect() sameElements Array[Row](Row(null, 2)))
    assert(df.where(df("A").isNull).collect() sameElements Array[Row](Row(null, 2)))
    assert(
      df.where(df("A").is_not_null).collect() sameElements Array[Row](
        Row(1.1, 1),
        Row(Double.NaN, 3)))
  }

  test("&& ||") {
    val df = session.sql(
      "select * from values(true,true),(true,false),(false, true), (false, false) as T(a, b)")

    assert(df.where(df("A") && df("B")).collect() sameElements Array[Row](Row(true, true)))
    assert(df.where(df("A") and df("B")).collect() sameElements Array[Row](Row(true, true)))
    assert(
      df.where(df("A") || df("B"))
        .collect() sameElements Array[Row](Row(true, true), Row(true, false), Row(false, true)))
    assert(
      df.where(df("A") or df("B"))
        .collect() sameElements Array[Row](Row(true, true), Row(true, false), Row(false, true)))
  }

  test("+ - * / %") {
    val df = session.sql("select * from values(11, 13) as T(a, b)")

    val expectedPlus = Array[Row](Row(24))
    assert(df.select(df("A") + df("B")).collect() sameElements expectedPlus)
    assert(df.select(df("A") plus df("B")).collect() sameElements expectedPlus)
    assert(df.select(df("A") plus 13).collect() sameElements expectedPlus)

    val expectedMinus = Array[Row](Row(-2))
    assert(df.select(df("A") - df("B")).collect() sameElements expectedMinus)
    assert(df.select(df("A") minus df("B")).collect() sameElements expectedMinus)
    assert(df.select(df("A") minus 13).collect() sameElements expectedMinus)

    val expectedMultiply = Array[Row](Row(143))
    assert(df.select(df("A") * df("B")).collect() sameElements expectedMultiply)
    assert(df.select(df("A") multiply df("B")).collect() sameElements expectedMultiply)
    assert(df.select(df("A") multiply 13).collect() sameElements expectedMultiply)

    val expectedMod = Array[Row](Row(11))
    assert(df.select(df("A") % df("B")).collect() sameElements expectedMod)
    assert(df.select(df("A") mod df("B")).collect() sameElements expectedMod)
    assert(df.select(df("A") mod 13).collect() sameElements expectedMod)

    val assertDivide = (result: Array[Row]) => {
      assert(result.length == 1)
      assert(result(0).length == 1)
      assert(result(0).getDecimal(0).toString == "0.846154")
    }

    assertDivide(df.select(df("A") / df("B")).collect())
    assertDivide(df.select(df("A") divide df("B")).collect())
    assertDivide(df.select(df("A") divide 13).collect())
  }

  test("cast") {
    val castWithDataTypeSchema = testData1.select(testData1("NUM").cast(StringType)).schema
    val castWithStringSchema = testData1.select(testData1("NUM").cast("string")).schema
    val expectedSchema = StructType(
      Array(StructField("\"CAST (\"\"NUM\"\" AS STRING)\"", StringType, nullable = false)))

    assert(castWithDataTypeSchema == expectedSchema)
    assert(castWithStringSchema == expectedSchema)

    val exception = intercept[IllegalArgumentException] {
      testData1.select(testData1("NUM").cast("bad_type")).collect()
    }
    assert(
      exception.getMessage.toLowerCase(java.util.Locale.ROOT).contains("unsupported data type"))
  }

  test("order") {
    assert(
      nullData1
        .sort(nullData1("A").asc)
        .collect() sameElements Array[Row](Row(null), Row(null), Row(1), Row(2), Row(3)))
    assert(
      nullData1
        .sort(nullData1("A").asc_nulls_first)
        .collect() sameElements Array[Row](Row(null), Row(null), Row(1), Row(2), Row(3)))
    assert(
      nullData1
        .sort(nullData1("A").asc_nulls_last)
        .collect() sameElements Array[Row](Row(1), Row(2), Row(3), Row(null), Row(null)))

    assert(
      nullData1
        .sort(nullData1("A").desc)
        .collect() sameElements Array[Row](Row(3), Row(2), Row(1), Row(null), Row(null)))
    assert(
      nullData1
        .sort(nullData1("A").desc_nulls_last)
        .collect() sameElements Array[Row](Row(3), Row(2), Row(1), Row(null), Row(null)))
    assert(
      nullData1
        .sort(nullData1("A").desc_nulls_first)
        .collect() sameElements Array[Row](Row(null), Row(null), Row(3), Row(2), Row(1)))
  }

  test("bitwise operator") {
    val df = session.sql("select * from values(1, 2) as T(a, b)")

    assert(df.select(df("A").bitand(df("B"))).collect() sameElements Array[Row](Row(0)))
    assert(df.select(df("A").bitor(df("B"))).collect() sameElements Array[Row](Row(3)))
    assert(df.select(df("A").bitxor(df("B"))).collect() sameElements Array[Row](Row(3)))
  }

  test("withColumn with special column names") {
    // Ensure that One and "One" are different column names
    checkAnswer(Seq((1)).toDF(""""One"""").withColumn("Two", lit("two")), Row(1, "two") :: Nil)
    checkAnswer(Seq((1)).toDF(""""One"""").withColumn("One", lit("two")), Row(1, "two") :: Nil)
    checkAnswer(Seq((1)).toDF("One").withColumn(""""One"""", lit("two")), Row(1, "two") :: Nil)

    // Ensure that One and ONE are the same
    checkAnswer(Seq((1)).toDF("one").withColumn(""""ONE"""", lit("two")), Row("two") :: Nil)
    checkAnswer(Seq((1)).toDF("One").withColumn("One", lit("two")), Row("two") :: Nil)
    checkAnswer(Seq((1)).toDF("one").withColumn("ONE", lit("two")), Row("two") :: Nil)
    checkAnswer(Seq((1)).toDF("OnE").withColumn("oNe", lit("two")), Row("two") :: Nil)

    // Ensure that One and ONE are the same
    checkAnswer(Seq((1)).toDF(""""OnE"""").withColumn(""""OnE"""", lit("two")), Row("two") :: Nil)
  }

  test("toDF with special column names") {
    assert(Seq((1)).toDF("ONE").schema equals Seq((1)).toDF("one").schema)
    assert(Seq((1)).toDF("OnE").schema equals Seq((1)).toDF("oNe").schema)
    assert(Seq((1)).toDF("OnE").schema equals Seq((1)).toDF(""""ONE"""").schema)
    assert(Seq((1)).toDF("ONE").schema != Seq((1)).toDF(""""oNe"""").schema)
    assert(Seq((1)).toDF(""""ONe"""").schema != Seq((1)).toDF(""""oNe"""").schema)
    assert(Seq((1)).toDF(""""ONe"""").schema != Seq((1)).toDF("ONe").schema)
  }

  test("column resolution with different kinds of names") {
    var df = Seq((1)).toDF("One")
    checkAnswer(df.select(df("one")), Row(1) :: Nil)
    checkAnswer(df.select(df("oNe")), Row(1) :: Nil)
    checkAnswer(df.select(df(""""ONE"""")), Row(1) :: Nil)
    intercept[SnowparkClientException] {
      df.col(""""One"""")
    }

    df = Seq((1)).toDF("One One")
    checkAnswer(df.select(df("One One")), Row(1) :: Nil)
    checkAnswer(df.select(df("\"One One\"")), Row(1) :: Nil)
    intercept[SnowparkClientException] {
      df.col(""""one one"""")
    }
    intercept[SnowparkClientException] {
      df("one one")
    }
    intercept[SnowparkClientException] {
      df(""""ONE ONE"""")
    }

    df = Seq((1)).toDF(""""One One"""")
    checkAnswer(df.select(df(""""One One"""")), Row(1) :: Nil)
    intercept[SnowparkClientException] {
      df.col(""""ONE ONE"""")
    }
  }

  test("Drop columns by string") {
    val df = Seq((1, 2)).toDF("One", """"One"""")
    assert(df.drop("one").schema.fields.map(_.name).head equals """"One"""")
    assert(df.drop(""""One"""").schema.fields.map(_.name).head equals """ONE""")
    assert(
      df.drop(Seq.empty[String]).schema.fields.map(_.name).toSeq.sorted equals Seq(
        """"One"""",
        """ONE"""))
    assert(
      df.drop(""""one"""").schema.fields.map(_.name).toSeq.sorted equals Seq(
        """"One"""",
        """ONE"""))

    val ex = intercept[SnowparkClientException] {
      df.drop("ONE", """"One"""").collect()
    }

    assert(ex.getMessage contains "all")
  }

  test("Drop columns by column") {
    val df = Seq((1, 2)).toDF("One", """"One"""")
    assert(df.drop(col("one")).schema.fields.map(_.name).head equals """"One"""")
    assert(df.drop(df(""""One"""")).schema.fields.map(_.name).head equals """ONE""")
    assert(
      df.drop(col(""""one"""")).schema.fields.map(_.name).toSeq.sorted equals Seq(
        """"One"""",
        """ONE"""))
    assert(
      df.drop(Seq.empty[Column]).schema.fields.map(_.name).toSeq.sorted equals Seq(
        """"One"""",
        """ONE"""))

    var ex = intercept[SnowparkClientException] {
      df.drop(df("ONE"), col(""""One"""")).collect()
    }

    assert(ex.getMessage contains "all")

    ex = intercept[SnowparkClientException] {
      df.drop(df("ONE") + col(""""One""""))
    }

    assert(ex.getMessage contains "name")

    // Note below should arguably not work, but does because the semantics is to drop by name.
    val df2 = Seq((1, 2)).toDF("One", """"One"""")
    assert(df.drop(df2("one")).schema.fields.map(_.name).head equals """"One"""")
  }

  test("Fully qualified column name") {
    val randomName = Random.nextInt.toString
    val temp = getFullyQualifiedTempSchema()
    val rName = s""""r_tr#!.${randomName}""""
    val sName = s""""s_tr#!.${randomName}""""
    val udfName = s""""u_tr#!.${randomName}""""
    try {
      session.sql(s"""create table ${temp}.${rName} ("d(" int)""").collect()
      session.sql(s"""create table ${temp}.${sName} ("c(" int)""").collect()
      session
        .sql(s"""create function ${temp}.${udfName}(v integer)
        returns float
        as '3.141592654::FLOAT'""")
        .collect()
      val df = session.sql(s"""select ${temp}.${rName}."d(",
          ${temp}.${sName}."c(", ${temp}.${udfName}(1 :: INT)
          FROM ${temp}.${rName}, ${temp}.${sName}""")
      val colsUnresolved = df.schema.fields.map(_.name).map(col).toSeq
      val colsResolved = df.schema.fields.map(_.name).map(df.apply).toSeq
      val df2 = df.select(colsUnresolved ++ colsResolved)
      df2.collect()
    } finally {
      session.sql(s"drop function ${temp}.${udfName}(integer)").collect()
      session.sql(s"drop table ${temp}.${sName}").collect()
      session.sql(s"drop table ${temp}.${rName}").collect()
    }
  }

  test("Column names with quotes") {
    val df = Seq((1, 2, 3)).toDF("col\"", "\"col\"", "\"\"\"col\"")
    checkAnswer(df.select(col("col\"")), Row(1) :: Nil)
    checkAnswer(df.select(col("\"col\"\"\"")), Row(1) :: Nil)
    checkAnswer(df.select(col("\"col\"")), Row(2) :: Nil)
    checkAnswer(df.select(col("\"\"\"col\"")), Row(3) :: Nil)
    intercept[Exception] {
      df.select(col("\"col\"\"")).collect()
    }
    intercept[Exception] {
      df.select(col("\"\"col\"")).collect()
    }
    intercept[Exception] {
      df.select(col("\"col\"\"\"\"")).collect()
    }
  }

  test("Column constructors: col") {
    val df = Seq((1, 2, 3)).toDF("col", "\"col\"", "col .")
    checkAnswer(df.select(col("col")), Row(1) :: Nil)
    checkAnswer(df.select(col("\"col\"")), Row(2) :: Nil)
    checkAnswer(df.select(col("col .")), Row(3) :: Nil)

    checkAnswer(df.select(col("COL")), Row(1) :: Nil)
    checkAnswer(df.select(col("CoL")), Row(1) :: Nil)
    checkAnswer(df.select(col("\"COL\"")), Row(1) :: Nil)
    intercept[Exception] {
      df.select(col("\"Col\"")).collect()
    }
    intercept[Exception] {
      df.select(col("COL .")).collect()
    }
    intercept[Exception] {
      df.select(col("\"CoL\"")).collect()
    }
  }

  test("Column constructors: $") {
    val df = Seq((1, 2, 3)).toDF("col", "\"col\"", "col .")
    checkAnswer(df.select($"col"), Row(1) :: Nil)
    checkAnswer(df.select($""""col""""), Row(2) :: Nil)
    checkAnswer(df.select($"col ."), Row(3) :: Nil)

    checkAnswer(df.select($"COL"), Row(1) :: Nil)
    checkAnswer(df.select($"CoL"), Row(1) :: Nil)
    checkAnswer(df.select($""""COL""""), Row(1) :: Nil)
    intercept[Exception] {
      df.select($""""Col"""").collect()
    }
    intercept[Exception] {
      df.select($"COL .").collect()
    }
    intercept[Exception] {
      df.select($""""CoL"""").collect()
    }
  }

  test("Column constructors: select") {
    val df = Seq((1, 2, 3)).toDF("col", "\"col\"", "col .")
    checkAnswer(df.select("col"), Row(1) :: Nil)
    checkAnswer(df.select("\"col\""), Row(2) :: Nil)
    checkAnswer(df.select("col ."), Row(3) :: Nil)

    checkAnswer(df.select("COL"), Row(1) :: Nil)
    checkAnswer(df.select("CoL"), Row(1) :: Nil)
    checkAnswer(df.select("\"COL\""), Row(1) :: Nil)
    intercept[Exception] {
      df.select("\"Col\"").collect()
    }
    intercept[Exception] {
      df.select("COL .").collect()
    }
  }

  test("Column constructors: symbol") {
    val df = Seq((1, 2, 3)).toDF("col", "\"col\"", "col .")
    checkAnswer(df.select('col), Row(1) :: Nil)
    checkAnswer(df.select('COL), Row(1) :: Nil)
    checkAnswer(df.select('CoL), Row(1) :: Nil)
  }

  test("sqlExpr column") {
    val df = Seq((1, 2, 3)).toDF("col", "\"col\"", "col .")
    checkAnswer(df.select(sqlExpr("col")), Row(1) :: Nil)
    checkAnswer(df.select(sqlExpr("\"col\"")), Row(2) :: Nil)

    checkAnswer(df.select(sqlExpr("COL")), Row(1) :: Nil)
    checkAnswer(df.select(sqlExpr("CoL")), Row(1) :: Nil)
    checkAnswer(df.select(sqlExpr("\"COL\"")), Row(1) :: Nil)
    checkAnswer(df.select(sqlExpr("col + 10")), Row(11) :: Nil)
    checkAnswer(df.select(sqlExpr("\"col\" + 10")), Row(12) :: Nil)
    checkAnswer(df.filter(sqlExpr("col < 1")), Nil)
    checkAnswer(df.filter(sqlExpr("\"col\" = 2")).select(col("col")), Row(1) :: Nil)
    intercept[Exception] {
      df.select(sqlExpr("\"Col\"")).collect()
    }
    intercept[Exception] {
      df.select(sqlExpr("COL .")).collect()
    }
    intercept[Exception] {
      df.select(sqlExpr("\"CoL\"")).collect()
    }
    intercept[Exception] {
      df.select(sqlExpr("col .")).collect()
    }
  }

  test("Errors for aliased columns") {
    val df = Seq((1)).toDF("c")
    var ex = intercept[SnowparkClientException] {
      df.select(col("a").as("b") + 10).collect()
    }
    assert(ex.getMessage contains "root")
    ex = intercept[SnowparkClientException] {
      df.groupBy(col("a")).agg(avg(col("a").as("b"))).collect()
    }
    assert(ex.getMessage contains "root")
  }

  test("like") {
    checkAnswer(string4.where(col("A").like(lit("%p%"))), Seq(Row("apple"), Row("peach")))
  }

  test("subfield") {
    checkAnswer(nullJson1.select(col("v")("a")), Seq(Row("null"), Row("\"foo\""), Row(null)))

    checkAnswer(array2.select(col("arr1")(0)), Seq(Row("1"), Row("6")))

    checkAnswer(array2.select(parse_json(col("f"))(0)("a")), Seq(Row("1"), Row("1")))

    // Row name is not case-sensitive. field name is case-sensitive
    checkAnswer(variant2.select(col("src")("vehicle")(0)("make")), Seq(Row("\"Honda\"")))
    checkAnswer(variant2.select(col("SRC")("vehicle")(0)("make")), Seq(Row("\"Honda\"")))
    checkAnswer(variant2.select(col("src")("vehicle")(0)("MAKE")), Seq(Row(null)))
    checkAnswer(variant2.select(col("src")("VEHICLE")(0)("make")), Seq(Row(null)))

    // Space and dot in key is fine. User need to escape single quote with two single quotes.
    checkAnswer(variant2.select(col("src")("date with '' and .")), Seq(Row("\"2017-04-28\"")))

    // path is not accepted
    checkAnswer(variant2.select(col("src")("salesperson.id")), Seq(Row(null)))
  }

  test("regexp") {
    checkAnswer(string4.where(col("a").regexp(lit("ap.le"))), Seq(Row("apple")))
  }

  test("collate") {
    checkAnswer(string3.where(col("a").collate("en_US-trim") === "abcba"), Seq(Row("  abcba  ")))
  }

  test("get column name") {
    assert(integer1.col("a").getName.get == "\"A\"")
    assert((col("col") > 100).getName.isEmpty)
  }

  test("when case") {
    checkAnswer(
      nullData1.select(
        functions
          .when(col("a").is_null, lit(5))
          .when(col("a") === 1, lit(6))
          .otherwise(lit(7))
          .as("a")),
      Seq(Row(5), Row(7), Row(6), Row(7), Row(5)))

    checkAnswer(
      nullData1.select(
        functions
          .when(col("a").is_null, lit(5))
          .when(col("a") === 1, lit(6))
          .`else`(lit(7))
          .as("a")),
      Seq(Row(5), Row(7), Row(6), Row(7), Row(5)))

    // no column typed value
    checkAnswer(
      nullData1.select(
        functions
          .when(col("a").is_null, lit(1))
          .when(col("a") === 1, col("a") / 2)
          .when(col("a") === 2, col("a") * 2)
          .when(col("a") === 3, pow(col("a"), 2))
          .as("a")),
      Seq(Row(0.5), Row(1.0), Row(1.0), Row(4.0), Row(9.0)))

    checkAnswer(
      nullData1.select(
        functions
          .when(col("a").is_null, "null_value")
          .when(col("a") <= 2, "lower or equal than two")
          .when(col("a") >= 3, "greater than two")
          .as("a")),
      Seq(
        Row("greater than two"),
        Row("lower or equal than two"),
        Row("lower or equal than two"),
        Row("null_value"),
        Row("null_value")))

    // No column otherwise
    checkAnswer(
      nullData1.select(
        functions
          .when(col("a").is_null, lit(5))
          .when(col("a") === 1, lit(6))
          .otherwise(7)
          .as("a")),
      Seq(Row(5), Row(7), Row(6), Row(7), Row(5)))

    // Handling nulls
    checkAnswer(
      nullData1.select(
        functions
          .when(col("a").is_null, null)
          .when(col("a") === 1, null)
          .otherwise(null)
          .as("a")),
      Seq(Row(null), Row(null), Row(null), Row(null), Row(null)))

    // empty otherwise
    checkAnswer(
      nullData1.select(
        functions
          .when(col("a").is_null, lit(5))
          .when(col("a") === 1, lit(6))
          .as("a")),
      Seq(Row(5), Row(null), Row(6), Row(null), Row(5)))

    // wrong type, snowflake sql exception
    assertThrows[SnowflakeSQLException](
      nullData1
        .select(
          functions
            .when(col("a").is_null, lit("a"))
            .when(col("a") === 1, lit(6))
            .as("a"))
        .collect())
  }

  test("lit contains '") {
    val df = Seq((1, "'"), (2, "''")).toDF("a", "b")
    checkAnswer(df.where(col("b") === "'"), Seq(Row(1, "'")))
  }

  test("test show binary data") {
    val df = session.sql("select '616263' :: Binary as A, '' :: Binary as B, NULL :: Binary as C")
    df.show()
    assert(
      getShowString(df, 10, 50) ==
        """-------------------------
          ||"A"       |"B"  |"C"   |
          |-------------------------
          ||'616263'  |''   |NULL  |
          |-------------------------
          |""".stripMargin)
  }

  test("In Expression 1: IN with constant value list") {
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    // filter without NOT
    val df1 = df.filter(col("a").in(Seq(1, 2)))
    assert(
      getShowString(df1, 10, 50) ==
        """-------------------------
          ||"A"  |"B"  |"C"  |"D"  |
          |-------------------------
          ||1    |a    |1    |1    |
          ||2    |b    |2    |2    |
          |-------------------------
          |""".stripMargin)

    // filter with NOT
    val df2 = df.filter(!col("a").in(Seq(lit(1), lit(2))))
    assert(
      getShowString(df2, 10, 50) ==
        """-------------------------
          ||"A"  |"B"  |"C"  |"D"  |
          |-------------------------
          ||3    |b    |33   |33   |
          |-------------------------
          |""".stripMargin)

    // select without NOT
    val df3 = df.select(col("a").in(Seq(1, 2)).as("in_result"))
    assert(
      getShowString(df3, 10, 50) ==
        """---------------
          ||"IN_RESULT"  |
          |---------------
          ||true         |
          ||true         |
          ||false        |
          |---------------
          |""".stripMargin)

    // select with NOT
    val df4 = df.select(!col("a").in(Seq(1, 2)).as("in_result"))
    assert(
      getShowString(df4, 10, 50) ==
        """---------------
          ||"IN_RESULT"  |
          |---------------
          ||false        |
          ||false        |
          ||true         |
          |---------------
          |""".stripMargin)
  }

  test("In Expression 2: In with sub query") {
    val df0 = Seq(1, 2, 5).toDF("a")
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    // filter without NOT
    val df1 = df.filter(col("a").in(df0.filter(col("a") < 3)))
    checkAnswer(df1, Seq(Row(1, "a", 1, 1), Row(2, "b", 2, 2)))

    // filter with NOT
    val df2 = df.filter(!df("a").in(df0.filter(col("a") < 3)))
    checkAnswer(df2, Seq(Row(3, "b", 33, 33)))

    // select without NOT
    val df3 = df.select(col("a").in(df0.filter(col("a") < 2)).as("in_result"))
    checkAnswer(df3, Seq(Row(true), Row(false), Row(false)))

    // select with NOT
    val df4 = df.select(!df("a").in(df0.filter(col("a") < 2)).as("in_result"))
    checkAnswer(df4, Seq(Row(false), Row(true), Row(true)))
  }

  test("In Expression 3: IN with all types") {
    testWithTimezone("America/Los_Angeles") {
      val schema = StructType(
        Seq(
          StructField("ID", LongType),
          StructField("string", StringType),
          StructField("byte", ByteType),
          StructField("short", ShortType),
          StructField("int", IntegerType),
          StructField("long", LongType),
          StructField("float", FloatType),
          StructField("double", DoubleType),
          StructField("decimal", DecimalType(10, 3)),
          StructField("boolean", BooleanType),
          StructField("binary", BinaryType),
          StructField("timestamp", TimestampType),
          StructField("date", DateType)))

      val timestamp: Long = 1606179541282L
      val largeData =
        for (i <- 0 to 5)
          yield Row(
            i.toLong,
            "a",
            1.toByte,
            2.toShort,
            3,
            4L,
            1.1f,
            1.2d,
            new java.math.BigDecimal(1.2),
            true,
            Array(1.toByte, 2.toByte),
            new Timestamp(timestamp - 100),
            new Date(timestamp - 100))

      val df = session.createDataFrame(largeData, schema)
      // scala style checks doesn't support to put all of these expression in one filter()
      // So split it as 2 steps.
      val df2 = df.filter(
        col("ID").in(Seq(1, 2)) &&
          col("string").in(Seq("a", "b")) &&
          col("byte").in(Seq(1, 2)) &&
          col("short").in(Seq(2, 3)) &&
          col("int").in(Seq(3, 4)) &&
          col("long").in(Seq(4, 5)) &&
          col("float").in(Seq(1.1f, 1.2f)) &&
          col("double").in(Seq(1.2d, 1.3d)) &&
          col("decimal").in(
            Seq(
              new java.math.BigDecimal(1.2).setScale(3, RoundingMode.HALF_UP),
              new java.math.BigDecimal(1.3).setScale(3, RoundingMode.HALF_UP))))
      val df3 = df2.filter(
        col("boolean").in(Seq(true, false)) &&
          col("binary").in(Seq(Array(1.toByte, 2.toByte), Array(2.toByte, 3.toByte))) &&
          col("timestamp")
            .in(Seq(new Timestamp(timestamp - 100), new Timestamp(timestamp - 200))) &&
          col("date").in(Seq(new Date(timestamp - 100), new Date(timestamp - 200))))

      df3.show()
      // scalastyle:off
      assert(
        getShowString(df3, 10, 50) ==
          """------------------------------------------------------------------------------------------------------------------------------------------------------
            ||"ID"  |"STRING"  |"BYTE"  |"SHORT"  |"INT"  |"LONG"  |"FLOAT"  |"DOUBLE"  |"DECIMAL"  |"BOOLEAN"  |"BINARY"  |"TIMESTAMP"              |"DATE"      |
            |------------------------------------------------------------------------------------------------------------------------------------------------------
            ||1     |a         |1       |2        |3      |4       |1.1      |1.2       |1.200      |true       |'0102'    |2020-11-23 16:59:01.182  |2020-11-23  |
            ||2     |a         |1       |2        |3      |4       |1.1      |1.2       |1.200      |true       |'0102'    |2020-11-23 16:59:01.182  |2020-11-23  |
            |------------------------------------------------------------------------------------------------------------------------------------------------------
            |""".stripMargin)
      // scalastyle:on
    }
  }

  // Below cases are supported by snowflake SQL, but they are confusing,
  // so they are disabled. They may be supported if users request them in the future.
  test("In Expression 4: Negative test to input Column in value list") {
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    val ex1 = intercept[SnowparkClientException] {
      df.filter(col("a").in(Seq(col("c"))))
    }
    assert(ex1.errorCode.equals("0314"))

    val ex2 = intercept[SnowparkClientException] {
      df.filter(col("a").in(Seq(1, df("c"))))
    }
    assert(ex2.errorCode.equals("0314"))

    val ex3 = intercept[SnowparkClientException] {
      df.filter(col("a").in(Seq(1, df.select("c").limit(1))))
    }
    assert(ex3.errorCode.equals("0314"))
  }

  test("In Expression 5: Negative test that sub query has multiple columns.") {
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    val ex = intercept[SnowparkClientException] {
      df.filter(col("a").in(df.select("c", "d")))
    }
    assert(ex.errorCode.equals("0315"))
  }

  test("In Expression 6: multiple columns with const values") {
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    // filter without NOT
    val df1 =
      df.filter(functions.in(Seq(col("a"), col("b")), Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "c"))))
    df1.show()
    checkAnswer(df1, Seq(Row(1, "a", 1, 1), Row(2, "b", 2, 2)))

    // filter with NOT
    val df2 =
      df.filter(!functions.in(Seq(col("a"), col("b")), Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "c"))))
    checkAnswer(df2, Seq(Row(3, "b", 33, 33)))

    // select without NOT
    val df3 =
      df.select(
        functions
          .in(Seq(col("a"), col("c")), Seq(Seq(1, 1), Seq(2, 2), Seq(3, 3)))
          .as("in_result"))
    assert(
      getShowString(df3, 10, 50) ==
        """---------------
          ||"IN_RESULT"  |
          |---------------
          ||true         |
          ||true         |
          ||false        |
          |---------------
          |""".stripMargin)

    // select with NOT
    val df4 =
      df.select(
        (!functions.in(Seq(col("a"), col("c")), Seq(Seq(1, 1), Seq(2, 2), Seq(3, 3))))
          .as("in_result"))
    assert(
      getShowString(df4, 10, 50) ==
        """---------------
          ||"IN_RESULT"  |
          |---------------
          ||false        |
          ||false        |
          ||true         |
          |---------------
          |""".stripMargin)
  }

  test("In Expression 7: multiple columns with sub query") {
    val df0 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("a", "b")
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    // filter without NOT
    val df1 = df.filter(functions.in(Seq(col("a"), col("b")), df0))
    checkAnswer(df1, Seq(Row(1, "a", 1, 1), Row(2, "b", 2, 2)))

    // filter with NOT
    val df2 = df.filter(!functions.in(Seq(col("a"), col("b")), df0))
    checkAnswer(df2, Seq(Row(3, "b", 33, 33)))

    // select without NOT
    val df3 = df.select(functions.in(Seq(col("a"), col("b")), df0).as("in_result"))
    assert(
      getShowString(df3, 10, 50) ==
        """---------------
          ||"IN_RESULT"  |
          |---------------
          ||true         |
          ||true         |
          ||false        |
          |---------------
          |""".stripMargin)

    // select with NOT
    val df4 = df.select((!functions.in(Seq(col("a"), col("b")), df0)).as("in_result"))
    assert(
      getShowString(df4, 10, 50) ==
        """---------------
          ||"IN_RESULT"  |
          |---------------
          ||false        |
          ||false        |
          ||true         |
          |---------------
          |""".stripMargin)
  }

  // Below cases are supported by snowflake SQL, but they are confusing,
  // so they are disabled. They may be supported if users request them in the future.
  test("In Expression 8: Negative test to input Column in value list") {
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    val ex1 = intercept[SnowparkClientException] {
      df.filter(functions.in(Seq(col("a"), col("b")), Seq(Seq(1, "a"), Seq(col("c"), "b"))))
    }
    assert(ex1.errorCode.equals("0314"))
  }

  test("In Expression 9: Negative test for the column count doesn't match the value list") {
    val df = Seq((1, "a", 1, 1), (2, "b", 2, 2), (3, "b", 33, 33))
      .toDF("a", "b", "c", "d")

    val ex1 = intercept[SnowparkClientException] {
      df.filter(functions.in(Seq(col("a"), col("b")), Seq(Seq(1, "a", 2))))
    }
    assert(ex1.errorCode.equals("0315"))

    val ex2 = intercept[SnowparkClientException] {
      df.filter(functions.in(Seq(col("a"), col("b")), df.select("a", "b", "c")))
    }
    assert(ex2.errorCode.equals("0315"))
  }

  test("Column.in(Column, Seq) fails when Sequence is empty") {
    val inCond = Seq()
    val df = session.table("information_schema.packages").filter(col("language").in(inCond))
    assert(df.count().equals(0L))
  }
}
