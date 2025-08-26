package com.snowflake.snowpark_test

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.sql.{Date, Time, Timestamp}

trait FunctionSuite extends TestData {
  import session.implicits._

  test("col") {
    checkAnswer(testData1.select(col("bool")), Seq(Row(true), Row(false)))
    checkAnswer(testData1.select(column("num")), Seq(Row(1), Row(2)))
  }

  test("lit") {
    checkAnswer(testData1.select(lit(1)), Seq(Row(1), Row(1)))
  }

  test("approx count distinct") {
    checkAnswer(duplicatedNumbers.select(approx_count_distinct(col("A"))), Seq(Row(3)))
  }

  test("avg") {
    checkAnswer(duplicatedNumbers.select(avg(col("A"))), Seq(Row(2.200000)))
  }

  test("corr") {
    checkAnswer(
      number1.groupBy(col("K")).agg(corr(col("V1"), col("v2"))),
      Seq(Row(1, null), Row(2, 0.40367115665231024)))
  }

  test("count") {
    checkAnswer(duplicatedNumbers.select(count(col("A"))), Seq(Row(5)))

    checkAnswer(duplicatedNumbers.select(count_distinct(col("A"))), Seq(Row(3)))

    checkAnswer(duplicatedNumbers.select(countDistinct(col("A"))), Seq(Row(3)))

    checkAnswer(duplicatedNumbers.select(countDistinct("A")), Seq(Row(3)))
  }

  test("covariance") {
    checkAnswer(
      number1.groupBy("K").agg(covar_pop(col("V1"), col("V2"))),
      Seq(Row(1, 0), Row(2, 38.75)))

    checkAnswer(
      number1.groupBy("K").agg(covar_samp(col("V1"), col("V2"))),
      Seq(Row(1, null), Row(2, 51.666666666666664)))
  }

  test("grouping") {
    checkAnswer(
      xyz
        .cube("X", "Y")
        .agg(grouping(col("X")), grouping(col("Y")), grouping_id(col("X"), col("Y"))),
      Seq(
        Row(1, 2, 0, 0, 0),
        Row(2, 1, 0, 0, 0),
        Row(2, 2, 0, 0, 0),
        Row(1, null, 0, 1, 1),
        Row(2, null, 0, 1, 1),
        Row(null, null, 1, 1, 3),
        Row(null, 2, 1, 0, 2),
        Row(null, 1, 1, 0, 2)))
  }

  test("kurtosis") {
    checkAnswer(
      xyz.select(kurtosis(col("X")), kurtosis(col("Y")), kurtosis(col("Z"))),
      Seq(Row(-3.333333333333, 5.000000000000, 3.613736609956)))
  }

  test("max, min, mean") {
    // Case 01: Non-null values
    val expected1 = Seq(Row(2, 1, 3.600000))
    checkAnswer(xyz.select(max(col("X")), min(col("Y")), mean(col("Z"))), expected1)
    checkAnswer(xyz.select(max("X"), min("Y"), mean("Z")), expected1)

    // Case 02: Some null values
    val expected2 = Seq(Row(3, 1, 2.000000))
    checkAnswer(nullInts.select(max(col("A")), min(col("A")), mean(col("A"))), expected2)
    checkAnswer(nullInts.select(max("A"), min("A"), mean("A")), expected2)

    // Case 03: All null values
    val expected3 = Seq(Row(null, null, null))
    checkAnswer(allNulls.select(max(col("A")), min(col("A")), mean(col("A"))), expected3)
    checkAnswer(allNulls.select(max("A"), min("A"), mean("A")), expected3)
  }

  test("skew") {
    checkAnswer(
      xyz.select(skew(col("X")), skew(col("Y")), skew(col("Z"))),
      Seq(Row(-0.6085811063146803, -2.236069766354172, 1.8414236309018863)))
  }

  test("stddev") {
    checkAnswer(
      xyz.select(stddev(col("X")), stddev_samp(col("Y")), stddev_pop(col("Z"))),
      Seq(Row(0.5477225575051661, 0.4472135954999579, 3.3226495451672298)))
  }

  test("sum") {
    checkAnswer(
      duplicatedNumbers.groupBy("A").agg(sum(col("A"))),
      Seq(Row(3, 6), Row(2, 4), Row(1, 1)))

    checkAnswer(duplicatedNumbers.groupBy("A").agg(sum("A")), Seq(Row(3, 6), Row(2, 4), Row(1, 1)))

    checkAnswer(
      duplicatedNumbers.groupBy("A").agg(sum_distinct(col("A"))),
      Seq(Row(3, 3), Row(2, 2), Row(1, 1)))
  }

  test("variance") {
    checkAnswer(
      xyz.groupBy("X").agg(variance(col("Y")), var_pop(col("Z")), var_samp(col("Z"))),
      Seq(Row(1, 0.000000, 1.000000, 2.000000), Row(2, 0.333333, 14.888889, 22.333333)))
  }

  test("cume_dist") {
    checkAnswer(
      xyz.select(cume_dist().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(0.3333333333333333), Row(1.0), Row(1.0), Row(1.0), Row(1.0)))
  }

  test("dense_rank") {
    checkAnswer(
      xyz.select(dense_rank().over(Window.orderBy(col("X")))),
      Seq(Row(1), Row(1), Row(2), Row(2), Row(2)))
  }

  test("lag") {
    checkAnswer(
      xyz.select(lag(col("Z"), 1, lit(0)).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(0), Row(10), Row(1), Row(0), Row(1)))

    checkAnswer(
      xyz.select(lag(col("Z"), 1).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(null), Row(10), Row(1), Row(null), Row(1)))

    checkAnswer(
      xyz.select(lag(col("Z")).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(null), Row(10), Row(1), Row(null), Row(1)))
  }

  test("lead") {
    checkAnswer(
      xyz.select(lead(col("Z"), 1, lit(0)).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(1), Row(3), Row(0), Row(3), Row(0)))

    checkAnswer(
      xyz.select(lead(col("Z"), 1).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(1), Row(3), Row(null), Row(3), Row(null)))

    checkAnswer(
      xyz.select(lead(col("Z")).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(1), Row(3), Row(null), Row(3), Row(null)))
  }

  test("ntile") {
    val df = xyz.withColumn("n", lit(4))
    checkAnswer(
      df.select(ntile(col("n")).over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(1), Row(2), Row(3), Row(1), Row(2)))
  }

  test("percent_rank") {
    checkAnswer(
      xyz.select(percent_rank().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(0.0), Row(0.5), Row(0.5), Row(0.0), Row(0.0)))
  }

  test("rank") {
    checkAnswer(
      xyz.select(rank().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(1), Row(2), Row(2), Row(1), Row(1)))
  }

  test("row_number") {
    checkAnswer(
      xyz.select(row_number().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(1), Row(2), Row(3), Row(1), Row(2)))
  }

  test("coalesce") {
    checkAnswer(
      nullData2.select(coalesce(col("A"), col("B"), col("C"))),
      Seq(Row(1), Row(2), Row(3), Row(null), Row(1), Row(1), Row(1)))
  }

  test("NaN and Null") {
    checkAnswer(
      nanData1.select(equal_nan(col("A")), is_null(col("A"))),
      Seq(Row(false, false), Row(true, false), Row(null, true), Row(false, false)))
  }

  test("negate and not") {
    val df = session.sql("select * from values(1, true),(-2,false) as T(a,b)")
    checkAnswer(df.select(negate(col("A")), not(col("B"))), Seq(Row(-1, false), Row(2, true)))
  }

  test("random") {
    val df = session.sql("select 1")
    df.select(random(123)).collect()
    df.select(random()).collect()
  }

  test("sqrt") {
    checkAnswer(testData1.select(sqrt(col("NUM"))), Seq(Row(1.0), Row(1.4142135623730951)))
  }

  test("bitwise not") {
    checkAnswer(testData1.select(bitnot(col("NUM"))), Seq(Row(-2), Row(-3)))
  }

  test("abs") {
    checkAnswer(number2.select(abs(col("X"))), Seq(Row(1), Row(0), Row(5)))
  }

  test("ceil floor") {
    checkAnswer(double1.select(ceil(col("A"))), Seq(Row(2), Row(3), Row(4)))
    checkAnswer(double1.select(ceil(col("A"))), Seq(Row(2), Row(3), Row(4)))

    checkAnswer(double1.select(floor(col("A"))), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(double1.select(floor(col("A"))), Seq(Row(1), Row(2), Row(3)))
  }

  test("greatest least") {
    checkAnswer(
      xyz.select(greatest(col("X"), col("Y"), col("Z"))),
      Seq(Row(2), Row(3), Row(10), Row(2), Row(3)))

    checkAnswer(
      xyz.select(least(col("X"), col("Y"), col("Z"))),
      Seq(Row(1), Row(1), Row(1), Row(1), Row(2)))
  }

  test("round") {
    // Case: Scale greater than or equal to zero.
    val expected1 = Seq(Row(1.0), Row(2.0), Row(3.0))
    checkAnswer(double1.select(round(col("A"))), expected1)
    checkAnswer(double1.select(round(col("A"), lit(0))), expected1)
    checkAnswer(double1.select(round(col("A"), 0)), expected1)

    // Case: Scale less than zero.
    val df2 = session.sql("select * from values(5),(55),(555) as T(a)")
    val expected2 = Seq(Row(10, 0), Row(60, 100), Row(560, 600))
    checkAnswer(df2.select(round(col("a"), lit(-1)), round(col("a"), lit(-2))), expected2)
    checkAnswer(df2.select(round(col("a"), -1), round(col("a"), -2)), expected2)
  }

  test("asin acos") {
    checkAnswer(
      double2.select(acos(col("A")), asin(col("A"))),
      Seq(
        Row(1.4706289056333368, 0.1001674211615598),
        Row(1.369438406004566, 0.2013579207903308),
        Row(1.2661036727794992, 0.3046926540153975)))

    checkAnswer(
      double2.select(acos(col("A")), asin(col("A"))),
      Seq(
        Row(1.4706289056333368, 0.1001674211615598),
        Row(1.369438406004566, 0.2013579207903308),
        Row(1.2661036727794992, 0.3046926540153975)))
  }

  test("atan atan2") {
    checkAnswer(
      double2.select(atan(col("B")), atan(col("A"))),
      Seq(
        Row(0.4636476090008061, 0.09966865249116204),
        Row(0.5404195002705842, 0.19739555984988078),
        Row(0.6107259643892086, 0.2914567944778671)))

    checkAnswer(
      double2
        .select(atan2(col("B"), col("A"))),
      Seq(Row(1.373400766945016), Row(1.2490457723982544), Row(1.1659045405098132)))
  }

  test("cos cosh") {
    checkAnswer(
      double2.select(cos(col("A")), cos(col("A")), cosh(col("B")), cosh(col("B"))),
      Seq(
        Row(0.9950041652780258, 0.9950041652780258, 1.1276259652063807, 1.1276259652063807),
        Row(0.9800665778412416, 0.9800665778412416, 1.1854652182422676, 1.1854652182422676),
        Row(0.955336489125606, 0.955336489125606, 1.255169005630943, 1.255169005630943)))
  }

  test("exp") {
    checkAnswer(
      number2.select(exp(col("X")), exp(col("X"))),
      Seq(
        Row(2.718281828459045, 2.718281828459045),
        Row(1.0, 1.0),
        Row(0.006737946999085467, 0.006737946999085467)))
  }

  test("factorial") {
    checkAnswer(integer1.select(factorial(col("A"))), Seq(Row(1), Row(2), Row(6)))
  }

  test("log") {
    checkAnswer(
      integer1.select(log(lit(2), col("A")), log(lit(4), col("A"))),
      Seq(Row(0.0, 0.0), Row(1.0, 0.5), Row(1.5849625007211563, 0.7924812503605781)))
  }

  test("pow") {
    checkAnswer(
      double2.select(
        pow(col("A"), col("B")),
        pow(col("A"), "B"),
        pow("A", col("B")),
        pow("A", "B"),
        pow(col("A"), 0.8),
        pow("A", 0.8),
        pow(0.4, col("B")),
        pow(0.4, "B")),
      Seq(
        Row(0.31622776601683794, 0.31622776601683794, 0.31622776601683794, 0.31622776601683794,
          0.15848931924611134, 0.15848931924611134, 0.6324555320336759, 0.6324555320336759),
        Row(0.3807307877431757, 0.3807307877431757, 0.3807307877431757, 0.3807307877431757,
          0.27594593229224296, 0.27594593229224296, 0.5770799623628855, 0.5770799623628855),
        Row(0.4305116202499342, 0.4305116202499342, 0.4305116202499342, 0.4305116202499342,
          0.3816778909618176, 0.3816778909618176, 0.526552881733695, 0.526552881733695)))
  }

  test("shiftleft shiftright") {
    checkAnswer(
      integer1.select(bitshiftleft(col("A"), lit(1)), bitshiftright(col("A"), lit(1))),
      Seq(Row(2, 0), Row(4, 1), Row(6, 1)))
  }

  test("sin sinh") {
    checkAnswer(
      double2.select(sin(col("A")), sin(col("A")), sinh(col("A")), sinh(col("A"))),
      Seq(
        Row(0.09983341664682815, 0.09983341664682815, 0.10016675001984403, 0.10016675001984403),
        Row(0.19866933079506122, 0.19866933079506122, 0.20133600254109402, 0.20133600254109402),
        Row(0.29552020666133955, 0.29552020666133955, 0.3045202934471426, 0.3045202934471426)))
  }

  test("tan tanh") {
    checkAnswer(
      double2.select(tan(col("A")), tan(col("A")), tanh(col("A")), tanh(col("A"))),
      Seq(
        Row(0.10033467208545055, 0.10033467208545055, 0.09966799462495582, 0.09966799462495582),
        Row(0.2027100355086725, 0.2027100355086725, 0.197375320224904, 0.197375320224904),
        Row(0.30933624960962325, 0.30933624960962325, 0.2913126124515909, 0.2913126124515909)))
  }

  test("degrees") {
    checkAnswer(
      double2.select(degrees(col("A")), degrees(col("B"))),
      Seq(
        Row(5.729577951308233, 28.64788975654116),
        Row(11.459155902616466, 34.37746770784939),
        Row(17.188733853924695, 40.10704565915762)))
  }

  test("radians") {
    checkAnswer(
      double1.select(radians(col("A")), radians(col("A"))),
      Seq(
        Row(0.019390607989657, 0.019390607989657),
        Row(0.038781215979314, 0.038781215979314),
        Row(0.058171823968971005, 0.058171823968971005)))
  }

  test("md5 sha1 sha2") {
    checkAnswer(
      string1.select(md5(col("A")), sha1(col("A")), sha2(col("A"), 224)),
      Seq(
        Row(
          "5a105e8b9d40e1329780d62ea2265d8a", // pragma: allowlist secret
          "b444ac06613fc8d63795be9ad0beaf55011936ac", // pragma: allowlist secret
          "aff3c83c40e2f1ae099a0166e1f27580525a9de6acd995f21717e984" // pragma: allowlist secret
        ),
        Row(
          "ad0234829205b9033196ba818f7a872b", // pragma: allowlist secret
          "109f4b3c50d7b0df729d299bc6f8e9ef9066971f", // pragma: allowlist secret
          "35f757ad7f998eb6dd3dd1cd3b5c6de97348b84a951f13de25355177" // pragma: allowlist secret
        ),
        Row(
          "8ad8757baa8564dc136c1e07507f4a98", // pragma: allowlist secret
          "3ebfa301dc59196f18593c45e519287a23297589", // pragma: allowlist secret
          "d2d5c076b2435565f66649edd604dd5987163e8a8240953144ec652f" // pragma: allowlist secret
        )))
  }

  test("hash") {
    checkAnswer(
      string1.select(hash(col("A"))),
      Seq(Row(-1996792119384707157L), Row(-410379000639015509L), Row(9028932499781431792L)))
  }

  test("ascii") {
    checkAnswer(string1.select(ascii(col("B"))), Seq(Row(97), Row(98), Row(99)))
  }

  test("concat_ws") {
    checkAnswer(
      string1.select(concat_ws(lit(","), col("A"), col("B"))),
      Seq(Row("test1,a"), Row("test2,b"), Row("test3,c")))
  }

  test("initcap length lower upper") {
    checkAnswer(
      string2.select(initcap(col("A")), length(col("A")), lower(col("A")), upper(col("A"))),
      Seq(Row("Asdfg", 5, "asdfg", "ASDFG"), Row("Qqq", 3, "qqq", "QQQ"), Row("Qw", 2, "qw", "QW")))
  }

  test("lpad rpad") {
    checkAnswer(
      string2.select(lpad(col("A"), lit(8), lit("X")), rpad(col("A"), lit(9), lit("S"))),
      Seq(Row("XXXasdFg", "asdFgSSSS"), Row("XXXXXqqq", "qqqSSSSSS"), Row("XXXXXXQw", "QwSSSSSSS")))
  }

  test("ltrim rtrim, trim") {
    checkAnswer(
      string3.select(ltrim(col("A")), rtrim(col("A"))),
      Seq(Row("abcba  ", "  abcba"), Row("a12321a   ", " a12321a")))

    checkAnswer(
      string3
        .select(ltrim(col("A"), lit(" a")), rtrim(col("A"), lit(" a")), trim(col("A"), lit("a "))),
      Seq(Row("bcba  ", "  abcb", "bcb"), Row("12321a   ", " a12321", "12321")))
  }

  test("repeat") {
    checkAnswer(string1.select(repeat(col("B"), lit(3))), Seq(Row("aaa"), Row("bbb"), Row("ccc")))
  }

  test("builtin function") {
    val repeat = functions.builtin("repeat")
    checkAnswer(string1.select(repeat(col("B"), 3)), Seq(Row("aaa"), Row("bbb"), Row("ccc")))
  }

  test("soundex") {
    checkAnswer(string4.select(soundex(col("A"))), Seq(Row("a140"), Row("b550"), Row("p200")))
  }

  test("substring - basic functionality") {
    val expectedSubstrings = Seq(Row("est1"), Row("est2"), Row("est3"))
    // With Column parameters
    checkAnswer(string1.select(substring(col("A"), lit(2), lit(4))), expectedSubstrings)
    // With literal parameters
    checkAnswer(string1.select(substring(col("A"), 2, 4)), expectedSubstrings)
  }

  test("substring - start position variations") {
    // Start position 1 (first character)
    val expectedFirstThree = Seq(Row("tes"), Row("tes"), Row("tes"))
    checkAnswer(string1.select(substring(col("A"), 1, 3)), expectedFirstThree)
    // Start position 0 (should behave like position 1)
    checkAnswer(string1.select(substring(col("A"), 0, 3)), expectedFirstThree)

    // Starting from the last character (position 5)
    val expectedLastChar = Seq(Row("1"), Row("2"), Row("3"))
    checkAnswer(string1.select(substring(col("A"), 5, 1)), expectedLastChar)
  }

  test("substring - length variations") {
    // Length 0 - should return empty string regardless of start position
    val expectedEmptyStrings = Seq(Row(""), Row(""), Row(""))
    checkAnswer(string1.select(substring(col("A"), 2, 0)), expectedEmptyStrings)

    // Length 1 - should return single character
    val expectedSingleChar = Seq(Row("e"), Row("e"), Row("e"))
    checkAnswer(string1.select(substring(col("A"), 2, 1)), expectedSingleChar)

    // Very large length (should return remainder of string from position)
    val expectedRemainder = Seq(Row("est1"), Row("est2"), Row("est3"))
    checkAnswer(string1.select(substring(col("A"), 2, 1000)), expectedRemainder)
  }

  test("substring - negative values") {
    // Negative start position (-1) - should return characters from the end
    val expectedFromEnd = Seq(Row("1"), Row("2"), Row("3"))
    checkAnswer(string1.select(substring(col("A"), -1, 3)), expectedFromEnd)

    // Negative length - should return empty string
    val expectedEmptyStrings = Seq(Row(""), Row(""), Row(""))
    checkAnswer(string1.select(substring(col("A"), 2, -1)), expectedEmptyStrings)

    // Both negative start and length - should return empty string
    checkAnswer(string1.select(substring(col("A"), -1, -1)), expectedEmptyStrings)
  }

  test("substring - boundary conditions") {
    // Start position equals string length - should get last character
    val expectedLastCharacter = Seq(Row("1"), Row("2"), Row("3"))
    checkAnswer(string1.select(substring(col("A"), 5, 2)), expectedLastCharacter)

    // Start position is greater than string length - should return empty string
    val expectedEmpty = Seq(Row(""), Row(""), Row(""))
    checkAnswer(string1.select(substring(col("A"), 10, 2)), expectedEmpty)

    // Start + length equals string length - should extract entire string
    val expectedFullString = Seq(Row("test1"), Row("test2"), Row("test3"))
    checkAnswer(string1.select(substring(col("A"), 1, 5)), expectedFullString)

    // Start + length exceeds string length - should return remainder from position
    val expectedRemainder = Seq(Row("st1"), Row("st2"), Row("st3"))
    checkAnswer(string1.select(substring(col("A"), 3, 10)), expectedRemainder)
  }

  test("substring - special string cases") {
    val emptyStrings = Seq("", "", "").toDF("A")
    val expectedEmptyResults = Seq(Row(""), Row(""), Row(""))

    // Substring of empty string should always return empty string
    checkAnswer(emptyStrings.select(substring(col("A"), 1, 2)), expectedEmptyResults)
    checkAnswer(emptyStrings.select(substring(col("A"), -1, 3)), expectedEmptyResults)
  }

  test("substring - null handling") {
    // Case 1: Null string input - should return null regardless of other parameters
    val nullStrings = Seq(null, "test", null).toDF("A")
    val expectedNullResults = Seq(Row(null), Row("te"), Row(null))
    checkAnswer(nullStrings.select(substring(col("A"), 1, 2)), expectedNullResults)

    // Case 2: Null start position - any null parameter should result in null output
    val testDataNullStart = session.createDataFrame(
      Seq(Row("test1", null, 2), Row("test2", 1, 2)),
      StructType(
        Seq(
          StructField("str", StringType),
          StructField("start", IntegerType),
          StructField("len", IntegerType))))
    val expectedNullStartResults = Seq(Row(null), Row("te"))
    checkAnswer(
      testDataNullStart.select(substring(col("str"), col("start"), col("len"))),
      expectedNullStartResults)

    // Case 3: Null length - any null parameter should result in null output
    val testDataNullLen = session.createDataFrame(
      Seq(Row("test1", 1, null), Row("test2", 1, 2)),
      StructType(
        Seq(
          StructField("str", StringType),
          StructField("start", IntegerType),
          StructField("len", IntegerType))))
    val expectedNullLenResults = Seq(Row(null), Row("te"))
    checkAnswer(
      testDataNullLen.select(substring(col("str"), col("start"), col("len"))),
      expectedNullLenResults)
  }

  test("translate") {
    checkAnswer(
      string3.select(translate(col("A"), lit("ab "), lit("XY"))),
      Seq(Row("XYcYX"), Row("X12321X")))
  }

  test("add months") {
    testWithTimezone() {
      checkAnswer(
        date1.select(add_months(col("A"), lit(1))),
        Seq(Row(Date.valueOf("2020-09-01")), Row(Date.valueOf("2011-01-01"))))
    }
  }

  test("current date") {
    // zero1.select(current_date()) gets the date on server, which uses session timezone.
    // System.currentTimeMillis() is based on jvm timezone. They should not always be equal.
    // We can set local JVM timezone to session timezone to ensure it passes.
    testWithTimezone("GMT") {
      checkAnswer(zero1.select(current_date()), Seq(Row(new Date(System.currentTimeMillis()))))
    }
    testWithTimezone("Etc/GMT+8") {
      checkAnswer(zero1.select(current_date()), Seq(Row(new Date(System.currentTimeMillis()))))
    }
    testWithTimezone("Etc/GMT-8") {
      checkAnswer(zero1.select(current_date()), Seq(Row(new Date(System.currentTimeMillis()))))
    }
  }

  test("current timestamp") {
    testWithTimezone() {
      assert(
        (System.currentTimeMillis() - zero1
          .select(current_timestamp())
          .collect()(0)
          .getTimestamp(0)
          .getTime).abs < 100000)
    }
  }

  test("year month day week quarter") {
    testWithTimezone() {
      checkAnswer(
        date1.select(
          year(col("A")),
          month(col("A")),
          dayofmonth(col("A")),
          dayofweek(col("A")),
          dayofyear(col("A")),
          quarter(col("A")),
          weekofyear(col("A")),
          last_day(col("A"))),
        Seq(
          Row(2020, 8, 1, 6, 214, 3, 31, new Date(120, 7, 31)),
          Row(2010, 12, 1, 3, 335, 4, 48, new Date(110, 11, 31))))
    }
  }

  test("next day") {
    testWithTimezone() {
      checkAnswer(
        date1.select(next_day(col("A"), lit("FR"))),
        Seq(Row(new Date(120, 7, 7)), Row(new Date(110, 11, 3))))
    }
  }

  test("previous day") {
    testWithTimezone() {
      date2.select(previous_day(col("a"), col("b"))).show()
      checkAnswer(
        date2.select(previous_day(col("a"), col("b"))),
        Seq(Row(new Date(120, 6, 27)), Row(new Date(110, 10, 24))))
    }
  }

  test("hour minute second") {
    testWithTimezone() {
      checkAnswer(
        timestamp1.select(hour(col("A")), minute(col("A")), second(col("A"))),
        Seq(Row(13, 11, 20), Row(1, 30, 5)))
    }
  }

  test("datediff") {
    testWithTimezone() {
      checkAnswer(
        timestamp1
          .select(col("a"), dateadd("year", lit(1), col("a")).as("b"))
          .select(datediff("year", col("a"), col("b"))),
        Seq(Row(1), Row(1)))
    }
  }

  test("dateadd") {
    testWithTimezone() {
      checkAnswer(
        date1.select(dateadd("year", lit(1), col("a"))),
        Seq(Row(new Date(121, 7, 1)), Row(new Date(111, 11, 1))))
    }
  }

  test("to_timestamp") {
    testWithTimezone() {
      checkAnswer(
        long1.select(to_timestamp(col("A"))),
        Seq(
          Row(Timestamp.valueOf("2019-06-25 16:19:17.0")),
          Row(Timestamp.valueOf("2019-08-10 23:25:57.0")),
          Row(Timestamp.valueOf("2006-10-22 01:12:37.0"))))

      val df = session.sql("select * from values('04/05/2020 01:02:03') as T(a)")

      checkAnswer(
        df.select(to_timestamp(col("A"), lit("mm/dd/yyyy hh24:mi:ss"))),
        Seq(Row(Timestamp.valueOf("2020-04-05 01:02:03.0"))))
    }
  }

  test("try_to_timestamp") {
    testWithTimezone() {
      val unixDf =
        session.sql("select * from values('1561479557'),('1565479557'),('INVALID') as T(a)")
      val unixDfConverted = unixDf.select(try_to_timestamp(col("A")));
      val unixConvertedExpected = Seq(
        Row(Timestamp.valueOf("2019-06-25 16:19:17.0")),
        Row(Timestamp.valueOf("2019-08-10 23:25:57.0")),
        Row(null))
      checkAnswer(unixDfConverted, unixConvertedExpected)

      val formatDf = session.sql("select * from values('04/05/2020 01:02:03'),('INVALID') as T(a)")
      val formatDfConverted =
        formatDf.select(try_to_timestamp(col("A"), lit("mm/dd/yyyy hh24:mi:ss")))
      val formatConvertedExpected = Seq(Row(Timestamp.valueOf("2020-04-05 01:02:03.0")), Row(null))
      checkAnswer(formatDfConverted, formatConvertedExpected)
    }
  }

  test("convert_timezone") {
    testWithTimezone() {
      checkAnswer(
        timestampNTZ.select(
          convert_timezone(lit("America/Los_Angeles"), lit("America/New_York"), col("a"))),
        Seq(
          Row(Timestamp.valueOf("2020-05-01 16:11:20.0")),
          Row(Timestamp.valueOf("2020-08-21 04:30:05.0"))))

      val df = Seq(("2020-05-01 16:11:20.0 +02:00", "2020-08-21 04:30:05.0 -06:00")).toDF("a", "b")
      checkAnswer(
        df.select(
          convert_timezone(lit("America/Los_Angeles"), col("a")),
          convert_timezone(lit("America/New_York"), col("b"))),
        Seq(
          Row(
            Timestamp.valueOf("2020-05-01 07:11:20.0"),
            Timestamp.valueOf("2020-08-21 06:30:05.0"))))
    }
    // -06:00 -> New_York should be -06:00 -> -04:00, which is +2 hours.
  }

  test("to_date") {
    testWithTimezone() {
      val df = session.sql("select * from values('2020-05-11') as T(a)")
      checkAnswer(df.select(to_date(col("A"))), Seq(Row(new Date(120, 4, 11))))

      val df1 = session.sql("select * from values('2020.07.23') as T(a)")
      checkAnswer(df1.select(to_date(col("A"), lit("YYYY.MM.DD"))), Seq(Row(new Date(120, 6, 23))))
    }
  }

  test("try_to_date") {
    val df = session.sql("select * from values('2020-05-11'),('INVALID') as T(a)")
    checkAnswer(df.select(try_to_date(col("A"))), Seq(Row(new Date(120, 4, 11)), Row(null)))

    val df1 = session.sql("select * from values('2020.07.23'),('INVALID') as T(a)")
    checkAnswer(
      df1.select(try_to_date(col("A"), lit("YYYY.MM.DD"))),
      Seq(Row(new Date(120, 6, 23)), Row(null)))
  }

  test("date_trunc") {
    testWithTimezone() {
      checkAnswer(
        timestamp1.select(date_trunc("quarter", col("A"))),
        Seq(
          Row(Timestamp.valueOf("2020-04-01 00:00:00.0")),
          Row(Timestamp.valueOf("2020-07-01 00:00:00.0"))))
    }
  }

  test("trunc") {
    val df = Seq((3.14, 1)).toDF("expr", "scale")
    checkAnswer(df.select(trunc(col("expr"), col("scale"))), Seq(Row(3.1)))
  }

  test("concat") {
    checkAnswer(
      string1.select(concat(col("A"), col("B"))),
      Seq(Row("test1a"), Row("test2b"), Row("test3c")))
  }

  test("split") {
    assert(
      string5
        .select(split(col("A"), lit(",")))
        .collect()(0)
        .getString(0)
        .replaceAll("[ \n]", "") == "[\"1\",\"2\",\"3\",\"4\",\"5\"]")
  }

  test("contains") {
    checkAnswer(
      string4.select(contains(col("a"), lit("app"))),
      Seq(Row(true), Row(false), Row(false)))
  }

  test("startswith") {
    checkAnswer(
      string4.select(startswith(col("a"), lit("ban"))),
      Seq(Row(false), Row(true), Row(false)))
  }

  test("char") {
    val df = Seq((84, 85), (96, 97)).toDF("A", "B")

    checkAnswer(df.select(char(col("A")), char(col("B"))), Seq(Row("T", "U"), Row("`", "a")))
  }

  test("array_overlap") {
    checkAnswer(
      array1
        .select(arrays_overlap(col("ARR1"), col("ARR2"))),
      Seq(Row(true), Row(false)))
  }

  test("array_intersection") {
    checkAnswer(
      array1.select(array_intersection(col("ARR1"), col("ARR2"))),
      Seq(Row("[\n  3\n]"), Row("[]")))
  }

  test("is_array") {
    checkAnswer(array1.select(is_array(col("ARR1"))), Seq(Row(true), Row(true)))
    checkAnswer(
      variant1.select(is_array(col("arr1")), is_array(col("bool1")), is_array(col("str1"))),
      Seq(Row(true, false, false)))
  }

  test("is_boolean") {
    checkAnswer(
      variant1.select(is_boolean(col("arr1")), is_boolean(col("bool1")), is_boolean(col("str1"))),
      Seq(Row(false, true, false)))
  }

  test("is_binary") {
    checkAnswer(
      variant1.select(is_binary(col("bin1")), is_binary(col("bool1")), is_binary(col("str1"))),
      Seq(Row(true, false, false)))
  }

  test("is_char/is_varchar") {
    checkAnswer(
      variant1.select(is_char(col("str1")), is_char(col("bin1")), is_char(col("bool1"))),
      Seq(Row(true, false, false)))
    checkAnswer(
      variant1.select(is_varchar(col("str1")), is_varchar(col("bin1")), is_varchar(col("bool1"))),
      Seq(Row(true, false, false)))
  }

  test("is_date/is_date_value") {
    checkAnswer(
      variant1.select(is_date(col("date1")), is_date(col("time1")), is_date(col("bool1"))),
      Seq(Row(true, false, false)))
    checkAnswer(
      variant1.select(
        is_date_value(col("date1")),
        is_date_value(col("time1")),
        is_date_value(col("str1"))),
      Seq(Row(true, false, false)))
  }

  test("is_decimal") {
    checkAnswer(
      variant1
        .select(is_decimal(col("decimal1")), is_decimal(col("double1")), is_decimal(col("num1"))),
      Seq(Row(true, false, true)))
  }

  test("is_double/is_real") {
    checkAnswer(
      variant1.select(
        is_double(col("decimal1")),
        is_double(col("double1")),
        is_double(col("num1")),
        is_double(col("bool1"))),
      Seq(Row(true, true, true, false)))

    checkAnswer(
      variant1.select(
        is_real(col("decimal1")),
        is_real(col("double1")),
        is_real(col("num1")),
        is_real(col("bool1"))),
      Seq(Row(true, true, true, false)))
  }

  test("is_integer") {
    checkAnswer(
      variant1.select(
        is_integer(col("decimal1")),
        is_integer(col("double1")),
        is_integer(col("num1")),
        is_integer(col("bool1"))),
      Seq(Row(false, false, true, false)))
  }

  test("is_null_value") {
    checkAnswer(
      nullJson1.select(is_null_value(sqlExpr("v:a"))),
      Seq(Row(true), Row(false), Row(null)))
  }

  test("is_object") {
    checkAnswer(
      variant1.select(is_object(col("obj1")), is_object(col("arr1")), is_object(col("str1"))),
      Seq(Row(true, false, false)))
  }

  test("is_time") {
    checkAnswer(
      variant1
        .select(is_time(col("time1")), is_time(col("date1")), is_time(col("timestamp_tz1"))),
      Seq(Row(true, false, false)))
  }

  test("is_timestamp_*") {
    checkAnswer(
      variant1.select(
        is_timestamp_ntz(col("timestamp_ntz1")),
        is_timestamp_ntz(col("timestamp_tz1")),
        is_timestamp_ntz(col("timestamp_ltz1"))),
      Seq(Row(true, false, false)))

    checkAnswer(
      variant1.select(
        is_timestamp_ltz(col("timestamp_ntz1")),
        is_timestamp_ltz(col("timestamp_tz1")),
        is_timestamp_ltz(col("timestamp_ltz1"))),
      Seq(Row(false, false, true)))

    checkAnswer(
      variant1.select(
        is_timestamp_tz(col("timestamp_ntz1")),
        is_timestamp_tz(col("timestamp_tz1")),
        is_timestamp_tz(col("timestamp_ltz1"))),
      Seq(Row(false, true, false)))
  }

  test("current_region") {
    val result = zero1.select(current_region()).collect()
    assert(result.length == 1)
    // for example, public.aws_us_west_2, qa2, prod3
    assert(result(0).getString(0).length > 0)
  }

  test("current_time") {
    val localtime = zero1.select(current_time()).collect()(0).getTime(0).toString
    assert(localtime.matches("\\d{2}:\\d{2}:\\d{2}"))
  }

  test("current_version") {
    zero1.select(current_version()).show()
    val version = zero1.select(current_version()).collect()(0).getString(0)
    assert(version.matches("\\d+\\.\\d+\\.\\d+.*"))
  }

  test("current_account") {
    val account = zero1.select(current_account()).collect()
    assert(account.length == 1)
  }

  test("current_role") {
    val role = zero1.select(current_role()).collect()
    assert(role.length == 1)
  }

  test("current_statement") {
    assert(
      zero1
        .select(current_statement())
        .collect()(0)
        .getString(0)
        .trim
        .startsWith("SELECT current_statement()"))
  }

  test("current_available_roles") {
    val rolesString = zero1.select(current_available_roles()).collect()(0).getString(0)
    val mapper = new ObjectMapper()
    val rolesJson = mapper.readTree(rolesString)
    assert(rolesJson.isArray)
    val arr = rolesJson.asInstanceOf[ArrayNode]
    val roles = new Array[String](arr.size)
    (0 until arr.size()).foreach(index => roles(index) = arr.get(index).asText())
    val role = zero1.select(current_role()).collect()(0).getString(0)
    assert(roles.contains(role))
  }

  test("current_session") {
    val sessionID = zero1.select(current_session()).collect()(0).getString(0)
    assert(sessionID.matches("\\d+"))
  }

  test("current_user") {
    assert(
      zero1
        .select(current_user())
        .collect()(0)
        .getString(0)
        .equalsIgnoreCase(getUserFromProperties))
  }

  test("current_database") {
    assert(
      zero1
        .select(current_database())
        .collect()(0)
        .getString(0)
        .equalsIgnoreCase(getDatabaseFromProperties.replaceAll("""^"|"$""", "")))
  }

  test("current_schema") {
    assert(
      zero1
        .select(current_schema())
        .collect()(0)
        .getString(0)
        .equalsIgnoreCase(getSchemaFromProperties.replaceAll("""^"|"$""", "")))
  }

  test("current_schemas") {
    val schemasString = zero1.select(current_schemas()).collect()(0).getString(0)
    val mapper = new ObjectMapper()
    val schemasJson = mapper.readTree(schemasString)
    assert(schemasJson.isArray)
    val arr = schemasJson.asInstanceOf[ArrayNode]
    val schemas = new Array[String](arr.size)
    (0 until arr.size()).foreach(index => schemas(index) = arr.get(index).asText())

    val schema = zero1.select(current_schema()).collect()(0).getString(0)
    assert(schemas.exists(_.replaceAll("""^"|"$""", "").endsWith(schema)))
  }

  test("current_warehouse") {
    assert(
      zero1
        .select(current_warehouse())
        .collect()(0)
        .getString(0)
        .equalsIgnoreCase(getWarehouseFromProperties.replaceAll("""^"|"$""", "")))
  }

  test("date_from_parts") {
    testWithTimezone() {
      val df = Seq((2020, 9, 16)).toDF("year", "month", "day")
      val result = df.select(date_from_parts(col("year"), col("month"), col("day")))
      checkAnswer(result, Seq(Row(new Date(120, 8, 16))))
    }
  }

  test("dayname") {
    testWithTimezone() {
      checkAnswer(date1.select(dayname(col("a"))), Seq(Row("Sat"), Row("Wed")))
    }
  }

  test("monthname") {
    testWithTimezone() {
      checkAnswer(date1.select(monthname(col("a"))), Seq(Row("Aug"), Row("Dec")))
    }
  }

  test("endswith") {
    checkAnswer(string4.filter(endswith(col("a"), lit("le"))), Seq(Row("apple")))
  }

  test("insert") {
    checkAnswer(
      string4.select(insert(col("a"), lit(2), lit(3), lit("abc"))),
      Seq(Row("aabce"), Row("babcna"), Row("pabch")))
  }

  test("left") {
    checkAnswer(string4.select(left(col("a"), lit(2))), Seq(Row("ap"), Row("ba"), Row("pe")))
  }

  test("right") {
    checkAnswer(string4.select(right(col("a"), lit(2))), Seq(Row("le"), Row("na"), Row("ch")))
  }

  test("sysdate") {
    assert(
      zero1
        .select(sysdate())
        .collect()(0)
        .getTimestamp(0)
        .toString
        .length > 0)
  }

  test("regexp_count") {
    checkAnswer(string4.select(regexp_count(col("a"), lit("a"))), Seq(Row(1), Row(3), Row(1)))

    checkAnswer(
      string4.select(regexp_count(col("a"), lit("a"), lit(2), lit("c"))),
      Seq(Row(0), Row(3), Row(1)))
  }

  test("replace") {
    checkAnswer(
      string4.select(replace(col("a"), lit("a"))),
      Seq(Row("pple"), Row("bnn"), Row("pech")))

    checkAnswer(
      string4.select(replace(col("a"), lit("a"), lit("z"))),
      Seq(Row("zpple"), Row("bznznz"), Row("pezch")))

  }

  test("time_from_parts") {
    testWithTimezone() {
      assert(
        zero1
          .select(time_from_parts(lit(1), lit(2), lit(3)))
          .collect()(0)
          .getTime(0)
          .equals(new Time(32523000)))

      assert(
        zero1
          .select(time_from_parts(lit(1), lit(2), lit(3), lit(444444444)))
          .collect()(0)
          .getTime(0)
          .equals(new Time(32523444)))
    }
  }

  test("charindex") {
    checkAnswer(string4.select(charindex(lit("na"), col("a"))), Seq(Row(0), Row(3), Row(0)))

    checkAnswer(string4.select(charindex(lit("na"), col("a"), lit(4))), Seq(Row(0), Row(5), Row(0)))
  }

  test("collate") {
    checkAnswer(string3.where(collate(col("a"), "en_US-trim") === "abcba"), Seq(Row("  abcba  ")))
  }

  test("collation") {
    checkAnswer(zero1.select(collation(lit("f").collate("de"))), Seq(Row("de")))
  }

  test("div0") {
    checkAnswer(zero1.select(div0(lit(2), lit(0)), div0(lit(4), lit(2))), Seq(Row(0.0, 2.0)))
  }

  test("timestamp_from_parts") {
    testWithTimezone() {
      assert(
        date3
          .select(
            timestamp_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.0")

      assert(
        date3
          .select(
            timestamp_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second"),
              col("nanosecond")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.001234567")

      assert(
        date3
          .select(timestamp_from_parts(
            date_from_parts(col("year"), col("month"), col("day")),
            time_from_parts(col("hour"), col("minute"), col("second"), col("nanosecond"))))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.001234567")
    }
  }

  test("timestamp_ltz_from_parts") {
    testWithTimezone() {
      assert(
        date3
          .select(
            timestamp_ltz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.0")

      assert(
        date3
          .select(
            timestamp_ltz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second"),
              col("nanosecond")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.001234567")
    }
  }

  test("timestamp_ntz_from_parts") {
    testWithTimezone() {
      assert(
        date3
          .select(
            timestamp_ntz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.0")

      assert(
        date3
          .select(
            timestamp_ntz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second"),
              col("nanosecond")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.001234567")

      assert(
        date3
          .select(timestamp_ntz_from_parts(
            date_from_parts(col("year"), col("month"), col("day")),
            time_from_parts(col("hour"), col("minute"), col("second"), col("nanosecond"))))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.001234567")
    }
  }

  test("timestamp_tz_from_parts") {
    testWithTimezone() {
      assert(
        date3
          .select(
            timestamp_tz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.0")

      assert(
        date3
          .select(
            timestamp_tz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second"),
              col("nanosecond")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.001234567")

      assert(
        date3
          .select(
            timestamp_tz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second"),
              col("nanosecond"),
              col("timezone")))
          .collect()(0)
          .getTimestamp(0)
          .toString == "2020-10-28 13:35:47.001234567")

      assert(
        date3
          .select(
            timestamp_tz_from_parts(
              col("year"),
              col("month"),
              col("day"),
              col("hour"),
              col("minute"),
              col("second"),
              col("nanosecond"),
              lit("America/New_York")))
          .collect()(0)
          .getTimestamp(0)
          .toGMTString == "28 Oct 2020 17:35:47 GMT")
    }
  }

  test("check_json") {
    checkAnswer(nullJson1.select(check_json(col("v"))), Seq(Row(null), Row(null), Row(null)))

    checkAnswer(
      invalidJson1.select(check_json(col("v"))),
      Seq(
        Row("incomplete object value, pos 11"),
        Row("missing colon, pos 7"),
        Row("unfinished string, pos 5")))
  }

  test("check_xml") {
    checkAnswer(
      nullXML1.select(check_xml(col("v"))),
      Seq(Row(null), Row(null), Row(null), Row(null)))

    checkAnswer(
      invalidXML1.select(check_xml(col("v"))),
      Seq(
        Row("no opening tag for </t>, pos 8"),
        Row("missing closing tags: </t1></t1>, pos 8"),
        Row("bad character in XML tag name: '<', pos 4")))
  }

  test("json_extract_path_text") {
    checkAnswer(
      validJson1.select(json_extract_path_text(col("v"), col("k"))),
      Seq(Row(null), Row("foo"), Row(null), Row(null)))
  }

  test("parse_json") {
    checkAnswer(
      nullJson1.select(parse_json(col("v"))),
      Seq(Row("{\n  \"a\": null\n}"), Row("{\n  \"a\": \"foo\"\n}"), Row(null)))
  }

  test("parse_xml") {
    checkAnswer(
      nullXML1.select(parse_xml(col("v"))),
      Seq(
        Row("<t1>\n  foo\n  <t2>bar</t2>\n  <t3></t3>\n</t1>"),
        Row("<t1></t1>"),
        Row(null),
        Row(null)))
  }

  test("strip_null_value") {
    checkAnswer(nullJson1.select(sqlExpr("v:a")), Seq(Row("null"), Row("\"foo\""), Row(null)))

    checkAnswer(
      nullJson1.select(strip_null_value(sqlExpr("v:a"))),
      Seq(Row(null), Row("\"foo\""), Row(null)))
  }

  test("array_agg") {
    assert(
      monthlySales.select(array_agg(col("amount"))).collect()(0).get(0).toString ==
        "[\n  10000,\n  400,\n  4500,\n  35000,\n  5000,\n  3000,\n  200,\n  90500,\n  6000,\n  " +
        "5000,\n  2500,\n  9500,\n  8000,\n  10000,\n  800,\n  4500\n]")
  }

  test("array_agg WITHIN GROUP") {
    assert(
      monthlySales
        .select(array_agg(col("amount")).withinGroup(col("amount")))
        .collect()(0)
        .get(0)
        .toString ==
        "[\n  200,\n  400,\n  800,\n  2500,\n  3000,\n  4500,\n  4500,\n  5000,\n  5000,\n  " +
        "6000,\n  8000,\n  9500,\n  10000,\n  10000,\n  35000,\n  90500\n]")
  }

  test("array_agg WITHIN GROUP ORDER BY DESC") {
    assert(
      monthlySales
        .select(array_agg(col("amount")).withinGroup(col("amount").desc))
        .collect()(0)
        .get(0)
        .toString ==
        "[\n  90500,\n  35000,\n  10000,\n  10000,\n  9500,\n  8000,\n  6000,\n  5000,\n" +
        "  5000,\n  4500,\n  4500,\n  3000,\n  2500,\n  800,\n  400,\n  200\n]")
  }

  test("array_agg WITHIN GROUP ORDER BY multiple columns") {
    val sortColumns = Seq(col("month").asc, col("empid").desc, col("amount"))
    val amountValues = monthlySales.sort(sortColumns).select("amount").collect()
    val expected = "[\n  " + amountValues.map { _.getLong(0).toString }.mkString(",\n  ") + "\n]"
    // println(expected)
    assert(
      monthlySales
        .select(array_agg(col("amount")).withinGroup(sortColumns))
        .collect()(0)
        .get(0)
        .toString == expected)
  }

  test("window function array_agg WITHIN GROUP") {
    val value1 = "[\n  1,\n  3\n]"
    val value2 = "[\n  1,\n  3,\n  10\n]"
    checkAnswer(
      xyz.select(
        array_agg(col("Z"))
          .withinGroup(Seq(col("Z"), col("Y")))
          .over(Window.partitionBy(col("X")))),
      Seq(Row(value1), Row(value1), Row(value2), Row(value2), Row(value2)))
  }

  test("array_append") {
    checkAnswer(
      array1.select(array_append(array_append(col("arr1"), lit("amount")), lit(32.21))),
      Seq(
        Row("[\n  1,\n  2,\n  3,\n  \"amount\",\n  3.221000000000000e+01\n]"),
        Row("[\n  6,\n  7,\n  8,\n  \"amount\",\n  3.221000000000000e+01\n]")))

    // Get array result in List[Variant]
    val resultSet =
      array1.select(array_append(array_append(col("arr1"), lit("amount")), lit(32.21))).collect()
    val row1 = Seq(
      new Variant(1),
      new Variant(2),
      new Variant(3),
      new Variant("amount"),
      new Variant("3.221000000000000e+01"))
    assert(resultSet(0).getSeqOfVariant(0).equals(row1))
    val row2 = Seq(
      new Variant(6),
      new Variant(7),
      new Variant(8),
      new Variant("amount"),
      new Variant("3.221000000000000e+01"))
    assert(resultSet(1).getSeqOfVariant(0).equals(row2))

    checkAnswer(
      array2.select(array_append(array_append(col("arr1"), col("d")), col("e"))),
      Seq(
        Row("[\n  1,\n  2,\n  3,\n  2,\n  \"e1\"\n]"),
        Row("[\n  6,\n  7,\n  8,\n  1,\n  \"e2\"\n]")))
  }

  test("array_cat") {
    checkAnswer(
      array1.select(array_cat(col("arr1"), col("arr2"))),
      Seq(
        Row("[\n  1,\n  2,\n  3,\n  3,\n  4,\n  5\n]"),
        Row("[\n  6,\n  7,\n  8,\n  9,\n  0,\n  1\n]")))
  }

  test("array_compact") {
    checkAnswer(
      nullArray1.select(array_compact(col("arr1"))),
      Seq(Row("[\n  1,\n  3\n]"), Row("[\n  6,\n  8\n]")))
  }

  test("array_construct") {
    assert(
      zero1
        .select(array_construct(lit(1), lit(1.2), lit("string"), lit(""), lit(null)))
        .collect()(0)
        .getString(0) ==
        "[\n  1,\n  1.200000000000000e+00,\n  \"string\",\n  \"\",\n  undefined\n]")

    assert(
      zero1
        .select(array_construct())
        .collect()(0)
        .getString(0) ==
        "[]")

    checkAnswer(
      integer1
        .select(array_construct(col("a"), lit(1.2), lit(null))),
      Seq(
        Row("[\n  1,\n  1.200000000000000e+00,\n  undefined\n]"),
        Row("[\n  2,\n  1.200000000000000e+00,\n  undefined\n]"),
        Row("[\n  3,\n  1.200000000000000e+00,\n  undefined\n]")))
  }

  test("array_construct_compact") {
    assert(
      zero1
        .select(array_construct_compact(lit(1), lit(1.2), lit("string"), lit(""), lit(null)))
        .collect()(0)
        .getString(0) ==
        "[\n  1,\n  1.200000000000000e+00,\n  \"string\",\n  \"\"\n]")

    assert(
      zero1
        .select(array_construct_compact())
        .collect()(0)
        .getString(0) ==
        "[]")

    checkAnswer(
      integer1
        .select(array_construct_compact(col("a"), lit(1.2), lit(null))),
      Seq(
        Row("[\n  1,\n  1.200000000000000e+00\n]"),
        Row("[\n  2,\n  1.200000000000000e+00\n]"),
        Row("[\n  3,\n  1.200000000000000e+00\n]")))
  }

  test("array_contains") {
    assert(
      zero1
        .select(array_contains(lit(1), array_construct(lit(1), lit(1.2), lit("string"))))
        .collect()(0)
        .getBoolean(0))

    assert(
      !zero1
        .select(array_contains(lit(-1), array_construct(lit(1), lit(1.2), lit("string"))))
        .collect()(0)
        .getBoolean(0))

    checkAnswer(
      integer1
        .select(array_contains(col("a"), array_construct(lit(1), lit(1.2), lit("string")))),
      Seq(Row(true), Row(false), Row(false)))
  }

  test("array_insert") {
    checkAnswer(
      array2.select(array_insert(col("arr1"), col("d"), col("e"))),
      Seq(Row("[\n  1,\n  2,\n  \"e1\",\n  3\n]"), Row("[\n  6,\n  \"e2\",\n  7,\n  8\n]")))
  }

  test("array_position") {
    checkAnswer(array2.select(array_position(col("d"), col("arr1"))), Seq(Row(1), Row(null)))
  }

  test("array_prepend") {
    checkAnswer(
      array1.select(array_prepend(array_prepend(col("arr1"), lit("amount")), lit(32.21))),
      Seq(
        Row("[\n  3.221000000000000e+01,\n  \"amount\",\n  1,\n  2,\n  3\n]"),
        Row("[\n  3.221000000000000e+01,\n  \"amount\",\n  6,\n  7,\n  8\n]")))

    checkAnswer(
      array2.select(array_prepend(array_prepend(col("arr1"), col("d")), col("e"))),
      Seq(
        Row("[\n  \"e1\",\n  2,\n  1,\n  2,\n  3\n]"),
        Row("[\n  \"e2\",\n  1,\n  6,\n  7,\n  8\n]")))
  }

  test("array_size") {
    checkAnswer(array2.select(array_size(col("arr1"))), Seq(Row(3), Row(3)))

    checkAnswer(array2.select(array_size(col("d"))), Seq(Row(null), Row(null)))

    checkAnswer(array2.select(array_size(parse_json(col("f")))), Seq(Row(1), Row(2)))
  }

  test("array_slice") {
    checkAnswer(
      array3.select(array_slice(col("arr1"), col("d"), col("e"))),
      Seq(Row("[\n  2\n]"), Row("[\n  5\n]"), Row("[\n  6,\n  7\n]")))
  }

  test("array_to_string") {
    checkAnswer(
      array3.select(array_to_string(col("arr1"), col("f"))),
      Seq(Row("1,2,3"), Row("4, 5, 6"), Row("6;7;8")))
  }

  test("objectagg") {
    checkAnswer(
      object1.select(objectagg(col("key"), col("value"))),
      Seq(Row("{\n  \"age\": 21,\n  \"zip\": 94401\n}")))
  }

  test("object_construct") {
    checkAnswer(
      object1.select(object_construct(col("key"), col("value"))),
      Seq(Row("{\n  \"age\": 21\n}"), Row("{\n  \"zip\": 94401\n}")))

    checkAnswer(object1.select(object_construct()), Seq(Row("{}"), Row("{}")))
  }

  test("object_delete") {
    checkAnswer(
      object2.select(object_delete(col("obj"), col("k"), lit("name"), lit("non-exist-key"))),
      Seq(Row("{\n  \"zip\": 21021\n}"), Row("{\n  \"age\": 26,\n  \"zip\": 94021\n}")))
  }

  test("object_insert") {
    checkAnswer(
      object2.select(object_insert(col("obj"), lit("key"), lit("v"))),
      Seq(
        Row("{\n  \"age\": 21,\n  \"key\": \"v\",\n  \"name\": \"Joe\",\n  \"zip\": 21021\n}"),
        Row("{\n  \"age\": 26,\n  \"key\": \"v\",\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")))

    // Get object result in Map[String, Variant]
    val resultSet = object2.select(object_insert(col("obj"), lit("key"), lit("v"))).collect()
    val row1 = Map(
      "age" -> new Variant(21),
      "key" -> new Variant("v"),
      "name" -> new Variant("Joe"),
      "zip" -> new Variant(21021))
    assert(resultSet(0).getMapOfVariant(0).equals(row1))
    val row2 = Map(
      "age" -> new Variant(26),
      "key" -> new Variant("v"),
      "name" -> new Variant("Jay"),
      "zip" -> new Variant(94021))
    assert(resultSet(1).getMapOfVariant(0).equals(row2))

    checkAnswer(
      object2.select(object_insert(col("obj"), col("k"), col("v"), col("flag"))),
      Seq(
        Row("{\n  \"age\": 0,\n  \"name\": \"Joe\",\n  \"zip\": 21021\n}"),
        Row("{\n  \"age\": 26,\n  \"key\": 0,\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")))
  }

  test("object_pick") {
    checkAnswer(
      object2.select(object_pick(col("obj"), col("k"), lit("name"), lit("non-exist-key"))),
      Seq(Row("{\n  \"age\": 21,\n  \"name\": \"Joe\"\n}"), Row("{\n  \"name\": \"Jay\"\n}")))

    checkAnswer(
      object2.select(object_pick(col("obj"), array_construct(lit("name"), lit("zip")))),
      Seq(
        Row("{\n  \"name\": \"Joe\",\n  \"zip\": 21021\n}"),
        Row("{\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")))
  }

  test("as_array") {
    checkAnswer(
      array1.select(as_array(col("ARR1"))),
      Seq(Row("[\n  1,\n  2,\n  3\n]"), Row("[\n  6,\n  7,\n  8\n]")))
    checkAnswer(
      variant1.select(as_array(col("arr1")), as_array(col("bool1")), as_array(col("str1"))),
      Seq(Row("[\n  \"Example\"\n]", null, null)))
  }

  test("as_binary") {
    checkAnswer(
      variant1.select(as_binary(col("bin1")), as_binary(col("bool1")), as_binary(col("str1"))),
      Seq(Row(Array[Byte](115, 110, 111, 119), null, null)))
  }

  test("as_char/as_varchar") {
    checkAnswer(
      variant1.select(as_char(col("str1")), as_char(col("bin1")), as_char(col("bool1"))),
      Seq(Row("X", null, null)))
    checkAnswer(
      variant1.select(as_varchar(col("str1")), as_varchar(col("bin1")), as_varchar(col("bool1"))),
      Seq(Row("X", null, null)))
  }

  test("as_date") {
    testWithTimezone() {
      checkAnswer(
        variant1.select(as_date(col("date1")), as_date(col("time1")), as_date(col("bool1"))),
        Seq(Row(new Date(117, 1, 24), null, null)))
    }
  }

  test("as_decimal/as_number") {
    checkAnswer(
      variant1
        .select(as_decimal(col("decimal1")), as_decimal(col("double1")), as_decimal(col("num1"))),
      Seq(Row(1, null, 15)))

    assert(
      variant1
        .select(as_decimal(col("decimal1"), 6))
        .collect()(0)
        .getLong(0) == 1)

    assert(
      variant1
        .select(as_decimal(col("decimal1"), 6, 3))
        .collect()(0)
        .getDecimal(0)
        .doubleValue() == 1.23)

    checkAnswer(
      variant1
        .select(as_number(col("decimal1")), as_number(col("double1")), as_number(col("num1"))),
      Seq(Row(1, null, 15)))

    assert(
      variant1
        .select(as_number(col("decimal1"), 6))
        .collect()(0)
        .getLong(0) == 1)

    assert(
      variant1
        .select(as_number(col("decimal1"), 6, 3))
        .collect()(0)
        .getDecimal(0)
        .doubleValue() == 1.23)
  }

  test("as_double/as_real") {
    checkAnswer(
      variant1.select(
        as_double(col("decimal1")),
        as_double(col("double1")),
        as_double(col("num1")),
        as_double(col("bool1"))),
      Seq(Row(1.23, 3.21, 15.0, null)))

    checkAnswer(
      variant1.select(
        as_real(col("decimal1")),
        as_real(col("double1")),
        as_real(col("num1")),
        as_real(col("bool1"))),
      Seq(Row(1.23, 3.21, 15.0, null)))
  }

  test("as_integer") {
    checkAnswer(
      variant1.select(
        as_integer(col("decimal1")),
        as_integer(col("double1")),
        as_integer(col("num1")),
        as_integer(col("bool1"))),
      Seq(Row(1, null, 15, null)))
  }

  test("as_object") {
    checkAnswer(
      variant1.select(as_object(col("obj1")), as_object(col("arr1")), as_object(col("str1"))),
      Seq(Row("{\n  \"Tree\": \"Pine\"\n}", null, null)))
  }

  test("as_time") {
    testWithTimezone() {
      checkAnswer(
        variant1
          .select(as_time(col("time1")), as_time(col("date1")), as_time(col("timestamp_tz1"))),
        Seq(Row(Time.valueOf("20:57:01"), null, null)))
    }
  }

  test("as_timestamp_*") {
    testWithTimezone() {
      checkAnswer(
        variant1.select(
          as_timestamp_ntz(col("timestamp_ntz1")),
          as_timestamp_ntz(col("timestamp_tz1")),
          as_timestamp_ntz(col("timestamp_ltz1"))),
        Seq(Row(Timestamp.valueOf("2017-02-24 12:00:00.456"), null, null)))

      checkAnswer(
        variant1.select(
          as_timestamp_ltz(col("timestamp_ntz1")),
          as_timestamp_ltz(col("timestamp_tz1")),
          as_timestamp_ltz(col("timestamp_ltz1"))),
        Seq(Row(null, null, Timestamp.valueOf("2017-02-24 04:00:00.123"))))

      checkAnswer(
        variant1.select(
          as_timestamp_tz(col("timestamp_ntz1")),
          as_timestamp_tz(col("timestamp_tz1")),
          as_timestamp_tz(col("timestamp_ltz1"))),
        Seq(Row(null, Timestamp.valueOf("2017-02-24 13:00:00.123"), null)))
    }
  }

  test("strtok_to_array") {
    checkAnswer(
      string6
        .select(strtok_to_array(col("a"), col("b"))),
      Seq(
        Row("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]"),
        Row("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]")))

    checkAnswer(
      string6
        .select(strtok_to_array(col("a"))),
      Seq(
        Row("[\n  \"1,2,3,4,5\"\n]"),
        Row("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]")))
  }

  test("to_array") {
    checkAnswer(
      integer1
        .select(to_array(col("a"))),
      Seq(Row("[\n  1\n]"), Row("[\n  2\n]"), Row("[\n  3\n]")))
  }

  test("to_json") {
    checkAnswer(
      integer1
        .select(to_json(col("a"))),
      Seq(Row("1"), Row("2"), Row("3")))

    testWithTimezone() {
      checkAnswer(
        variant1
          .select(to_json(col("time1"))),
        Seq(Row("\"20:57:01\"")))
    }
  }

  test("to_object") {
    checkAnswer(
      variant1
        .select(to_object(col("obj1"))),
      Seq(Row("{\n  \"Tree\": \"Pine\"\n}")))
  }

  test("to_variant") {
    checkAnswer(
      integer1
        .select(to_variant(col("a"))),
      Seq(Row("1"), Row("2"), Row("3")))
    assert(integer1.select(to_variant(col("a"))).collect()(0).getVariant(0).equals(new Variant(1)))
  }

  test("to_xml") {
    checkAnswer(
      integer1
        .select(to_xml(col("a"))),
      Seq(
        Row("<SnowflakeData type=\"INTEGER\">1</SnowflakeData>"),
        Row("<SnowflakeData type=\"INTEGER\">2</SnowflakeData>"),
        Row("<SnowflakeData type=\"INTEGER\">3</SnowflakeData>")))
  }

  test("get") {
    checkAnswer(
      object2
        .select(get(col("obj"), col("k"))),
      Seq(Row("21"), Row(null)))

    checkAnswer(
      object2
        .select(get(col("obj"), lit("AGE"))),
      Seq(Row(null), Row(null)))
  }

  test("get_ignore_case") {
    checkAnswer(
      object2
        .select(get(col("obj"), col("k"))),
      Seq(Row("21"), Row(null)))

    checkAnswer(
      object2
        .select(get_ignore_case(col("obj"), lit("AGE"))),
      Seq(Row("21"), Row("26")))
  }

  test("object_keys") {
    checkAnswer(
      object2
        .select(object_keys(col("obj"))),
      Seq(
        Row("[\n  \"age\",\n  \"name\",\n  \"zip\"\n]"),
        Row("[\n  \"age\",\n  \"name\",\n  \"zip\"\n]")))
  }

  test("xmlget") {
    checkAnswer(
      validXML1
        .select(get(xmlget(col("v"), col("t2")), lit('$'))),
      Seq(Row("\"bar\""), Row(null), Row("\"foo\"")))

    assert(
      validXML1
        .select(get(xmlget(col("v"), col("t2")), lit('$')))
        .collect()(0)
        .getVariant(0)
        .equals(new Variant("\"bar\"")))

    checkAnswer(
      validXML1
        .select(get(xmlget(col("v"), col("t3"), lit('0')), lit('@'))),
      Seq(Row("\"t3\""), Row(null), Row(null)))

    checkAnswer(
      validXML1
        .select(get(xmlget(col("v"), col("t2"), col("instance")), lit('$'))),
      Seq(Row("\"bar\""), Row(null), Row("\"bar\"")))
  }

  test("get_path") {
    checkAnswer(
      validJson1
        .select(get_path(col("v"), col("k"))),
      Seq(Row("null"), Row("\"foo\""), Row(null), Row(null)))
  }

  test("approx_percentile") {
    checkAnswer(approxNumbers.select(approx_percentile(col("a"), 0.5)), Seq(Row(4.5)))
  }

  test("approx_percentile_accumulate") {
    checkAnswer(
      approxNumbers.select(approx_percentile_accumulate(col("a"))),
      Seq(Row("{\n  \"state\": [\n    0.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
        "1.000000000000000e+00,\n    1.000000000000000e+00,\n    2.000000000000000e+00,\n    " +
        "1.000000000000000e+00,\n    3.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
        "4.000000000000000e+00,\n    1.000000000000000e+00,\n    5.000000000000000e+00,\n    " +
        "1.000000000000000e+00,\n    6.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
        "7.000000000000000e+00,\n    1.000000000000000e+00,\n    8.000000000000000e+00,\n    " +
        "1.000000000000000e+00,\n    9.000000000000000e+00,\n    1.000000000000000e+00\n  ],\n  " +
        "\"type\": \"tdigest\",\n  \"version\": 1\n}")))
  }

  test("approx_percentile_estimate") {
    checkAnswer(
      approxNumbers.select(approx_percentile_estimate(approx_percentile_accumulate(col("a")), 0.5)),
      approxNumbers.select(approx_percentile(col("a"), 0.5)))
  }

  test("approx_percentile_combine") {
    val df1 = approxNumbers
      .select(col("a"))
      .where($"a" >= 3)
      .select(approx_percentile_accumulate(col("a")).as("b"))
    val df2 = approxNumbers.select(approx_percentile_accumulate(col("a")).as("b"))
    val df = df1.union(df2)
    print(df.collect()(0))
    checkAnswer(
      df.select(approx_percentile_combine(col("b"))),
      Seq(
        Row("{\n  \"state\": [\n    0.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
          "1.000000000000000e+00,\n    1.000000000000000e+00,\n    2.000000000000000e+00,\n    " +
          "1.000000000000000e+00,\n    3.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
          "3.000000000000000e+00,\n    1.000000000000000e+00,\n    4.000000000000000e+00,\n    " +
          "1.000000000000000e+00,\n    4.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
          "5.000000000000000e+00,\n    1.000000000000000e+00,\n    5.000000000000000e+00,\n    " +
          "1.000000000000000e+00,\n    6.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
          "6.000000000000000e+00,\n    1.000000000000000e+00,\n    7.000000000000000e+00,\n    " +
          "1.000000000000000e+00,\n    7.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
          "8.000000000000000e+00,\n    1.000000000000000e+00,\n    8.000000000000000e+00,\n    " +
          "1.000000000000000e+00,\n    9.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
          "9.000000000000000e+00,\n    1.000000000000000e+00\n  ],\n  \"type\": \"tdigest\",\n  " +
          "\"version\": 1\n}")))
  }

  test("toScalar(DataFrame) with SELECT") {
    // Re-use testData1 and nullData1 for testing
    // testData1.show()
    // nullData1.show()

    // Use 1 col(DataFrame) in SELECT column list
    val df = nullData1.filter(col("a") === 3)
    val subQuery = toScalar(df)
    var expectedResult = Seq(Row(1, true, 3), Row(2, false, 3))
    checkAnswer(testData1.select(col("num"), col("bool"), subQuery), expectedResult)
    checkAnswer(testData1.select(col("num"), col("bool"), subQuery.as("abc")), expectedResult)

    // Use 2 toScalar(DataFrame) in SELECT column list
    val dfMax = nullData1.agg(max(col("a")))
    val dfMin = nullData1.agg(min(col("a")))
    expectedResult = Seq(Row(1, 3, 1), Row(2, 3, 1))
    checkAnswer(testData1.select(col("num"), toScalar(dfMax), toScalar(dfMin)), expectedResult)
    // Test difference column sequence
    expectedResult = Seq(Row(3, 1, 1), Row(3, 2, 1))
    checkAnswer(testData1.select(toScalar(dfMax), col("num"), toScalar(dfMin)), expectedResult)

    // Test Column operation such as +/- on toScalar(DataFrame)
    expectedResult = Seq(Row(1 + 3, 3 - 1), Row(2 + 3, 3 - 1))
    checkAnswer(
      testData1.select(col("num") + toScalar(dfMax), toScalar(dfMax) - toScalar(dfMin)),
      expectedResult)
  }

  test("col(DataFrame) with SELECT") {
    // Re-use testData1 and nullData1 for testing
    // testData1.show()
    // nullData1.show()

    // Use 1 col(DataFrame) in SELECT column list
    val df = nullData1.filter(col("a") === 3)
    val subQuery = col(df)
    var expectedResult = Seq(Row(1, true, 3), Row(2, false, 3))
    checkAnswer(testData1.select(col("num"), col("bool"), subQuery), expectedResult)
    checkAnswer(testData1.select(col("num"), col("bool"), subQuery.as("abc")), expectedResult)

    // Use 2 col(DataFrame) in SELECT column list
    val dfMax = nullData1.agg(max(col("a")))
    val dfMin = nullData1.agg(min(col("a")))
    expectedResult = Seq(Row(1, 3, 1), Row(2, 3, 1))
    checkAnswer(testData1.select(col("num"), col(dfMax), col(dfMin)), expectedResult)
    // Test difference column sequence
    expectedResult = Seq(Row(3, 1, 1), Row(3, 2, 1))
    checkAnswer(testData1.select(col(dfMax), col("num"), col(dfMin)), expectedResult)

    // Test Column operation such as +/- on col(DataFrame)
    expectedResult = Seq(Row(1 + 3, 3 - 1), Row(2 + 3, 3 - 1))
    checkAnswer(testData1.select(col("num") + col(dfMax), col(dfMax) - col(dfMin)), expectedResult)
  }

  test("col(DataFrame) with WHERE") {
    // Re-use testData1 and nullData1 for testing
    // testData1.show()
    // nullData1.show()

    // Use 1 col(DataFrame)in WHERE clause
    val df = nullData1.filter(col("a") === 2)
    val subQuery = col(df)
    var expectedResult = Seq(Row(1, true, "a"))
    checkAnswer(testData1.filter(col("num") < subQuery), expectedResult)
    expectedResult = Seq(Row(2, false, "b"))
    checkAnswer(testData1.filter(col("num") >= subQuery), expectedResult)

    // Use 2 col(DataFrame) in SELECT column list
    val dfMax = nullData1.agg(max(col("a")))
    val dfMin = nullData1.agg(min(col("a")))
    expectedResult = Seq(Row(2, false, "b"))
    checkAnswer(
      testData1.filter(col("num") > col(dfMin) && col("num") < col(dfMax)),
      expectedResult)
    expectedResult = Seq(Row(1, true, "a"))
    checkAnswer(
      testData1.filter(col("num") >= col(dfMin) && col("num") < col(dfMax) - 1),
      expectedResult)
    // empty result
    assert(
      testData1
        .filter(col("num") < col(dfMin) && col("num") > col(dfMax))
        .count() === 0)
  }

  test("col(DataFrame) negative test") {
    // col(DataFrame) needs to be used for single column data frame.
    assertThrows[SnowparkClientException]({
      col(testData1)
    })

    // If multiple rows are returned, error happens.
    assertThrows[SnowflakeSQLException]({
      testData1.select(col("num"), col(nullData1)).show()
    })
    assertThrows[SnowflakeSQLException]({
      testData1.filter(col("num") < col(nullData1)).show()
    })
  }

  test("iff") {
    val df =
      Seq((true, 2, 2, 4), (false, 12, 12, 14), (true, 22, 23, 24)).toDF("a", "b", "c", "d")

    val df1 = df.select(df("a"), df("b"), df("d"), iff(col("a"), df("b"), df("d")))
    assert(
      getShowString(df1, 10, 50) ==
        """--------------------------------------------------
          ||"A"    |"B"  |"D"  |"IFF(""A"", ""B"", ""D"")"  |
          |--------------------------------------------------
          ||true   |2    |4    |2                           |
          ||false  |12   |14   |14                          |
          ||true   |22   |24   |22                          |
          |--------------------------------------------------
          |""".stripMargin)

    val df2 = df.select(df("b"), df("c"), df("d"), iff(col("b") === col("c"), df("b"), df("d")))
    assert(
      getShowString(df2, 10, 50) ==
        """----------------------------------------------------------
          ||"B"  |"C"  |"D"  |"IFF((""B"" = ""C""), ""B"", ""D"")"  |
          |----------------------------------------------------------
          ||2    |2    |4    |2                                     |
          ||12   |12   |14   |12                                    |
          ||22   |23   |24   |24                                    |
          |----------------------------------------------------------
          |""".stripMargin)
  }

  test("seq") {
    checkAnswer(session.generator(5, Seq(seq1())), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(session.generator(5, Seq(seq2())), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(session.generator(5, Seq(seq4())), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(session.generator(5, Seq(seq8())), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))

    // test wrap-around
    assert(
      session
        .generator(Byte.MaxValue.toInt + 10, Seq(seq1(false).as("a")))
        .where(col("a") < 0)
        .count() > 0)
    assert(
      session
        .generator(Byte.MaxValue.toInt + 10, Seq(seq1().as("a")))
        .where(col("a") < 0)
        .count() == 0)
    assert(
      session
        .generator(Short.MaxValue.toInt + 10, Seq(seq2(false).as("a")))
        .where(col("a") < 0)
        .count() > 0)
    assert(
      session
        .generator(Short.MaxValue.toInt + 10, Seq(seq2().as("a")))
        .where(col("a") < 0)
        .count() == 0)
    assert(
      session
        .generator(Int.MaxValue.toLong + 10, Seq(seq4(false).as("a")))
        .where(col("a") < 0)
        .count() > 0)
    assert(
      session
        .generator(Int.MaxValue.toLong + 10, Seq(seq4().as("a")))
        .where(col("a") < 0)
        .count() == 0)
    // do not test the wrap-around of seq8, too costly.

    // test range
    assert(
      session
        .generator(Byte.MaxValue.toLong + 10, Seq(seq1(false).as("a")))
        .where(col("a") > Byte.MaxValue)
        .count() == 0)
    assert(
      session
        .generator(Byte.MaxValue.toLong + 10, Seq(seq2(false).as("a")))
        .where(col("a") > Byte.MaxValue)
        .count() > 0)
    assert(
      session
        .generator(Short.MaxValue.toLong + 10, Seq(seq4(false).as("a")))
        .where(col("a") > Short.MaxValue)
        .count() > 0)
    assert(
      session
        .generator(Int.MaxValue.toLong + 10, Seq(seq8(false).as("a")))
        .where(col("a") > Int.MaxValue)
        .count() > 0)
  }

  test("uniform") {
    val result = session.generator(5, uniform(lit(1), lit(5), random())).collect()
    assert(result.length == 5)
    result.foreach(row => {
      assert(row.size == 1)
      assert(row.getInt(0) >= 1)
      assert(row.getInt(0) <= 5)
    })
  }

  test("listagg") {
    val df = Seq(1, 2, 3, 2, 4, 5).toDF("col")
    checkAnswer(
      df.select(
        listagg(df.col("col"))
          .withinGroup(df.col("col").asc)),
      Seq(Row("122345")))
    checkAnswer(
      df.select(
        listagg(df.col("col"), ",")
          .withinGroup(df.col("col").asc)),
      Seq(Row("1,2,2,3,4,5")))
    checkAnswer(
      df.select(
        listagg(df.col("col"), ",", isDistinct = true)
          .withinGroup(df.col("col").asc)),
      Seq(Row("1,2,3,4,5")))

    // delimiter is '
    checkAnswer(
      df.select(
        listagg(df.col("col"), "'", isDistinct = true)
          .withinGroup(df.col("col").asc)),
      Seq(Row("1'2'3'4'5")))
  }

  test("regexp_replace") {
    val data = Seq("cat", "dog", "mouse").toDF("a")
    val pattern = lit("^ca|^[m|d]o")
    var expected = Seq(Row("t"), Row("g"), Row("use"))
    checkAnswer(data.select(regexp_replace(data("a"), pattern)), expected)

    val replacement = lit("ch")
    expected = Seq(Row("cht"), Row("chg"), Row("chuse"))
    checkAnswer(data.select(regexp_replace(data("a"), pattern, replacement)), expected)
  }
  test("regexp_extract") {
    val data = Seq("A MAN A PLAN A CANAL").toDF("a")
    var expected = Seq(Row("MAN"))
    checkAnswer(data.select(regexp_extract(col("a"), "A\\W+(\\w+)", 1, 1, 1)), expected)
    expected = Seq(Row("PLAN"))
    checkAnswer(data.select(regexp_extract(col("a"), "A\\W+(\\w+)", 1, 2, 1)), expected)
    expected = Seq(Row("CANAL"))
    checkAnswer(data.select(regexp_extract(col("a"), "A\\W+(\\w+)", 1, 3, 1)), expected)

  }
  test("signum") {
    val df = Seq(1).toDF("a")
    checkAnswer(df.select(sign(col("a"))), Seq(Row(1)))
    val df1 = Seq(-2).toDF("a")
    checkAnswer(df1.select(sign(col("a"))), Seq(Row(-1)))
    val df2 = Seq(0).toDF("a")
    checkAnswer(df2.select(sign(col("a"))), Seq(Row(0)))
  }
  test("sign") {
    val df = Seq(1).toDF("a")
    checkAnswer(df.select(sign(col("a"))), Seq(Row(1)))
    val df1 = Seq(-2).toDF("a")
    checkAnswer(df1.select(sign(col("a"))), Seq(Row(-1)))
    val df2 = Seq(0).toDF("a")
    checkAnswer(df2.select(sign(col("a"))), Seq(Row(0)))
  }

  test("substring_index") {
    val df = Seq("It was the best of times, it was the worst of times").toDF("a")
    checkAnswer(
      df.select(substring_index("It was the best of times, it was the worst of times", "was", 1)),
      Seq(Row("It was ")))
  }

  test("desc column order") {
    val input = Seq(1, 2, 3).toDF("data")
    val expected = Seq(3, 2, 1).toDF("data")

    val inputStr = Seq("a", "b", "c").toDF("dataStr")
    val expectedStr = Seq("c", "b", "a").toDF("dataStr")

    checkAnswer(input.sort(desc("data")), expected)
    checkAnswer(inputStr.sort(desc("dataStr")), expectedStr)
  }

  test("asc column order") {
    val input = Seq(3, 2, 1).toDF("data")
    val expected = Seq(1, 2, 3).toDF("data")

    val inputStr = Seq("c", "b", "a").toDF("dataStr")
    val expectedStr = Seq("a", "b", "c").toDF("dataStr")

    checkAnswer(input.sort(asc("data")), expected)
    checkAnswer(inputStr.sort(asc("dataStr")), expectedStr)
  }

  test("column array size") {

    val input = Seq(Array(1, 2, 3)).toDF("size")
    val expected = Seq((3)).toDF("size")
    checkAnswer(input.select(size(col("size"))), expected)
  }

  test("expr function") {

    val input = Seq(1, 2, 3).toDF("id")
    val expected = Seq((3)).toDF("id")
    checkAnswer(input.filter(expr("id > 2")), expected)
  }

  test("array function") {

    val input = Seq((1, 2, 3), (4, 5, 6)).toDF("a", "b", "c")
    val expected = Seq(Array(1, 2), Array(4, 5)).toDF("id")
    checkAnswer(input.select(array(col("a"), col("b")).as("id")), expected)
  }

  test("date format function") {
    testWithTimezone() {
      val input = Seq("2023-10-10", "2022-05-15").toDF("date")
      val expected = Seq("2023/10/10", "2022/05/15").toDF("formatted_date")

      checkAnswer(
        input.select(date_format(col("date"), "YYYY/MM/DD").as("formatted_date")),
        expected)
    }
  }

  test("last function") {
    val input =
      Seq((5, "a", 10), (5, "b", 20), (3, "d", 15), (3, "e", 40)).toDF("grade", "name", "score")
    val window = Window.partitionBy(col("grade")).orderBy(col("score").desc)
    val expected = Seq("a", "a", "d", "d").toDF("last_score_name")

    checkAnswer(input.select(last(col("name")).over(window).as("last_score_name")), expected)
  }

  test("log10 Column function") {
    val input = session.createDataFrame(Seq(100)).toDF("a")
    val expected = Seq(2.0).toDF("log10")
    checkAnswer(input.select(log10(col("a")).as("log10")), expected)
  }

  test("log10 String function") {
    val input = session.createDataFrame(Seq("100")).toDF("a")
    val expected = Seq(2.0).toDF("log10")
    checkAnswer(input.select(log10("a").as("log10")), expected)
  }

  test("log1p Column function") {
    val input = session.createDataFrame(Seq(0.1)).toDF("a")
    val expected = Seq(0.09531017980432493).toDF("log1p")
    checkAnswer(input.select(log1p(col("a")).as("log10")), expected)
  }

  test("log1p String function") {
    val input = session.createDataFrame(Seq(0.1)).toDF("a")
    val expected = Seq(0.09531017980432493).toDF("log1p")
    checkAnswer(input.select(log1p("a").as("log1p")), expected)
  }

  test("base64 function") {
    val input = session.createDataFrame(Seq("test")).toDF("a")
    val expected = Seq("dGVzdA==").toDF("base64")
    checkAnswer(input.select(base64(col("a")).as("base64")), expected)
  }

  test("unbase64 function") {
    val input = session.createDataFrame(Seq("dGVzdA==")).toDF("a")
    val expected = Seq("test").toDF("unbase64")
    checkAnswer(input.select(unbase64(col("a")).as("unbase64")), expected)
  }

  test("reverse") {
    val data = Seq("cat").toDF("a")
    checkAnswer(data.select(reverse(col("a"))), Seq(Row("tac")))
  }

  test("isnull") {
    checkAnswer(
      nanData1.select(equal_nan(col("A")), isnull(col("A"))),
      Seq(Row(false, false), Row(true, false), Row(null, true), Row(false, false)))
  }

  test("unix_timestamp") {
    testWithTimezone() {
      val expected = Seq(1368056360).toDF("epoch")
      val data = Seq(Timestamp.valueOf("2013-05-08 23:39:20.123")).toDF("a")
      checkAnswer(data.select(unix_timestamp(col("a"))), expected)
    }
  }

  test("locate Column function") {
    val input =
      session.createDataFrame(Seq(("scala", "java scala python"), ("b", "abcd"))).toDF("a", "b")
    val expected = Seq((6), (2)).toDF("locate")
    checkAnswer(input.select(locate(col("a"), col("b"), 1).as("locate")), expected)
  }

  test("locate String function") {

    val input = session.createDataFrame(Seq("java scala python")).toDF("a")
    val expected = Seq(6).toDF("locate")
    checkAnswer(input.select(locate("scala", col("a")).as("locate")), expected)
  }

  test("ntile function") {
    val input = Seq((5, 15), (5, 15), (5, 15), (5, 20)).toDF("grade", "score")
    val window = Window.partitionBy(col("grade")).orderBy(col("score"))
    val expected = Seq((1), (1), (2), (2)).toDF("ntile")
    checkAnswer(input.select(ntile(2).over(window).as("ntile")), expected)
  }

  test("randn seed function") {
    val input = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
    val expected = Seq((5777523539921853504L), (-8190739547906189845L), (-1138438814981368515L))
      .toDF("randn_with_seed")
    val df = input.withColumn("randn_with_seed", randn(123L)).select("randn_with_seed")

    checkAnswer(df, expected)
  }

  test("randn function") {
    val input = session.createDataFrame(Seq((1), (2), (3))).toDF("a")

    assert(input.withColumn("randn", randn()).select("randn").first() != null)
  }

  test("date_add1") {
    testWithTimezone() {
      checkAnswer(
        date1.select(date_add(col("a"), lit(1))),
        Seq(Row(Date.valueOf("2020-08-02")), Row(Date.valueOf("2010-12-02"))))
    }
  }

  test("date_add2") {
    testWithTimezone() {
      checkAnswer(
        date1.select(date_add(1, col("a"))),
        Seq(Row(Date.valueOf("2020-08-02")), Row(Date.valueOf("2010-12-02"))))
    }
  }

  test("collect_set") {
    array1.select(collect_set(col("ARR1"))).show()
  }

  test("from_unixtime_1") {
    testWithTimezone() {
      val input = Seq("20231010", "20220515").toDF("date")
      checkAnswer(
        input.select(from_unixtime(col("date")).as("formatted_date")),
        Seq(Row("1970-08-23 03:43:30.000"), Row("1970-08-23 00:48:35.000")))
    }
  }

  test("from_unixtime_2") {
    testWithTimezone() {
      val input = Seq("20231010", "456700809").toDF("date")
      val expected = Seq("1970/08/23", "1984/06/21").toDF("formatted_date")

      checkAnswer(
        input.select(from_unixtime(col("date"), "YYYY/MM/DD").as("formatted_date")),
        expected)
    }
  }

  test("monotonically_increasing_id") {
    checkAnswer(
      session.generator(5, Seq(monotonically_increasing_id())),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
  }

  test("shiftleft") {
    val input = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
    checkAnswer(input.select(shiftleft(col("A"), 1)), Seq(Row(2), Row(4), Row(6)))
  }

  test("shiftright") {
    val input = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
    checkAnswer(input.select(shiftright(col("A"), 1)), Seq(Row(0), Row(1), Row(1)))
  }

  test("hex") {
    val input = session.createDataFrame(Seq((1), (2), (3))).toDF("a")
    checkAnswer(
      input.withColumn("hex_col", hex(col("A"))).select("hex_col"),
      Seq(Row("31"), Row("32"), Row("33")))
  }

  test("unhex") {
    val input = session.createDataFrame(Seq((31), (32), (33))).toDF("a")
    checkAnswer(
      input.withColumn("unhex_col", unhex(col("A"))).select("unhex_col"),
      Seq(Row("1"), Row("2"), Row("3")))
  }
  test("months_between") {
    testWithTimezone() {
      val months_between = functions.builtin("MONTHS_BETWEEN")
      val input = Seq(
        (Date.valueOf("2010-08-02"), Date.valueOf("2010-07-02")),
        (Date.valueOf("2020-12-02"), Date.valueOf("2020-08-02")))
        .toDF("a", "b")
      checkAnswer(
        input.select(months_between(col("a"), col("b"))),
        Seq(Row((1.000000)), Row(4.000000)))
    }
  }

  test("instr") {
    val df = Seq("It was the best of times, it was the worst of times").toDF("a")
    checkAnswer(df.select(instr(col("a"), "was")), Seq(Row(4)))
  }

  test("format_number1") {

    checkAnswer(
      number3.select(ltrim(format_number(col("a"), 0))),
      Seq(Row(("1")), Row(("2")), Row(("3"))))
  }

  test("format_number2") {

    checkAnswer(
      number3.select(ltrim(format_number(col("a"), 2))),
      Seq(Row(("1.00")), Row(("2.00")), Row(("3.00"))))
  }

  test("format_number3") {

    checkAnswer(
      number3.select(ltrim(format_number(col("a"), -1))),
      Seq(Row((null)), Row((null)), Row((null))))
  }

  test("from_utc_timestamp") {
    testWithTimezone() {
      val expected = Seq(Timestamp.valueOf("2024-04-05 01:02:03.0")).toDF("a")
      val data = Seq("2024-04-05 01:02:03").toDF("a")
      checkAnswer(data.select(from_utc_timestamp(col("a"))), expected)
    }
  }

  test("to_utc_timestamp") {
    testWithTimezone() {
      val expected = Seq(Timestamp.valueOf("2024-04-05 01:02:03.0")).toDF("a")
      val data = Seq("2024-04-05 01:02:03").toDF("a")
      checkAnswer(data.select(to_utc_timestamp(col("a"))), expected)
    }
  }
}

class EagerFunctionSuite extends FunctionSuite with EagerSession

class LazyFunctionSuite extends FunctionSuite with LazySession
