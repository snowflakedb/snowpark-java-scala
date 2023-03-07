package com.snowflake.snowpark_test

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions.{repeat, _}
import com.snowflake.snowpark.types._
import net.snowflake.client.jdbc.SnowflakeSQLException

import java.sql.{Date, Time, Timestamp}
import scala.collection.JavaConverters._

trait FunctionSuite extends TestData {
  import session.implicits._

  test("col") {
    checkAnswer(testData1.select(col("bool")), Seq(Row(true), Row(false)), sort = false)
    checkAnswer(testData1.select(column("num")), Seq(Row(1), Row(2)), sort = false)
  }

  test("lit") {
    checkAnswer(testData1.select(lit(1)), Seq(Row(1), Row(1)), sort = false)
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
        Row(null, 1, 1, 0, 2)),
      sort = false)
  }

  test("kurtosis") {
    checkAnswer(
      xyz.select(kurtosis(col("X")), kurtosis(col("Y")), kurtosis(col("Z"))),
      Seq(Row(-3.333333333333, 5.000000000000, 3.613736609956)))
  }

  test("max, min, mean") {
    checkAnswer(
      xyz.select(max(col("X")), min(col("Y")), mean(col("Z"))),
      Seq(Row(2, 1, 3.600000)))
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
      Seq(Row(3, 6), Row(2, 4), Row(1, 1)),
      sort = false)

    checkAnswer(
      duplicatedNumbers.groupBy("A").agg(sum_distinct(col("A"))),
      Seq(Row(3, 3), Row(2, 2), Row(1, 1)),
      sort = false)
  }

  test("variance") {
    checkAnswer(
      xyz.groupBy("X").agg(variance(col("Y")), var_pop(col("Z")), var_samp(col("Z"))),
      Seq(Row(1, 0.000000, 1.000000, 2.000000), Row(2, 0.333333, 14.888889, 22.333333)),
      sort = false)
  }

  test("cume_dist") {
    checkAnswer(
      xyz.select(cume_dist().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(0.3333333333333333), Row(1.0), Row(1.0), Row(1.0), Row(1.0)),
      sort = false)
  }

  test("dense_rank") {
    checkAnswer(
      xyz.select(dense_rank().over(Window.orderBy(col("X")))),
      Seq(Row(1), Row(1), Row(2), Row(2), Row(2)),
      sort = false)
  }

  test("lag") {
    checkAnswer(
      xyz.select(lag(col("Z"), 1, lit(0)).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(0), Row(10), Row(1), Row(0), Row(1)),
      sort = false)

    checkAnswer(
      xyz.select(lag(col("Z"), 1).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(null), Row(10), Row(1), Row(null), Row(1)),
      sort = false)

    checkAnswer(
      xyz.select(lag(col("Z")).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(null), Row(10), Row(1), Row(null), Row(1)),
      sort = false)
  }

  test("lead") {
    checkAnswer(
      xyz.select(lead(col("Z"), 1, lit(0)).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(1), Row(3), Row(0), Row(3), Row(0)),
      sort = false)

    checkAnswer(
      xyz.select(lead(col("Z"), 1).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(1), Row(3), Row(null), Row(3), Row(null)),
      sort = false)

    checkAnswer(
      xyz.select(lead(col("Z")).over(Window.partitionBy(col("X")).orderBy(col("X")))),
      Seq(Row(1), Row(3), Row(null), Row(3), Row(null)),
      sort = false)
  }

  test("ntile") {
    val df = xyz.withColumn("n", lit(4))
    checkAnswer(
      df.select(ntile(col("n")).over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(1), Row(2), Row(3), Row(1), Row(2)),
      sort = false)
  }

  test("percent_rank") {
    checkAnswer(
      xyz.select(percent_rank().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(0.0), Row(0.5), Row(0.5), Row(0.0), Row(0.0)),
      sort = false)
  }

  test("rank") {
    checkAnswer(
      xyz.select(rank().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(1), Row(2), Row(2), Row(1), Row(1)),
      sort = false)
  }

  test("row_number") {
    checkAnswer(
      xyz.select(row_number().over(Window.partitionBy(col("X")).orderBy(col("Y")))),
      Seq(Row(1), Row(2), Row(3), Row(1), Row(2)),
      sort = false)
  }

  test("coalesce") {
    checkAnswer(
      nullData2.select(coalesce(col("A"), col("B"), col("C"))),
      Seq(Row(1), Row(2), Row(3), Row(null), Row(1), Row(1), Row(1)),
      sort = false)
  }

  test("NaN and Null") {
    checkAnswer(
      nanData1.select(equal_nan(col("A")), is_null(col("A"))),
      Seq(Row(false, false), Row(true, false), Row(null, true), Row(false, false)),
      sort = false)
  }

  test("negate and not") {
    val df = session.sql("select * from values(1, true),(-2,false) as T(a,b)")
    checkAnswer(
      df.select(negate(col("A")), not(col("B"))),
      Seq(Row(-1, false), Row(2, true)),
      sort = false)
  }

  test("random") {
    val df = session.sql("select 1")
    df.select(random(123)).collect()
    df.select(random()).collect()
  }

  test("sqrt") {
    checkAnswer(
      testData1.select(sqrt(col("NUM"))),
      Seq(Row(1.0), Row(1.4142135623730951)),
      sort = false)
  }

  test("bitwise not") {
    checkAnswer(testData1.select(bitnot(col("NUM"))), Seq(Row(-2), Row(-3)), sort = false)
  }

  test("abs") {
    checkAnswer(number2.select(abs(col("X"))), Seq(Row(1), Row(0), Row(5)), sort = false)
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
      Seq(Row(2), Row(3), Row(10), Row(2), Row(3)),
      sort = false)

    checkAnswer(
      xyz.select(least(col("X"), col("Y"), col("Z"))),
      Seq(Row(1), Row(1), Row(1), Row(1), Row(2)),
      sort = false)
  }

  test("round") {
    checkAnswer(double1.select(round(col("A"))), Seq(Row(1.0), Row(2.0), Row(3.0)))
    checkAnswer(double1.select(round(col("A"), lit(0))), Seq(Row(1.0), Row(2.0), Row(3.0)))

  }

  test("asin acos") {
    checkAnswer(
      double2.select(acos(col("A")), asin(col("A"))),
      Seq(
        Row(1.4706289056333368, 0.1001674211615598),
        Row(1.369438406004566, 0.2013579207903308),
        Row(1.2661036727794992, 0.3046926540153975)),
      sort = false)

    checkAnswer(
      double2.select(acos(col("A")), asin(col("A"))),
      Seq(
        Row(1.4706289056333368, 0.1001674211615598),
        Row(1.369438406004566, 0.2013579207903308),
        Row(1.2661036727794992, 0.3046926540153975)),
      sort = false)
  }

  test("atan atan2") {
    checkAnswer(
      double2.select(atan(col("B")), atan(col("A"))),
      Seq(
        Row(0.4636476090008061, 0.09966865249116204),
        Row(0.5404195002705842, 0.19739555984988078),
        Row(0.6107259643892086, 0.2914567944778671)),
      sort = false)

    checkAnswer(
      double2
        .select(atan2(col("B"), col("A"))),
      Seq(Row(1.373400766945016), Row(1.2490457723982544), Row(1.1659045405098132)),
      sort = false)
  }

  test("cos cosh") {
    checkAnswer(
      double2.select(cos(col("A")), cos(col("A")), cosh(col("B")), cosh(col("B"))),
      Seq(
        Row(0.9950041652780258, 0.9950041652780258, 1.1276259652063807, 1.1276259652063807),
        Row(0.9800665778412416, 0.9800665778412416, 1.1854652182422676, 1.1854652182422676),
        Row(0.955336489125606, 0.955336489125606, 1.255169005630943, 1.255169005630943)),
      sort = false)
  }

  test("exp") {
    checkAnswer(
      number2.select(exp(col("X")), exp(col("X"))),
      Seq(
        Row(2.718281828459045, 2.718281828459045),
        Row(1.0, 1.0),
        Row(0.006737946999085467, 0.006737946999085467)),
      sort = false)
  }

  test("factorial") {
    checkAnswer(integer1.select(factorial(col("A"))), Seq(Row(1), Row(2), Row(6)), sort = false)
  }

  test("log") {
    checkAnswer(
      integer1.select(log(lit(2), col("A")), log(lit(4), col("A"))),
      Seq(Row(0.0, 0.0), Row(1.0, 0.5), Row(1.5849625007211563, 0.7924812503605781)),
      sort = false)
  }

  test("pow") {
    checkAnswer(
      double2.select(pow(col("A"), col("B"))),
      Seq(Row(0.31622776601683794), Row(0.3807307877431757), Row(0.4305116202499342)),
      sort = false)
  }
  test("shiftleft shiftright") {
    checkAnswer(
      integer1.select(bitshiftleft(col("A"), lit(1)), bitshiftright(col("A"), lit(1))),
      Seq(Row(2, 0), Row(4, 1), Row(6, 1)),
      sort = false)
  }

  test("sin sinh") {
    checkAnswer(
      double2.select(sin(col("A")), sin(col("A")), sinh(col("A")), sinh(col("A"))),
      Seq(
        Row(0.09983341664682815, 0.09983341664682815, 0.10016675001984403, 0.10016675001984403),
        Row(0.19866933079506122, 0.19866933079506122, 0.20133600254109402, 0.20133600254109402),
        Row(0.29552020666133955, 0.29552020666133955, 0.3045202934471426, 0.3045202934471426)),
      sort = false)
  }

  test("tan tanh") {
    checkAnswer(
      double2.select(tan(col("A")), tan(col("A")), tanh(col("A")), tanh(col("A"))),
      Seq(
        Row(0.10033467208545055, 0.10033467208545055, 0.09966799462495582, 0.09966799462495582),
        Row(0.2027100355086725, 0.2027100355086725, 0.197375320224904, 0.197375320224904),
        Row(0.30933624960962325, 0.30933624960962325, 0.2913126124515909, 0.2913126124515909)),
      sort = false)
  }

  test("degrees") {
    checkAnswer(
      double2.select(degrees(col("A")), degrees(col("B"))),
      Seq(
        Row(5.729577951308233, 28.64788975654116),
        Row(11.459155902616466, 34.37746770784939),
        Row(17.188733853924695, 40.10704565915762)),
      sort = false)
  }

  test("radians") {
    checkAnswer(
      double1.select(radians(col("A")), radians(col("A"))),
      Seq(
        Row(0.019390607989657, 0.019390607989657),
        Row(0.038781215979314, 0.038781215979314),
        Row(0.058171823968971005, 0.058171823968971005)),
      sort = false)
  }

  test("md5 sha1 sha2") {
    checkAnswer(
      string1.select(md5(col("A")), sha1(col("A")), sha2(col("A"), 224)),
      Seq(
        Row(
          "5a105e8b9d40e1329780d62ea2265d8a", // pragma: allowlist secret
          "b444ac06613fc8d63795be9ad0beaf55011936ac", // pragma: allowlist secret
          "aff3c83c40e2f1ae099a0166e1f27580525a9de6acd995f21717e984"), // pragma: allowlist secret
        Row(
          "ad0234829205b9033196ba818f7a872b", // pragma: allowlist secret
          "109f4b3c50d7b0df729d299bc6f8e9ef9066971f", // pragma: allowlist secret
          "35f757ad7f998eb6dd3dd1cd3b5c6de97348b84a951f13de25355177"), // pragma: allowlist secret
        Row(
          "8ad8757baa8564dc136c1e07507f4a98", // pragma: allowlist secret
          "3ebfa301dc59196f18593c45e519287a23297589", // pragma: allowlist secret
          "d2d5c076b2435565f66649edd604dd5987163e8a8240953144ec652f")), // pragma: allowlist secret
      sort = false)
  }

  test("hash") {
    checkAnswer(
      string1.select(hash(col("A"))),
      Seq(Row(-1996792119384707157L), Row(-410379000639015509L), Row(9028932499781431792L)),
      sort = false)
  }

  test("ascii") {
    checkAnswer(string1.select(ascii(col("B"))), Seq(Row(97), Row(98), Row(99)), sort = false)
  }

  test("concat_ws") {
    checkAnswer(
      string1.select(concat_ws(lit(","), col("A"), col("B"))),
      Seq(Row("test1,a"), Row("test2,b"), Row("test3,c")),
      sort = false)
  }

  test("initcap length lower upper") {
    checkAnswer(
      string2.select(initcap(col("A")), length(col("A")), lower(col("A")), upper(col("A"))),
      Seq(
        Row("Asdfg", 5, "asdfg", "ASDFG"),
        Row("Qqq", 3, "qqq", "QQQ"),
        Row("Qw", 2, "qw", "QW")),
      sort = false)
  }

  test("lpad rpad") {
    checkAnswer(
      string2.select(lpad(col("A"), lit(8), lit("X")), rpad(col("A"), lit(9), lit("S"))),
      Seq(
        Row("XXXasdFg", "asdFgSSSS"),
        Row("XXXXXqqq", "qqqSSSSSS"),
        Row("XXXXXXQw", "QwSSSSSSS")),
      sort = false)
  }

  test("ltrim rtrim, trim") {
    checkAnswer(
      string3.select(ltrim(col("A")), rtrim(col("A"))),
      Seq(Row("abcba  ", "  abcba"), Row("a12321a   ", " a12321a")),
      sort = false)

    checkAnswer(
      string3
        .select(
          ltrim(col("A"), lit(" a")),
          rtrim(col("A"), lit(" a")),
          trim(col("A"), lit("a "))),
      Seq(Row("bcba  ", "  abcb", "bcb"), Row("12321a   ", " a12321", "12321")),
      sort = false)
  }

  test("repeat") {
    checkAnswer(
      string1.select(repeat(col("B"), lit(3))),
      Seq(Row("aaa"), Row("bbb"), Row("ccc")),
      sort = false)
  }

  test("builtin function") {
    val repeat = functions.builtin("repeat")
    checkAnswer(
      string1.select(repeat(col("B"), 3)),
      Seq(Row("aaa"), Row("bbb"), Row("ccc")),
      sort = false)
  }

  test("soundex") {
    checkAnswer(
      string4.select(soundex(col("A"))),
      Seq(Row("a140"), Row("b550"), Row("p200")),
      sort = false)
  }

  test("sub string") {
    checkAnswer(
      string1.select(substring(col("A"), lit(2), lit(4))),
      Seq(Row("est1"), Row("est2"), Row("est3")),
      sort = false)
  }

  test("translate") {
    checkAnswer(
      string3.select(translate(col("A"), lit("ab "), lit("XY"))),
      Seq(Row("XYcYX"), Row("X12321X")),
      sort = false)
  }

  test("add months, current date") {
    checkAnswer(
      date1.select(add_months(col("A"), lit(1))),
      Seq(Row(Date.valueOf("2020-09-01")), Row(Date.valueOf("2011-01-01"))),
      sort = false)
    // zero1.select(current_date()) gets the date on server, which uses session timezone.
    // System.currentTimeMillis() is based on jvm timezone. They should not always be equal.
    // We can set local JVM timezone to session timezone to ensure it passes.
    testWithAlteredSessionParameter(testWithTimezone({
      checkAnswer(zero1.select(current_date()), Seq(Row(new Date(System.currentTimeMillis()))))
    }, getTimeZone(session)), "TIMEZONE", "'GMT'")
    testWithAlteredSessionParameter(testWithTimezone({
      checkAnswer(zero1.select(current_date()), Seq(Row(new Date(System.currentTimeMillis()))))
    }, getTimeZone(session)), "TIMEZONE", "'Etc/GMT+8'")
    testWithAlteredSessionParameter(testWithTimezone({
      checkAnswer(zero1.select(current_date()), Seq(Row(new Date(System.currentTimeMillis()))))
    }, getTimeZone(session)), "TIMEZONE", "'Etc/GMT-8'")
  }

  test("current timestamp") {
    assert(
      (System.currentTimeMillis() - zero1
        .select(current_timestamp())
        .collect()(0)
        .getTimestamp(0)
        .getTime).abs < 100000)
  }

  test("year month day week quarter") {
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
        Row(2010, 12, 1, 3, 335, 4, 48, new Date(110, 11, 31))),
      sort = false)
  }

  test("next day") {
    checkAnswer(
      date1.select(next_day(col("A"), lit("FR"))),
      Seq(Row(new Date(120, 7, 7)), Row(new Date(110, 11, 3))),
      sort = false)
  }

  test("previous day") {
    date2.select(previous_day(col("a"), col("b"))).show()
    checkAnswer(
      date2.select(previous_day(col("a"), col("b"))),
      Seq(Row(new Date(120, 6, 27)), Row(new Date(110, 10, 24))),
      sort = false)
  }

  test("hour minute second") {
    checkAnswer(
      timestamp1.select(hour(col("A")), minute(col("A")), second(col("A"))),
      Seq(Row(13, 11, 20), Row(1, 30, 5)),
      sort = false)
  }

  test("datediff") {
    checkAnswer(
      timestamp1
        .select(col("a"), dateadd("year", lit(1), col("a")).as("b"))
        .select(datediff("year", col("a"), col("b"))),
      Seq(Row(1), Row(1)))
  }

  test("dateadd") {
    checkAnswer(
      date1.select(dateadd("year", lit(1), col("a"))),
      Seq(Row(new Date(121, 7, 1)), Row(new Date(111, 11, 1))),
      sort = false)
  }

  test("to_timestamp") {
    checkAnswer(
      long1.select(to_timestamp(col("A"))),
      Seq(
        Row(Timestamp.valueOf("2019-06-25 16:19:17.0")),
        Row(Timestamp.valueOf("2019-08-10 23:25:57.0")),
        Row(Timestamp.valueOf("2006-10-22 01:12:37.0"))),
      sort = false)

    val df = session.sql("select * from values('04/05/2020 01:02:03') as T(a)")

    checkAnswer(
      df.select(to_timestamp(col("A"), lit("mm/dd/yyyy hh24:mi:ss"))),
      Seq(Row(Timestamp.valueOf("2020-04-05 01:02:03.0"))))
  }

  test("convert_timezone") {
    checkAnswer(
      timestampNTZ.select(
        convert_timezone(lit("America/Los_Angeles"), lit("America/New_York"), col("a"))),
      Seq(
        Row(Timestamp.valueOf("2020-05-01 16:11:20.0")),
        Row(Timestamp.valueOf("2020-08-21 04:30:05.0"))),
      sort = false)

    val df = Seq(("2020-05-01 16:11:20.0 +02:00", "2020-08-21 04:30:05.0 -06:00")).toDF("a", "b")
    checkAnswer(
      df.select(
        convert_timezone(lit("America/Los_Angeles"), col("a")),
        convert_timezone(lit("America/New_York"), col("b"))),
      Seq(
        Row(
          Timestamp.valueOf("2020-05-01 07:11:20.0"),
          Timestamp.valueOf("2020-08-21 06:30:05.0"))))
    // -06:00 -> New_York should be -06:00 -> -04:00, which is +2 hours.
  }

  test("to_date") {
    val df = session.sql("select * from values('2020-05-11') as T(a)")
    checkAnswer(df.select(to_date(col("A"))), Seq(Row(new Date(120, 4, 11))))

    val df1 = session.sql("select * from values('2020.07.23') as T(a)")
    checkAnswer(df1.select(to_date(col("A"), lit("YYYY.MM.DD"))), Seq(Row(new Date(120, 6, 23))))
  }

  test("date_trunc") {
    checkAnswer(
      timestamp1.select(date_trunc("quarter", col("A"))),
      Seq(
        Row(Timestamp.valueOf("2020-04-01 00:00:00.0")),
        Row(Timestamp.valueOf("2020-07-01 00:00:00.0"))),
      sort = false)
  }

  test("trunc") {
    val df = Seq((3.14, 1)).toDF("expr", "scale")
    checkAnswer(df.select(trunc(col("expr"), col("scale"))), Seq(Row(3.1)))
  }

  test("concat") {
    checkAnswer(
      string1.select(concat(col("A"), col("B"))),
      Seq(Row("test1a"), Row("test2b"), Row("test3c")),
      sort = false)
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
      Seq(Row(true), Row(false), Row(false)),
      sort = false)
  }

  test("startswith") {
    checkAnswer(
      string4.select(startswith(col("a"), lit("ban"))),
      Seq(Row(false), Row(true), Row(false)),
      sort = false)
  }

  test("char") {
    val df = Seq((84, 85), (96, 97)).toDF("A", "B")

    checkAnswer(
      df.select(char(col("A")), char(col("B"))),
      Seq(Row("T", "U"), Row("`", "a")),
      sort = false)
  }

  test("array_overlap") {
    checkAnswer(
      array1
        .select(arrays_overlap(col("ARR1"), col("ARR2"))),
      Seq(Row(true), Row(false)),
      sort = false)
  }

  test("array_intersection") {
    checkAnswer(
      array1.select(array_intersection(col("ARR1"), col("ARR2"))),
      Seq(Row("[\n  3\n]"), Row("[]")),
      sort = false)
  }

  test("is_array") {
    checkAnswer(array1.select(is_array(col("ARR1"))), Seq(Row(true), Row(true)), sort = false)
    checkAnswer(
      variant1.select(is_array(col("arr1")), is_array(col("bool1")), is_array(col("str1"))),
      Seq(Row(true, false, false)),
      sort = false)
  }

  test("is_boolean") {
    checkAnswer(
      variant1.select(is_boolean(col("arr1")), is_boolean(col("bool1")), is_boolean(col("str1"))),
      Seq(Row(false, true, false)),
      sort = false)
  }

  test("is_binary") {
    checkAnswer(
      variant1.select(is_binary(col("bin1")), is_binary(col("bool1")), is_binary(col("str1"))),
      Seq(Row(true, false, false)),
      sort = false)
  }

  test("is_char/is_varchar") {
    checkAnswer(
      variant1.select(is_char(col("str1")), is_char(col("bin1")), is_char(col("bool1"))),
      Seq(Row(true, false, false)),
      sort = false)
    checkAnswer(
      variant1.select(is_varchar(col("str1")), is_varchar(col("bin1")), is_varchar(col("bool1"))),
      Seq(Row(true, false, false)),
      sort = false)
  }

  test("is_date/is_date_value") {
    checkAnswer(
      variant1.select(is_date(col("date1")), is_date(col("time1")), is_date(col("bool1"))),
      Seq(Row(true, false, false)),
      sort = false)
    checkAnswer(
      variant1.select(
        is_date_value(col("date1")),
        is_date_value(col("time1")),
        is_date_value(col("str1"))),
      Seq(Row(true, false, false)),
      sort = false)
  }

  test("is_decimal") {
    checkAnswer(
      variant1
        .select(is_decimal(col("decimal1")), is_decimal(col("double1")), is_decimal(col("num1"))),
      Seq(Row(true, false, true)),
      sort = false)
  }

  test("is_double/is_real") {
    checkAnswer(
      variant1.select(
        is_double(col("decimal1")),
        is_double(col("double1")),
        is_double(col("num1")),
        is_double(col("bool1"))),
      Seq(Row(true, true, true, false)),
      sort = false)

    checkAnswer(
      variant1.select(
        is_real(col("decimal1")),
        is_real(col("double1")),
        is_real(col("num1")),
        is_real(col("bool1"))),
      Seq(Row(true, true, true, false)),
      sort = false)
  }

  test("is_integer") {
    checkAnswer(
      variant1.select(
        is_integer(col("decimal1")),
        is_integer(col("double1")),
        is_integer(col("num1")),
        is_integer(col("bool1"))),
      Seq(Row(false, false, true, false)),
      sort = false)
  }

  test("is_null_value") {
    checkAnswer(
      nullJson1.select(is_null_value(sqlExpr("v:a"))),
      Seq(Row(true), Row(false), Row(null)),
      sort = false)
  }

  test("is_object") {
    checkAnswer(
      variant1.select(is_object(col("obj1")), is_object(col("arr1")), is_object(col("str1"))),
      Seq(Row(true, false, false)),
      sort = false)
  }

  test("is_time") {
    checkAnswer(
      variant1
        .select(is_time(col("time1")), is_time(col("date1")), is_time(col("timestamp_tz1"))),
      Seq(Row(true, false, false)),
      sort = false)
  }

  test("is_timestamp_*") {
    checkAnswer(
      variant1.select(
        is_timestamp_ntz(col("timestamp_ntz1")),
        is_timestamp_ntz(col("timestamp_tz1")),
        is_timestamp_ntz(col("timestamp_ltz1"))),
      Seq(Row(true, false, false)),
      sort = false)

    checkAnswer(
      variant1.select(
        is_timestamp_ltz(col("timestamp_ntz1")),
        is_timestamp_ltz(col("timestamp_tz1")),
        is_timestamp_ltz(col("timestamp_ltz1"))),
      Seq(Row(false, false, true)),
      sort = false)

    checkAnswer(
      variant1.select(
        is_timestamp_tz(col("timestamp_ntz1")),
        is_timestamp_tz(col("timestamp_tz1")),
        is_timestamp_tz(col("timestamp_ltz1"))),
      Seq(Row(false, true, false)),
      sort = false)
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
    val df = Seq((2020, 9, 16)).toDF("year", "month", "day")
    val result = df.select(date_from_parts(col("year"), col("month"), col("day")))
    checkAnswer(result, Seq(Row(new Date(120, 8, 16))))
  }

  test("dayname") {
    checkAnswer(date1.select(dayname(col("a"))), Seq(Row("Sat"), Row("Wed")), sort = false)
  }

  test("monthname") {
    checkAnswer(date1.select(monthname(col("a"))), Seq(Row("Aug"), Row("Dec")), sort = false)
  }

  test("endswith") {
    checkAnswer(string4.filter(endswith(col("a"), lit("le"))), Seq(Row("apple")))
  }

  test("insert") {
    checkAnswer(
      string4.select(insert(col("a"), lit(2), lit(3), lit("abc"))),
      Seq(Row("aabce"), Row("babcna"), Row("pabch")),
      sort = false)
  }

  test("left") {
    checkAnswer(
      string4.select(left(col("a"), lit(2))),
      Seq(Row("ap"), Row("ba"), Row("pe")),
      sort = false)
  }

  test("right") {
    checkAnswer(
      string4.select(right(col("a"), lit(2))),
      Seq(Row("le"), Row("na"), Row("ch")),
      sort = false)
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
    checkAnswer(
      string4.select(regexp_count(col("a"), lit("a"))),
      Seq(Row(1), Row(3), Row(1)),
      sort = false)

    checkAnswer(
      string4.select(regexp_count(col("a"), lit("a"), lit(2), lit("c"))),
      Seq(Row(0), Row(3), Row(1)),
      sort = false)
  }

  test("replace") {
    checkAnswer(
      string4.select(replace(col("a"), lit("a"))),
      Seq(Row("pple"), Row("bnn"), Row("pech")),
      sort = false)

    checkAnswer(
      string4.select(replace(col("a"), lit("a"), lit("z"))),
      Seq(Row("zpple"), Row("bznznz"), Row("pezch")),
      sort = false)

  }

  test("time_from_parts") {
    assert(
      zero1
        .select(time_from_parts(lit(1), lit(2), lit(3)))
        .collect()(0)
        .getTime(0)
        .equals(new Time(3723000)))

    assert(
      zero1
        .select(time_from_parts(lit(1), lit(2), lit(3), lit(444444444)))
        .collect()(0)
        .getTime(0)
        .equals(new Time(3723444)))
  }

  test("charindex") {
    checkAnswer(
      string4.select(charindex(lit("na"), col("a"))),
      Seq(Row(0), Row(3), Row(0)),
      sort = false)

    checkAnswer(
      string4.select(charindex(lit("na"), col("a"), lit(4))),
      Seq(Row(0), Row(5), Row(0)),
      sort = false)
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

  test("timestamp_ltz_from_parts") {
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

  test("timestamp_ntz_from_parts") {
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

  test("timestamp_tz_from_parts") {
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

  test("check_json") {
    checkAnswer(
      nullJson1.select(check_json(col("v"))),
      Seq(Row(null), Row(null), Row(null)),
      sort = false)

    checkAnswer(
      invalidJson1.select(check_json(col("v"))),
      Seq(
        Row("incomplete object value, pos 11"),
        Row("missing colon, pos 7"),
        Row("unfinished string, pos 5")),
      sort = false)
  }

  test("check_xml") {
    checkAnswer(
      nullXML1.select(check_xml(col("v"))),
      Seq(Row(null), Row(null), Row(null), Row(null)),
      sort = false)

    checkAnswer(
      invalidXML1.select(check_xml(col("v"))),
      Seq(
        Row("no opening tag for </t>, pos 8"),
        Row("missing closing tags: </t1></t1>, pos 8"),
        Row("bad character in XML tag name: '<', pos 4")),
      sort = false)
  }

  test("json_extract_path_text") {
    checkAnswer(
      validJson1.select(json_extract_path_text(col("v"), col("k"))),
      Seq(Row(null), Row("foo"), Row(null), Row(null)),
      sort = false)
  }

  test("parse_json") {
    checkAnswer(
      nullJson1.select(parse_json(col("v"))),
      Seq(Row("{\n  \"a\": null\n}"), Row("{\n  \"a\": \"foo\"\n}"), Row(null)),
      sort = false)
  }

  test("parse_xml") {
    checkAnswer(
      nullXML1.select(parse_xml(col("v"))),
      Seq(
        Row("<t1>\n  foo\n  <t2>bar</t2>\n  <t3></t3>\n</t1>"),
        Row("<t1></t1>"),
        Row(null),
        Row(null)),
      sort = false)
  }

  test("strip_null_value") {
    checkAnswer(
      nullJson1.select(sqlExpr("v:a")),
      Seq(Row("null"), Row("\"foo\""), Row(null)),
      sort = false)

    checkAnswer(
      nullJson1.select(strip_null_value(sqlExpr("v:a"))),
      Seq(Row(null), Row("\"foo\""), Row(null)),
      sort = false)
  }

  test("array_agg") {
    assert(monthlySales.select(array_agg(col("amount"))).collect()(0).get(0).toString ==
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
      Seq(Row(value1), Row(value1), Row(value2), Row(value2), Row(value2)),
      false)
  }

  test("array_append") {
    checkAnswer(
      array1.select(array_append(array_append(col("arr1"), lit("amount")), lit(32.21))),
      Seq(
        Row("[\n  1,\n  2,\n  3,\n  \"amount\",\n  3.221000000000000e+01\n]"),
        Row("[\n  6,\n  7,\n  8,\n  \"amount\",\n  3.221000000000000e+01\n]")),
      sort = false)

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
        Row("[\n  6,\n  7,\n  8,\n  1,\n  \"e2\"\n]")),
      sort = false)
  }

  test("array_cat") {
    checkAnswer(
      array1.select(array_cat(col("arr1"), col("arr2"))),
      Seq(
        Row("[\n  1,\n  2,\n  3,\n  3,\n  4,\n  5\n]"),
        Row("[\n  6,\n  7,\n  8,\n  9,\n  0,\n  1\n]")),
      sort = false)
  }

  test("array_compact") {
    checkAnswer(
      nullArray1.select(array_compact(col("arr1"))),
      Seq(Row("[\n  1,\n  3\n]"), Row("[\n  6,\n  8\n]")),
      sort = false)
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
        Row("[\n  3,\n  1.200000000000000e+00,\n  undefined\n]")),
      sort = false)
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
        Row("[\n  3,\n  1.200000000000000e+00\n]")),
      sort = false)
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
      Seq(Row(true), Row(false), Row(false)),
      sort = false)
  }

  test("array_insert") {
    checkAnswer(
      array2.select(array_insert(col("arr1"), col("d"), col("e"))),
      Seq(Row("[\n  1,\n  2,\n  \"e1\",\n  3\n]"), Row("[\n  6,\n  \"e2\",\n  7,\n  8\n]")),
      sort = false)
  }

  test("array_position") {
    checkAnswer(
      array2.select(array_position(col("d"), col("arr1"))),
      Seq(Row(1), Row(null)),
      sort = false)
  }

  test("array_prepend") {
    checkAnswer(
      array1.select(array_prepend(array_prepend(col("arr1"), lit("amount")), lit(32.21))),
      Seq(
        Row("[\n  3.221000000000000e+01,\n  \"amount\",\n  1,\n  2,\n  3\n]"),
        Row("[\n  3.221000000000000e+01,\n  \"amount\",\n  6,\n  7,\n  8\n]")),
      sort = false)

    checkAnswer(
      array2.select(array_prepend(array_prepend(col("arr1"), col("d")), col("e"))),
      Seq(
        Row("[\n  \"e1\",\n  2,\n  1,\n  2,\n  3\n]"),
        Row("[\n  \"e2\",\n  1,\n  6,\n  7,\n  8\n]")),
      sort = false)
  }

  test("array_size") {
    checkAnswer(array2.select(array_size(col("arr1"))), Seq(Row(3), Row(3)), sort = false)

    checkAnswer(array2.select(array_size(col("d"))), Seq(Row(null), Row(null)), sort = false)

    checkAnswer(
      array2.select(array_size(parse_json(col("f")))),
      Seq(Row(1), Row(2)),
      sort = false)
  }

  test("array_slice") {
    checkAnswer(
      array3.select(array_slice(col("arr1"), col("d"), col("e"))),
      Seq(Row("[\n  2\n]"), Row("[\n  5\n]"), Row("[\n  6,\n  7\n]")),
      sort = false)
  }

  test("array_to_string") {
    checkAnswer(
      array3.select(array_to_string(col("arr1"), col("f"))),
      Seq(Row("1,2,3"), Row("4, 5, 6"), Row("6;7;8")),
      sort = false)
  }

  test("objectagg") {
    checkAnswer(
      object1.select(objectagg(col("key"), col("value"))),
      Seq(Row("{\n  \"age\": 21,\n  \"zip\": 94401\n}")),
      sort = false)
  }

  test("object_construct") {
    checkAnswer(
      object1.select(object_construct(col("key"), col("value"))),
      Seq(Row("{\n  \"age\": 21\n}"), Row("{\n  \"zip\": 94401\n}")),
      sort = false)

    checkAnswer(object1.select(object_construct()), Seq(Row("{}"), Row("{}")), sort = false)
  }

  test("object_delete") {
    checkAnswer(
      object2.select(object_delete(col("obj"), col("k"), lit("name"), lit("non-exist-key"))),
      Seq(Row("{\n  \"zip\": 21021\n}"), Row("{\n  \"age\": 26,\n  \"zip\": 94021\n}")),
      sort = false)
  }

  test("object_insert") {
    checkAnswer(
      object2.select(object_insert(col("obj"), lit("key"), lit("v"))),
      Seq(
        Row("{\n  \"age\": 21,\n  \"key\": \"v\",\n  \"name\": \"Joe\",\n  \"zip\": 21021\n}"),
        Row("{\n  \"age\": 26,\n  \"key\": \"v\",\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")),
      sort = false)

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
        Row("{\n  \"age\": 26,\n  \"key\": 0,\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")),
      sort = false)
  }

  test("object_pick") {
    checkAnswer(
      object2.select(object_pick(col("obj"), col("k"), lit("name"), lit("non-exist-key"))),
      Seq(Row("{\n  \"age\": 21,\n  \"name\": \"Joe\"\n}"), Row("{\n  \"name\": \"Jay\"\n}")),
      sort = false)

    checkAnswer(
      object2.select(object_pick(col("obj"), array_construct(lit("name"), lit("zip")))),
      Seq(
        Row("{\n  \"name\": \"Joe\",\n  \"zip\": 21021\n}"),
        Row("{\n  \"name\": \"Jay\",\n  \"zip\": 94021\n}")),
      sort = false)
  }

  test("as_array") {
    checkAnswer(
      array1.select(as_array(col("ARR1"))),
      Seq(Row("[\n  1,\n  2,\n  3\n]"), Row("[\n  6,\n  7,\n  8\n]")),
      sort = false)
    checkAnswer(
      variant1.select(as_array(col("arr1")), as_array(col("bool1")), as_array(col("str1"))),
      Seq(Row("[\n  \"Example\"\n]", null, null)),
      sort = false)
  }

  test("as_binary") {
    checkAnswer(
      variant1.select(as_binary(col("bin1")), as_binary(col("bool1")), as_binary(col("str1"))),
      Seq(Row(Array[Byte](115, 110, 111, 119), null, null)),
      sort = false)
  }

  test("as_char/as_varchar") {
    checkAnswer(
      variant1.select(as_char(col("str1")), as_char(col("bin1")), as_char(col("bool1"))),
      Seq(Row("X", null, null)),
      sort = false)
    checkAnswer(
      variant1.select(as_varchar(col("str1")), as_varchar(col("bin1")), as_varchar(col("bool1"))),
      Seq(Row("X", null, null)),
      sort = false)
  }

  test("as_date") {
    checkAnswer(
      variant1.select(as_date(col("date1")), as_date(col("time1")), as_date(col("bool1"))),
      Seq(Row(new Date(117, 1, 24), null, null)),
      sort = false)
  }

  test("as_decimal/as_number") {
    checkAnswer(
      variant1
        .select(as_decimal(col("decimal1")), as_decimal(col("double1")), as_decimal(col("num1"))),
      Seq(Row(1, null, 15)),
      sort = false)

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
      Seq(Row(1, null, 15)),
      sort = false)

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
      Seq(Row(1.23, 3.21, 15.0, null)),
      sort = false)

    checkAnswer(
      variant1.select(
        as_real(col("decimal1")),
        as_real(col("double1")),
        as_real(col("num1")),
        as_real(col("bool1"))),
      Seq(Row(1.23, 3.21, 15.0, null)),
      sort = false)
  }

  test("as_integer") {
    checkAnswer(
      variant1.select(
        as_integer(col("decimal1")),
        as_integer(col("double1")),
        as_integer(col("num1")),
        as_integer(col("bool1"))),
      Seq(Row(1, null, 15, null)),
      sort = false)
  }

  test("as_object") {
    checkAnswer(
      variant1.select(as_object(col("obj1")), as_object(col("arr1")), as_object(col("str1"))),
      Seq(Row("{\n  \"Tree\": \"Pine\"\n}", null, null)),
      sort = false)
  }

  test("as_time") {
    checkAnswer(
      variant1
        .select(as_time(col("time1")), as_time(col("date1")), as_time(col("timestamp_tz1"))),
      Seq(Row(Time.valueOf("20:57:01"), null, null)),
      sort = false)
  }

  test("as_timestamp_*") {
    checkAnswer(
      variant1.select(
        as_timestamp_ntz(col("timestamp_ntz1")),
        as_timestamp_ntz(col("timestamp_tz1")),
        as_timestamp_ntz(col("timestamp_ltz1"))),
      Seq(Row(Timestamp.valueOf("2017-02-24 12:00:00.456"), null, null)),
      sort = false)

    checkAnswer(
      variant1.select(
        as_timestamp_ltz(col("timestamp_ntz1")),
        as_timestamp_ltz(col("timestamp_tz1")),
        as_timestamp_ltz(col("timestamp_ltz1"))),
      Seq(Row(null, null, Timestamp.valueOf("2017-02-24 04:00:00.123"))),
      sort = false)

    checkAnswer(
      variant1.select(
        as_timestamp_tz(col("timestamp_ntz1")),
        as_timestamp_tz(col("timestamp_tz1")),
        as_timestamp_tz(col("timestamp_ltz1"))),
      Seq(Row(null, Timestamp.valueOf("2017-02-24 13:00:00.123"), null)),
      sort = false)
  }

  test("strtok_to_array") {
    checkAnswer(
      string6
        .select(strtok_to_array(col("a"), col("b"))),
      Seq(
        Row("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]"),
        Row("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]")),
      sort = false)

    checkAnswer(
      string6
        .select(strtok_to_array(col("a"))),
      Seq(
        Row("[\n  \"1,2,3,4,5\"\n]"),
        Row("[\n  \"1\",\n  \"2\",\n  \"3\",\n  \"4\",\n  \"5\"\n]")),
      sort = false)
  }

  test("to_array") {
    checkAnswer(
      integer1
        .select(to_array(col("a"))),
      Seq(Row("[\n  1\n]"), Row("[\n  2\n]"), Row("[\n  3\n]")),
      sort = false)
  }

  test("to_json") {
    checkAnswer(
      integer1
        .select(to_json(col("a"))),
      Seq(Row("1"), Row("2"), Row("3")),
      sort = false)

    checkAnswer(
      variant1
        .select(to_json(col("time1"))),
      Seq(Row("\"20:57:01\"")),
      sort = false)
  }

  test("to_object") {
    checkAnswer(
      variant1
        .select(to_object(col("obj1"))),
      Seq(Row("{\n  \"Tree\": \"Pine\"\n}")),
      sort = false)
  }

  test("to_variant") {
    checkAnswer(
      integer1
        .select(to_variant(col("a"))),
      Seq(Row("1"), Row("2"), Row("3")),
      sort = false)
    assert(
      integer1.select(to_variant(col("a"))).collect()(0).getVariant(0).equals(new Variant(1)))
  }

  test("to_xml") {
    checkAnswer(
      integer1
        .select(to_xml(col("a"))),
      Seq(
        Row("<SnowflakeData type=\"INTEGER\">1</SnowflakeData>"),
        Row("<SnowflakeData type=\"INTEGER\">2</SnowflakeData>"),
        Row("<SnowflakeData type=\"INTEGER\">3</SnowflakeData>")),
      sort = false)
  }

  test("get") {
    checkAnswer(
      object2
        .select(get(col("obj"), col("k"))),
      Seq(Row("21"), Row(null)),
      sort = false)

    checkAnswer(
      object2
        .select(get(col("obj"), lit("AGE"))),
      Seq(Row(null), Row(null)),
      sort = false)
  }

  test("get_ignore_case") {
    checkAnswer(
      object2
        .select(get(col("obj"), col("k"))),
      Seq(Row("21"), Row(null)),
      sort = false)

    checkAnswer(
      object2
        .select(get_ignore_case(col("obj"), lit("AGE"))),
      Seq(Row("21"), Row("26")),
      sort = false)
  }

  test("object_keys") {
    checkAnswer(
      object2
        .select(object_keys(col("obj"))),
      Seq(
        Row("[\n  \"age\",\n  \"name\",\n  \"zip\"\n]"),
        Row("[\n  \"age\",\n  \"name\",\n  \"zip\"\n]")),
      sort = false)
  }

  test("xmlget") {
    checkAnswer(
      validXML1
        .select(get(xmlget(col("v"), col("t2")), lit('$'))),
      Seq(Row("\"bar\""), Row(null), Row("\"foo\"")),
      sort = false)

    assert(
      validXML1
        .select(get(xmlget(col("v"), col("t2")), lit('$')))
        .collect()(0)
        .getVariant(0)
        .equals(new Variant("\"bar\"")))

    checkAnswer(
      validXML1
        .select(get(xmlget(col("v"), col("t3"), lit('0')), lit('@'))),
      Seq(Row("\"t3\""), Row(null), Row(null)),
      sort = false)

    checkAnswer(
      validXML1
        .select(get(xmlget(col("v"), col("t2"), col("instance")), lit('$'))),
      Seq(Row("\"bar\""), Row(null), Row("\"bar\"")),
      sort = false)
  }

  test("get_path") {
    checkAnswer(
      validJson1
        .select(get_path(col("v"), col("k"))),
      Seq(Row("null"), Row("\"foo\""), Row(null), Row(null)),
      sort = false)
  }

  test("approx_percentile") {
    checkAnswer(
      approxNumbers.select(approx_percentile(col("a"), 0.5)),
      Seq(Row(4.5)),
      sort = false)
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
        "\"type\": \"tdigest\",\n  \"version\": 1\n}")),
      sort = false)
  }

  test("approx_percentile_estimate") {
    checkAnswer(
      approxNumbers.select(
        approx_percentile_estimate(approx_percentile_accumulate(col("a")), 0.5)),
      approxNumbers.select(approx_percentile(col("a"), 0.5)),
      sort = false)
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
      Seq(Row("{\n  \"state\": [\n    0.000000000000000e+00,\n    1.000000000000000e+00,\n    " +
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
        "\"version\": 1\n}")),
      sort = false)
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
    checkAnswer(
      testData1.select(col("num") + col(dfMax), col(dfMax) - col(dfMin)),
      expectedResult)
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

}

class EagerFunctionSuite extends FunctionSuite with EagerSession

class LazyFunctionSuite extends FunctionSuite with LazySession
