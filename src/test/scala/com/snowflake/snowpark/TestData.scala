package com.snowflake.snowpark

import java.util.TimeZone

trait TestData extends SNTestBase {
  import session.implicits._

  val defaultTimezone = TimeZone.getDefault
  val oldSfTimeZone =
    session.sql("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION").collect().head.getString(1)

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  session.runQuery(s"alter session set TIMEZONE = 'UTC'")

  val variant1: DataFrame =
    session.sql(
      "select to_variant(to_array('Example')) as arr1," +
        " to_variant(to_object(parse_json('{\"Tree\": \"Pine\"}'))) as obj1, " +
        " to_variant(to_binary('snow', 'utf-8')) as bin1," +
        " to_variant(true) as bool1," +
        " to_variant('X') as str1, " +
        " to_variant(to_date('2017-02-24')) as date1, " +
        " to_variant(to_time('20:57:01.123456789+07:00')) as time1, " +
        " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, " +
        " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, " +
        " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, " +
        " to_variant(1.23::decimal(6, 3)) as decimal1, " +
        " to_variant(3.21::double) as double1, " +
        " to_variant(15) as num1 ")

  val variant2: DataFrame =
    session.sql("""
                  |select parse_json(column1) as src
                  |from values
                  |('{
                  |    "date with '' and ." : "2017-04-28",
                  |    "salesperson" : {
                  |      "id": "55",
                  |      "name": "Frank Beasley"
                  |    },
                  |    "customer" : [
                  |      {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
                  |    ],
                  |    "vehicle" : [
                  |      {"make": "Honda", "extras":["ext warranty", "paint protection"]}
                  |    ]
                  |}')
                  |""".stripMargin)

  val date1: DataFrame =
    session.sql("select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)")

  val date2: DataFrame =
    session.sql(
      "select * from values('2020-08-01'::Date, 'mo'),('2010-12-01'::Date, 'we') as T(a,b)")

  val date3: DataFrame =
    Seq((2020, 10, 28, 13, 35, 47, 1234567, "America/Los_Angeles")).toDF(
      "year",
      "month",
      "day",
      "hour",
      "minute",
      "second",
      "nanosecond",
      "timezone")

  val timestamp1: DataFrame = session.sql(
    "select * from values('2020-05-01 13:11:20.000' :: timestamp)," +
      "('2020-08-21 01:30:05.000' :: timestamp) as T(a)")

  val timestampNTZ: DataFrame = session.sql(
    "select * from values('2020-05-01 13:11:20.000' :: timestamp_ntz)," +
      "('2020-08-21 01:30:05.000' :: timestamp_ntz) as T(a)")

  TimeZone.setDefault(defaultTimezone)
  session.runQuery(s"alter session set TIMEZONE = '$oldSfTimeZone'")

  lazy val testData1: DataFrame =
    session.createDataFrame(Seq(Data(1, true, "a"), Data(2, false, "b")))

  lazy val testData2: DataFrame =
    session.createDataFrame(
      Seq(Data2(1, 1), Data2(1, 2), Data2(2, 1), Data2(2, 2), Data2(3, 1), Data2(3, 2)))

  lazy val testData3: DataFrame =
    session.createDataFrame(Seq(Data3(1, None), Data3(2, Some(2))))

  lazy val testData4: DataFrame =
    session.createDataFrame((1 to 100).map(i => Data4(i, i.toString)))

  lazy val lowerCaseData: DataFrame =
    session.createDataFrame(
      LowerCaseData(1, "a") ::
        LowerCaseData(2, "b") ::
        LowerCaseData(3, "c") ::
        LowerCaseData(4, "d") :: Nil)

  lazy val upperCaseData: DataFrame =
    session.createDataFrame(
      UpperCaseData(1, "A") ::
        UpperCaseData(2, "B") ::
        UpperCaseData(3, "C") ::
        UpperCaseData(4, "D") ::
        UpperCaseData(5, "E") ::
        UpperCaseData(6, "F") :: Nil)

  lazy val nullInts: DataFrame =
    session.createDataFrame(
      NullInts(1) ::
        NullInts(2) ::
        NullInts(3) ::
        NullInts(null) :: Nil)

  lazy val allNulls: DataFrame =
    session.createDataFrame(
      NullInts(null) ::
        NullInts(null) ::
        NullInts(null) ::
        NullInts(null) :: Nil)

  lazy val nullData1: DataFrame =
    session.sql("select * from values(null),(2),(1),(3),(null) as T(a)")

  lazy val nullData2: DataFrame =
    session.sql(
      "select * from values(1,2,3),(null,2,3),(null,null,3),(null,null,null)," +
        "(1,null,3),(1,null,null),(1,2,null) as T(a,b,c)")

  lazy val nullData3: DataFrame =
    session.sql(
      "select * from values(1.0, 1, true, 'a'),('NaN'::Double, 2, null, 'b')," +
        "(null, 3, false, null), (4.0, null, null, 'd'), (null, null, null, null), " +
        "('NaN'::Double, null, null, null) as T(flo, int, boo, str)")

  lazy val integer1: DataFrame = session.sql("select * from values(1),(2),(3) as T(a)")

  lazy val double1: DataFrame =
    session.sql("select * from values(1.111),(2.222),(3.333) as T(a)")

  lazy val double2: DataFrame =
    session.sql("select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)")

  lazy val double3: DataFrame =
    session.sql(
      "select * from values(1.0, 1),('NaN'::Double, 2),(null, 3)," +
        " (4.0, null), (null, null), ('NaN'::Double, null) as T(a, b)")

  lazy val double4: DataFrame =
    session.sql("select * from values(1.0, 1) as T(a, b)")

  lazy val nanData1: DataFrame =
    session.sql("select * from values(1.2),('NaN'::Double),(null),(2.3) as T(a)")

  lazy val duplicatedNumbers: DataFrame =
    session.sql("select * from values(3),(2),(1),(3),(2) as T(a)")

  lazy val approxNumbers: DataFrame =
    session.sql("select * from values(1),(2),(3),(4),(5),(6),(7),(8),(9),(0) as T(a)")

  lazy val approxNumbers2: DataFrame =
    session.sql(
      "select * from values(1, 1),(2, 1),(3, 3),(4, 3),(5, 3),(6, 3),(7, 3)," +
        "(8, 5),(9, 5),(0, 5) as T(a, T)")

  lazy val string1: DataFrame =
    session.sql("select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)")

  lazy val string2: DataFrame =
    session.sql("select * from values('asdFg'),('qqq'),('Qw') as T(a)")

  lazy val string3: DataFrame =
    session.sql("select * from values('  abcba  '), (' a12321a   ') as T(a)")

  lazy val string4: DataFrame =
    session.sql("select * from values('apple'),('banana'),('peach') as T(a)")

  lazy val string5: DataFrame =
    session.sql("select * from values('1,2,3,4,5') as T(a)")

  lazy val string6: DataFrame =
    session.sql("select * from values('1,2,3,4,5', ','),('1 2 3 4 5', ' ') as T(a, b)")

  lazy val string7: DataFrame =
    session.sql("select * from values('str', 1),(null, 2) as T(a, b)")

  lazy val array1: DataFrame =
    session.sql(
      "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 " +
        "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)")

  lazy val array2: DataFrame =
    session.sql(
      "select array_construct(a,b,c) as arr1, d, e, f " +
        "from values(1,2,3,2,'e1','[{a:1}]'),(6,7,8,1,'e2','[{a:1},{b:2}]') as T(a,b,c,d,e,f)")

  lazy val array3: DataFrame =
    session.sql(
      "select array_construct(a,b,c) as arr1, d, e, f " +
        "from values(1,2,3,1,2,','),(4,5,6,1,-1,', '),(6,7,8,0,2,';') as T(a,b,c,d,e,f)")

  lazy val object1: DataFrame =
    session.sql(
      "select key, to_variant(value) as value from values('age', 21),('zip', " +
        "94401) as T(key,value)")

  lazy val object2: DataFrame =
    session.sql(
      "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from values('age', 21, 'zip', " +
        "21021, 'name', 'Joe', 'age', 0, true),('age', 26, 'zip', 94021, 'name', 'Jay', 'key', " +
        "0, false) as T(a,b,c,d,e,f,k,v,flag)")

  lazy val nullArray1: DataFrame =
    session.sql(
      "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 " +
        "from values(1,null,3,3,null,5),(6,null,8,9,null,1) as T(a,b,c,d,e,f)")

  lazy val nullJson1: DataFrame = session.sql(
    "select parse_json(column1) as v  from values ('{\"a\": null}'), ('{\"a\": \"foo\"}'), (null)")

  lazy val validJson1: DataFrame = session.sql(
    "select parse_json(column1) as v, column2 as k from values ('{\"a\": null}','a'), " +
      "('{\"a\": \"foo\"}','a'), ('{\"a\": \"foo\"}','b'), (null,'a')")

  lazy val invalidJson1: DataFrame =
    session.sql("select (column1) as v from values ('{\"a\": null'), ('{\"a: \"foo\"}'), ('{\"a:')")

  lazy val nullXML1: DataFrame = session.sql(
    "select (column1) as v from values ('<t1>foo<t2>bar</t2><t3></t3></t1>'), " +
      "('<t1></t1>'), (null), ('')")

  lazy val validXML1: DataFrame = session.sql(
    "select parse_xml(a) as v, b as t2, c as t3, d as instance from values" +
      "('<t1>foo<t2>bar</t2><t3></t3></t1>','t2','t3',0),('<t1></t1>','t2','t3',0)," +
      "('<t1><t2>foo</t2><t2>bar</t2></t1>','t2','t3',1) as T(a,b,c,d)")

  lazy val invalidXML1: DataFrame =
    session.sql("select (column1) as v from values ('<t1></t>'), ('<t1><t1>'), ('<t1</t1>')")

  lazy val number1: DataFrame = session.createDataFrame(
    Seq(
      Number1(1, 10.0, 0),
      Number1(2, 10.0, 11.0),
      Number1(2, 20.0, 22.0),
      Number1(2, 25.0, 0),
      Number1(2, 30.0, 35.0)))

  lazy val decimalData: DataFrame =
    session.createDataFrame(
      DecimalData(1, 1) ::
        DecimalData(1, 2) ::
        DecimalData(2, 1) ::
        DecimalData(2, 2) ::
        DecimalData(3, 1) ::
        DecimalData(3, 2) :: Nil)

  lazy val number2: DataFrame =
    session.createDataFrame(Seq(Number2(1, 2, 3), Number2(0, -1, 4), Number2(-5, 0, -9)))

  lazy val number3: DataFrame = session.sql("select * from values(1),(2),(3) as T(a)")

  lazy val zero1: DataFrame = session.sql("select * from values(0) as T(a)")

  lazy val xyz: DataFrame =
    session.createDataFrame(
      Seq(
        Number2(1, 2, 1),
        Number2(1, 2, 3),
        Number2(2, 1, 10),
        Number2(2, 2, 1),
        Number2(2, 2, 3)))

  lazy val long1: DataFrame =
    session.sql("select * from values(1561479557),(1565479557),(1161479557) as T(a)")

  lazy val courseSales: DataFrame =
    session
      .createDataFrame(
        CourseSales("dotNET", 2012, 10000) ::
          CourseSales("Java", 2012, 20000) ::
          CourseSales("dotNET", 2012, 5000) ::
          CourseSales("dotNET", 2013, 48000) ::
          CourseSales("Java", 2013, 30000) :: Nil)

  lazy val monthlySales: DataFrame = session.createDataFrame(
    Seq(
      MonthlySales(1, 10000, "JAN"),
      MonthlySales(1, 400, "JAN"),
      MonthlySales(2, 4500, "JAN"),
      MonthlySales(2, 35000, "JAN"),
      MonthlySales(1, 5000, "FEB"),
      MonthlySales(1, 3000, "FEB"),
      MonthlySales(2, 200, "FEB"),
      MonthlySales(2, 90500, "FEB"),
      MonthlySales(1, 6000, "MAR"),
      MonthlySales(1, 5000, "MAR"),
      MonthlySales(2, 2500, "MAR"),
      MonthlySales(2, 9500, "MAR"),
      MonthlySales(1, 8000, "APR"),
      MonthlySales(1, 10000, "APR"),
      MonthlySales(2, 800, "APR"),
      MonthlySales(2, 4500, "APR")))

  lazy val columnNameHasSpecialCharacter: DataFrame = {
    Seq((1, 2), (3, 4)).toDF("\"col %\"", "\"col *\"")
  }

  lazy val nurse: DataFrame = Seq(
    (201, "Thomas Leonard Vicente", "LVN", "Technician"),
    (202, "Tamara Lolita VanZant", "LVN", "Technician"),
    (341, "Georgeann Linda Vente", "LVN", "General"),
    (471, "Andrea Renee Nouveau", "RN", "Amateur Extra"),
    (101, "Lily Vine", "LVN", null),
    (102, "Larry Vancouver", "LVN", null),
    (172, "Rhonda Nova", "RN", null)).toDF("id", "full_name", "medical_license", "radio_license")

}

case class Data(num: Int, bool: Boolean, str: String)

case class Data2(a: Int, b: Int)

case class Data3(a: Int, b: Option[Int])

case class LowerCaseData(n: Int, l: String)

case class UpperCaseData(N: Int, L: String)

case class NullInts(a: java.lang.Integer)

case class Number1(K: Int, v1: Double, v2: Double)

case class Number2(x: Int, y: Int, z: Int)

case class MonthlySales(empid: Int, amount: Int, month: String)

case class Data4(key: Int, value: String)

case class DecimalData(a: BigDecimal, b: BigDecimal)

case class CourseSales(course: String, year: Int, earnings: Double)

case class Fact(date: Int, hour: Int, minute: Int, room_name: String, temp: Double)
