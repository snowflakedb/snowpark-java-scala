package com.snowflake.snowpark_test

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions.{col, _}
import com.snowflake.snowpark.types.{Geography, Variant}

import java.io.{NotSerializableException, Serializable}
import java.sql.{Date, Time, Timestamp}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Random

trait UDFSuite extends TestData {
  import session.implicits._
  val tableName: String = randomName()
  val table1: String = randomName()
  val table2: String = randomName()
  val semiStructuredTable: String = randomName()
  val view1: String = s""""view % ${randomName()}""""
  val view2: String = s""""view ^ ${randomName()}""""
  val tmpStageName: String = randomStageName()

  // A non-serializable class
  class NonSerializable(val id: Int = -1) {
    override def hashCode(): Int = id

    override def equals(other: Any): Boolean = {
      other match {
        case o: NonSerializable => id == o.id
        case _ => false
      }
    }
  }
  // UDF code for closure cleaner test
  object TestClassWithoutFieldAccess extends Serializable {
    val nonSer = new NonSerializable
    val x = 5

    val run = (a: String) => {
      x
    }
  }

  override def beforeAll: Unit = {
    super.beforeAll()
    runQuery(s"CREATE TEMPORARY STAGE $tmpStageName", session)
    createTable(table1, "a int")
    runQuery(s"insert into $table1 values(1),(2),(3)", session)
    createTable(table2, "a int, b int")
    runQuery(s"insert into $table2 values(1, 2),(2, 3),(3, 4)", session)
    if (!isStoredProc(session)) {
      TestUtils.addDepsToClassPath(session)
    }
  }

  override def afterAll: Unit = {
    dropTable(table1)
    dropTable(table2)
    dropView(view1)
    dropView(view2)
    dropTable(tableName)
    dropTable(semiStructuredTable)
    runQuery(s"DROP STAGE IF EXISTS $tmpStageName", session)
    super.afterAll()
  }

  test("Basic UDF function") {
    val df = session.table(table1)
    val doubleUDF = udf((x: Int) => x + x)
    val doubleUDF1 = session.udf.registerTemporary((x: Int) => x + x)
    checkAnswer(df.select(doubleUDF(col("a"))), Seq(Row(2), Row(4), Row(6)))
    checkAnswer(df.select(doubleUDF1(col("a"))), Seq(Row(2), Row(4), Row(6)))
  }

  test("Basic UDF function with Option[Int]") {
    createTable(tableName, "a int, b int")
    runQuery(s"insert into $tableName values (1, null), (2, 2), (3, null)", session)
    val df = session.table(tableName)
    val doubleUDF = udf((x: Option[Int]) => x.map(a => a + a))
    checkAnswer(df.select(doubleUDF(col("a"))), Seq(Row(2), Row(4), Row(6)))
    checkAnswer(df.select(doubleUDF(col("b"))), Seq(Row(4), Row(null), Row(null)))
  }

  test("Tests for unsupported types") {
    val ex = intercept[UnsupportedOperationException] {
      udf((x: Map[String, String]) => x.keys.toArray)
    }
    assert(ex.getMessage.contains("mutable.Map[String,String]"))

    intercept[UnsupportedOperationException] {
      udf((x: java.lang.Integer) => x + x)
    }

    intercept[UnsupportedOperationException] {
      udf((x: Option[String]) => x.map(a => s"$a"))
    }

    intercept[UnsupportedOperationException] {
      udf((x: mutable.Map[String, String]) => x.keys)
    }
  }

  test("UDF with arrays") {
    createTable(semiStructuredTable, "a1 array")
    runQuery(s"insert into $semiStructuredTable select (array_construct('1', '2', '3'))", session)
    runQuery(s"insert into $semiStructuredTable select (array_construct('4', '5', '6'))", session)
    val df = session.table(semiStructuredTable)
    val listUdf = udf((x: Array[String]) => x.mkString(","))
    checkAnswer(df.select(listUdf(col("a1"))), Seq(Row("1,2,3"), Row("4,5,6")))
  }

  test("UDF with Map[String, String] input args") {
    createTable(semiStructuredTable, "o1 object")
    runQuery(
      s"insert into $semiStructuredTable" +
        s" select (object_construct('1', 'one', '2', 'two'))",
      session)
    runQuery(
      s"insert into $semiStructuredTable" +
        s" select (object_construct('10', 'ten', '20', 'twenty'))",
      session)

    val df = session.table(semiStructuredTable)
    val mapKeysUdf = udf((x: mutable.Map[String, String]) => x.keys.toArray)
    val arraySumUdf = udf((a: Array[String]) => a.map(_.toInt).reduceLeft(_ + _))
    checkAnswer(df.select(arraySumUdf(mapKeysUdf(col("o1")))), Seq(Row(3), Row(30)))
  }

  test("UDF with Map[String, String] as return args") {
    createTable(semiStructuredTable, "a1 array")
    runQuery(s"insert into $semiStructuredTable select (array_construct('1', '2', '3'))", session)
    runQuery(s"insert into $semiStructuredTable select (array_construct('4', '5', '6'))", session)
    val df = session.table(semiStructuredTable)

    val mapUdf = udf((a: Array[String]) => {
      var results = mutable.Map[String, String]()
      a.foreach(i => results += (i -> s"convertToMap$i"))
      results
    })
    val result = df.select(mapUdf(col("a1")).as("col")).sort(col("col").desc).collect()
    assert(result.size == 2)
    Seq(1, 2, 3).foreach(i => assert(result(0).getString(0).contains(s"convertToMap$i")))
    Seq(4, 5, 6).foreach(i => assert(result(1).getString(0).contains(s"convertToMap$i")))
  }

  test("UDF with multiple args of type map, array etc") {
    createTable(semiStructuredTable, "o1 object, o2 object, id varchar")
    runQuery(
      s"insert into $semiStructuredTable" +
        s" ( select object_construct('1', 'one', '2', 'two')," +
        s" object_construct('one', '10', 'two', '20')," +
        s" 'ID1')",
      session)
    runQuery(
      s"insert into $semiStructuredTable" +
        s" ( select object_construct('3', 'three', '4', 'four')," +
        s" object_construct('three', '30', 'four', '40')," +
        s" 'ID2')",
      session)

    val df = session.table(semiStructuredTable)
    val mapUdf =
      udf((map1: mutable.Map[String, String], map2: mutable.Map[String, String], id: String) => {
        val values = map1.values.map(v => map2.get(v))
        val res = values.filter(_.isDefined).map(_.get.toInt).reduceLeft(_ + _)
        mutable.Map(id -> res.toString)
      })

    val res = df.select(mapUdf($"o1", $"o2", $"id").as("col")).sort(col("col").desc).collect
    assert(res.size == 2)
    assert(res(0).getString(0).contains("\"ID1\": \"30\""))
    assert(res(1).getString(0).contains("\"ID2\": \"70\""))
  }

  test("Filter on top of UDF") {
    val df = session.table(table1)
    val doubleUDF = udf((x: Int) => x + x)
    val df2 = df.select(doubleUDF(col("a"))).filter(col("$1") > 4)
    checkAnswer(df2, Seq(Row(6)))
  }

  test("Compose on DataFrameReader") {
    // upload the file to stage
    uploadFileToStage(tmpStageName, testFileParquet, compress = false)
    val path: String = s"@$tmpStageName/$testFileParquet"
    val df1 = session.read.parquet(path).toDF("a")
    val replaceUdf = udf((elem: String) => elem.replaceAll("num", "id"))
    checkAnswer(
      df1.select(replaceUdf($"a")),
      Seq(Row("{\"id\":1,\"str\":\"str1\"}"), Row("{\"id\":2,\"str\":\"str2\"}")))
  }

  test("view with UDF") {
    columnNameHasSpecialCharacter.createOrReplaceView(view1)
    val df1 = session.sql(s"""select * from $view1""")
    val udf1 = udf((x: Int, y: Int) => x + y)
    df1
      .withColumn("\"col #\"", udf1(col("\"col %\""), col("\"col *\"")))
      .createOrReplaceView(view2)
    checkAnswer(session.sql(s"""select * from $view2"""), Seq(Row(1, 2, 3), Row(3, 4, 7)))
  }

  test("String return type") {
    val df = session.table(table1)
    val prefix = "Hello"
    val stringUdf = udf((x: Int) => s"$prefix$x")
    checkAnswer(
      df.withColumn("b", stringUdf(col("a"))),
      Seq(Row(1, "Hello1"), Row(2, "Hello2"), Row(3, "Hello3")))
  }

  test("test large closure", JavaStoredProcAWSOnly) {
    val df = session.table(table1)
    // The closure size can be tuned by changing factor.
    // In experimentation,
    // the largest successful factor is 6.75 (9078)
    // the smallest failed factor is 7.0 (9590)
    val factor = 64.0
    val longString = Random.alphanumeric.filter(_.isLetter).take((factor * 1024).toInt).mkString
    val stringUdf = udf((x: Int) => s"$longString$x")
    val rows = df.select(stringUdf(col("a"))).collect()
    assert(rows(1).getString(0).startsWith(longString))
  }

  test("java.lang.String return type") {
    val df = session.table(table1)
    val prefix = "Hello"
    val stringUdf = udf((x: Int) => new java.lang.String(s"$prefix$x"))
    checkAnswer(
      df.withColumn("b", stringUdf(col("a"))),
      Seq(Row(1, "Hello1"), Row(2, "Hello2"), Row(3, "Hello3")))
  }

  test("UDF function with multiple columns") {
    val df = session.table(table2)
    val sumUDF = udf((x: Int, y: Int) => x + y)
    checkAnswer(
      df.withColumn("c", sumUDF(col("a"), col("b"))),
      Seq(Row(1, 2, 3), Row(2, 3, 5), Row(3, 4, 7)))
  }

  test("Incorrect number of args") {
    val df = session.table(table2)
    val stringUdf = udf((x: Int) => s"Hello$x")
    val ex = intercept[SnowparkClientException] {
      df.withColumn("c", stringUdf(col("a"), col("b"))).collect()
    }
    assert(ex.message.contains("Incorrect number of arguments passed to the UDF"))
  }

  test("callUDF API", JavaStoredProcExcludeOwner) {
    val df = session.table(table1)
    val doubleUDF = (x: Int) => x + x
    val functionName = randomName()
    session.udf.registerTemporary(functionName, doubleUDF)
    checkAnswer(
      df.withColumn(
        "c",
        callUDF(s"${session.getFullyQualifiedCurrentSchema}.$functionName", col("a"))),
      Seq(Row(1, 2), Row(2, 4), Row(3, 6)))
  }

  test("Test for Long data type") {
    val df = Seq(1L, 2L, 3L).toDF("a")
    val UDF = udf((x: Long) => x + x)
    checkAnswer(df.withColumn("c", UDF(col("a"))), Seq(Row(1L, 2L), Row(2L, 4L), Row(3L, 6L)))
  }

  test("Test for Option[Long]") {
    createTable(tableName, "a bigint, b bigint")
    runQuery(s"insert into $tableName values (1, null), (2, 2), (3, null)", session)
    val df = session.table(tableName)
    val doubleUDF = udf((x: Option[Long]) => x.map(a => a + a))
    checkAnswer(df.select(doubleUDF(col("a"))), Seq(Row(2L), Row(4L), Row(6L)))
    checkAnswer(df.select(doubleUDF(col("b"))), Seq(Row(null), Row(4L), Row(null)))
  }

  test("Test for Short data type") {
    val df = Seq(1.toShort, 2.toShort, 3.toShort).toDF("a")
    val UDF = udf((x: Short) => x + x)
    checkAnswer(df.withColumn("c", UDF(col("a"))), Seq(Row(1, 2), Row(2, 4), Row(3, 6)))
  }

  test("Test for Option[Short]") {
    createTable(tableName, "a bigint, b bigint")
    runQuery(s"insert into $tableName values (1, null), (2, 2), (3, null)", session)
    val df = session.table(tableName)
    val doubleUDF = udf((x: Option[Short]) => x.map(a => a + a))
    checkAnswer(df.select(doubleUDF(col("a"))), Seq(Row(2), Row(4), Row(6)))
    checkAnswer(df.select(doubleUDF(col("b"))), Seq(Row(null), Row(4), Row(null)))
  }

  test("Test for Option[Float]") {
    createTable(tableName, "a float, b float")
    runQuery(s"insert into $tableName values (1.1, null), (2.2, 2.2), (3.3, null)", session)
    val df = session.table(tableName)
    val doubleUDF = udf((x: Option[Float]) => x.map(a => a + a))
    checkAnswer(df.select(doubleUDF(col("a"))), Seq(Row(2.2), Row(4.4), Row(6.6)))
    checkAnswer(df.select(doubleUDF(col("b"))), Seq(Row(null), Row(4.4), Row(null)))
  }

  test("Test for Float data type") {
    val df = Seq(1.1.floatValue(), 2.2.floatValue(), 3.3.floatValue()).toDF("a")
    val UDF = udf((x: Float) => x + x)
    checkAnswer(
      df.withColumn("c", UDF(col("a"))),
      Seq(Row(1.1, 2.2), Row(2.2, 4.4), Row(3.3, 6.6)))
  }

  test("Test for Option[Double]") {
    createTable(tableName, "a double, b double")
    runQuery(s"insert into $tableName values (1.1, null), (2.2, 2.2), (3.3, null)", session)
    val df = session.table(tableName)
    val doubleUDF = udf((x: Option[Double]) => x.map(a => a + a))
    checkAnswer(df.select(doubleUDF(col("a"))), Seq(Row(2.2), Row(4.4), Row(6.6)))
    checkAnswer(df.select(doubleUDF(col("b"))), Seq(Row(null), Row(4.4), Row(null)))
  }

  test("Test for Option[Boolean]") {
    createTable(tableName, "a double, b double")
    runQuery(s"insert into $tableName values (1, 1), (null, 2), (null, null)", session)
    val df = session.table(tableName)
    val UDF = udf((a: Option[Boolean], b: Option[Boolean]) => a == b)
    checkAnswer(
      df.withColumn("c", UDF($"a", $"b")).select($"c"),
      Seq(Row(true), Row(false), Row(true)))
  }

  test("Test for double data type") {
    val df = Seq(1.01, 2.01, 3.01).toDF("a")
    val UDF = udf((x: Double) => x + x)
    assert(
      df.withColumn("c", UDF(col("a"))).collect()
        sameElements
          Array[Row](Row(1.01, 2.02), Row(2.01, 4.02), Row(3.01, 6.02)))
  }

  test("Test for boolean data type") {
    val df = Seq((1, 1), (2, 2), (3, 4)).toDF("a", "b")
    val UDF = udf((a: Int, b: Int) => a == b)
    checkAnswer(
      df.withColumn("c", UDF($"a", $"b")).select($"c"),
      Seq(Row(true), Row(true), Row(false)))
  }

  test("Test for binary data type") {
    val data = Seq("Hello", "World")
    val df = data.toDF("a")

    val toBinary = udf((a: String) => a.getBytes)
    val fromBinary = udf((a: Array[Byte]) => new String(a))

    checkAnswer(df.withColumn("c", toBinary($"a")).select($"c"), data.map(s => Row(s.getBytes)))

    val df2 = data.map(_.getBytes()).toDF("a")

    checkAnswer(df2.withColumn("c", fromBinary($"a")).select($"c"), data.map(Row(_)))
  }

  // Excluding this test for known Timezone issue in stored proc
  test("Test for date and timestamp data type", JavaStoredProcExclude) {
    val input = Seq(
      (Date.valueOf("2019-01-01"), Timestamp.valueOf("2019-01-01 00:00:00")),
      (Date.valueOf("2020-01-01"), Timestamp.valueOf("2020-01-01 00:00:00")),
      (null, null))
    val out = input.map {
      case (null, null) => Row(null, null)
      case (a, b) =>
        Row(new Timestamp(a.getTime), new Date(b.getTime))
    }
    val dates = input.toDF("date", "timestamp")
    val toSNUDF = udf((x: Date) => if (x == null) null else new Timestamp(x.getTime))
    val toDateUDF = udf((x: Timestamp) => if (x == null) null else new Date(x.getTime))

    checkAnswer(
      dates
        .withColumn("c", toSNUDF(col("date")))
        .withColumn("d", toDateUDF(col("timestamp")))
        .select($"c", $"d"),
      out)
  }

  test("Test for time, date, timestamp with snowflake timezone") {
    var df = session.sql("select '00:00:00' :: time as col1")
    var addUDF = udf((x: Time) => new Time(x.getTime + 5000))
    assert(df.select(addUDF(col("col1"))).collect()(0).getTime(0).toString == "00:00:05")

    df = session.sql("select '2020-1-1' :: date as col1")
    addUDF = udf((x: Date) => new Date(x.getTime + 3600 * 1000 * 24))
    assert(df.select(addUDF(col("col1"))).collect()(0).getDate(0).toString == "2020-01-02")

    df = session.sql("select '2020-1-1 00:00:00' :: timestamp as col1")
    addUDF = udf((x: Timestamp) => new Timestamp(x.getTime + 5000))
    assert(
      df.select(addUDF(col("col1")))
        .collect()(0)
        .getTimestamp(0)
        .toString == "2020-01-01 00:00:05.0")
  }

  test("Test for Geography data type") {
    createTable(tableName, "g geography")
    runQuery(s"insert into $tableName values ('POINT(30 10)'), ('POINT(50 60)'), (null)", session)
    val df = session.table(tableName)

    val geographyUDF = udf((g: Geography) => {
      if (g == null) {
        null
      } else {
        if (g.asGeoJSON().equals("{\"coordinates\":[50,60],\"type\":\"Point\"}")) {
          Geography.fromGeoJSON(g.asGeoJSON())
        } else {
          Geography.fromGeoJSON(
            g.asGeoJSON()
              .replace("0", ""))
        }
      }
    })

    checkAnswer(
      df.select(geographyUDF(col("g"))),
      Seq(
        Row(
          Geography.fromGeoJSON(
            "{\n  \"coordinates\": [\n    3,\n    1\n  ],\n  \"type\": \"Point\"\n}")),
        Row(
          Geography.fromGeoJSON(
            "{\n  \"coordinates\": [\n    50,\n    60\n  ],\n  \"type\": \"Point\"\n}")),
        Row(null)))
  }

  // Excluding this test for known Timezone issue in stored proc
  test("Test for Variant Timestamp input", JavaStoredProcExclude) {
    val variantTimestampUDF = udf((v: Variant) => {
      if (v == null) {
        null
      } else {
        new Timestamp(v.asTimestamp().getTime + 5000)
      }
    })

    // The UDF will add 5 seconds to the timestamp.
    // '2017-02-24 12:00:00.456' -> '2017-02-24 12:00:05.456'
    checkAnswer(
      variant1.select(variantTimestampUDF(col("timestamp_ntz1"))),
      Seq(Row(Timestamp.valueOf("2017-02-24 20:00:05.456"))))
  }

  // Excluding this test for known Timezone issue in stored proc
  test("Test for Variant Time input", JavaStoredProcExclude) {
    val variantTimeUDF = udf((v: Variant) => new Timestamp(v.asTime().getTime + 5000))

    // The UDF will add 5 seconds to the time. There is precision loss in the time.
    // There is 8 hours time difference local computer and server,
    // so 20:57:01 -> 04:57:01 + one day. '1970-01-02 04:57:01.0' -> '1970-01-02 04:57:06.0'
    checkAnswer(
      variant1.select(variantTimeUDF(col("time1"))),
      Seq(Row(Timestamp.valueOf("1970-01-02 04:57:06.0"))))
  }

  // Excluding this test for known Timezone issue in stored proc
  test("Test for Variant Date input", JavaStoredProcExclude) {
    val variantUDF = udf((v: Variant) => new Timestamp(v.asDate().getTime + 5000))

    // The UDF will add 5 seconds to the date.
    // There is 8 hours time difference between local computer and server,
    // so 2017-02-24 -> 2017-02-24 08:00:00. '2017-02-24 08:00:00' -> '2017-02-24 08:00:05'
    checkAnswer(
      variant1.select(variantUDF(col("date1"))),
      Seq(Row(Timestamp.valueOf("2017-02-24 08:00:05.0"))))
  }

  test("Test for Variant String input") {
    val variantTimeUDF = udf((v: Variant) => v.asString())
    checkAnswer(variant1.select(variantTimeUDF(col("str1"))), Seq(Row("X")))
  }

  test("Test for Variant Binary input") {
    val variantTimeUDF = udf((v: Variant) => v)
    // snow -> 0x73 0x6e 0x6f 0x77
    checkAnswer(variant1.select(variantTimeUDF(col("bin1"))), Seq(Row("\"736E6F77\"")))
  }

  test("Test for Variant Boolean input") {
    val variantTimeUDF = udf((v: Variant) => v.asBoolean())
    checkAnswer(variant1.select(variantTimeUDF(col("bool1"))), Seq(Row(true)))
  }

  test("Test for Variant Number input") {
    val variantNumberUDF = udf((v: Variant) => v.asLong() + 20)
    // The UDF will add 20 to the value. 15 -> 35
    checkAnswer(variant1.select(variantNumberUDF(col("num1"))), Seq(Row(35)))
  }

  test("Test for Variant Null") {
    intercept[UnsupportedOperationException] {
      val variantNullOutputUDF = udf((_: Variant) => null)
      checkAnswer(variant1.select(variantNullOutputUDF(col("timestamp_ntz1"))), Seq(Row()))
    }

    val variantNullInputUDF = udf((v: Variant) => {
      if (v == null) {
        null
      } else {
        v.asMap()("a").asJsonString()
      }
    })
    checkAnswer(
      nullJson1.select(variantNullInputUDF(col("v"))),
      Seq(Row("null"), Row("\"foo\""), Row(null)),
      sort = false)
  }

  test("Test for string Variant output") {
    val variantOutputUDF = udf((_: Variant) => {
      new Variant("foo")
    })
    checkAnswer(variant1.select(variantOutputUDF(col("num1"))), Seq(Row("\"foo\"")))
  }

  test("Test for number Variant output") {
    val variantOutputUDF = udf((_: Variant) => new Variant(1))
    checkAnswer(variant1.select(variantOutputUDF(col("num1"))), Seq(Row("1")))
  }

  test("Test for null string Variant output") {
    val variantOutputUDF = udf((_: Variant) => new Variant("null"))
    checkAnswer(variant1.select(variantOutputUDF(col("num1"))), Seq(Row("null")))
  }

  test("Test for boolean Variant output") {
    val variantOutputUDF = udf((_: Variant) => new Variant(true))
    checkAnswer(variant1.select(variantOutputUDF(col("num1"))), Seq(Row("true")))
  }

  test("Test for json Variant output") {
    val variantOutputUDF = udf((_: Variant) => new Variant("{\"a\": \"foo\"}"))
    checkAnswer(
      variant1.select(variantOutputUDF(col("num1"))),
      Seq(Row("{\n  \"a\": \"foo\"\n}")))
  }

  test("Test for array Variant output") {
    val variantOutputUDF = udf((_: Variant) => new Variant("[1,2,3]"))
    checkAnswer(variant1.select(variantOutputUDF(col("num1"))), Seq(Row("[\n  1,\n  2,\n  3\n]")))
  }

  test("Test for Date Variant output") {
    val variantOutputUDF = udf((_: Variant) => new Variant(Date.valueOf("2020-10-10")))
    checkAnswer(variant1.select(variantOutputUDF(col("num1"))), Seq(Row("\"2020-10-10\"")))
  }

  test("Test for Time Variant output") {
    val variantOutputUDF = udf((_: Variant) => new Variant(Time.valueOf("01:02:03")))
    checkAnswer(variant1.select(variantOutputUDF(col("num1"))), Seq(Row("\"01:02:03\"")))
  }

  test("Test for Timestamp Variant output") {
    val variantOutputUDF =
      udf((_: Variant) => new Variant(Timestamp.valueOf("2020-10-10 01:02:03")))
    checkAnswer(
      variant1.select(variantOutputUDF(col("num1"))),
      Seq(Row("\"2020-10-10 01:02:03.0\"")))
  }

  test("Test for Array[Variant]") {
    var variantUDF = udf((v: Array[Variant]) => v ++ Array(new Variant(1)))
    // Variant("Example") is translated as "\"Example\"", which is a valid JSON string
    // Server is not parsing the JSON string correctly, resulting in twice-escaped string.
    // In JSON "\"Example\"" is a text node of value "Example", server should parse this JSON and
    // strip \" from it. todo: SNOW-254551
    checkAnswer(
      variant1.select(variantUDF(col("arr1"))),
      Seq(Row("[\n  \"\\\"Example\\\"\",\n  \"1\"\n]")))

    variantUDF = udf((v: Array[Variant]) => v ++ Array(null))
    checkAnswer(
      variant1.select(variantUDF(col("arr1"))),
      Seq(Row("[\n  \"\\\"Example\\\"\",\n  undefined\n]")))

    // UDF that returns null. Need the if ... else ... to define a return type.
    variantUDF = udf((v: Array[Variant]) => if (true) null else Array(new Variant(1)))
    checkAnswer(variant1.select(variantUDF(col("arr1"))), Seq(Row(null)))
  }

  test("Test for Map[String, Variant]") {
    var variantUDF = udf((v: mutable.Map[String, Variant]) => v + ("a" -> new Variant(1)))
    checkAnswer(
      variant1.select(variantUDF(col("obj1"))),
      Seq(Row("{\n  \"Tree\": \"\\\"Pine\\\"\",\n  \"a\": \"1\"\n}")))

    variantUDF = udf((v: mutable.Map[String, Variant]) => v + ("a" -> null))
    checkAnswer(
      variant1.select(variantUDF(col("obj1"))),
      Seq(Row("{\n  \"Tree\": \"\\\"Pine\\\"\",\n  \"a\": null\n}")))

    // UDF that returns null. Need the if ... else ... to define a return type.
    variantUDF = udf((v: mutable.Map[String, Variant]) =>
      if (true) null else mutable.Map[String, Variant]("a" -> new Variant(1)))
    checkAnswer(variant1.select(variantUDF(col("obj1"))), Seq(Row(null)))
  }

  // Excluding this test for known Timezone issue in stored proc
  test("Test for time and timestamp data type", JavaStoredProcExclude) {
    createTable(tableName, "time time, timestamp timestamp")
    runQuery(
      s"insert into $tableName select to_time(a), to_timestamp(b) from values('01:02:03', " +
        s"'1970-01-01 01:02:03'),(null, null) as T(a, b)",
      session)
    val times = session.table(tableName)
    val toSNUDF = udf((x: Time) => if (x == null) null else new Timestamp(x.getTime))
    val toTimeUDF = udf((x: Timestamp) => if (x == null) null else new Time(x.getTime))

    checkAnswer(
      times
        .withColumn("c", toSNUDF(col("time")))
        .withColumn("d", toTimeUDF(col("timestamp")))
        .select($"c", $"d"),
      Seq(
        Row(Timestamp.valueOf("1970-01-01 01:02:03"), Time.valueOf("01:02:03")),
        Row(null, null)))
  }
  // Excluding the tests for 2 to 22 args from stored procs to limit overall time
  // of the UDFSuite run as a regression test
// scalastyle:off line.size.limit
  /* This script generates the tests below
  (2 to 22).foreach { x =>
   val data = (1 to x).map(_.toString).reduceLeft(_ + ", " + _)
   val cols = (1 to x).map(a => s"c$a")
   val funcSig = cols.map(_ + ": Int").mkString(", ")
   val func = cols.reduceLeft(_ + "+" + _)
   val udfInvoker = cols.map("col(\""+ _ +"\")").mkString(", ")

   println(s"""
    |test("Test for num args : $x" , JavaStoredProcExclude) {
    | val result = (1 to $x).reduceLeft(_ + _)
    | val columns = (1 to $x).map("c" + _)
    | val df = Seq(($data)).toDF(columns)
    | val sum = udf(($funcSig) => $func)
    | val sum1 = session.udf.registerTemporary(($funcSig) => $func)
    | val funcName = randomName()
    | session.udf.registerTemporary(funcName, ($funcSig) => $func)
    | checkAnswer(
    |   df.withColumn("res", sum($udfInvoker)).select("res"),
    |    Seq(Row(result)))
    | checkAnswer(
    |   df.withColumn("res", sum1($udfInvoker)).select("res"),
    |    Seq(Row(result)))
    | checkAnswer(
    |   df.withColumn("res", callUDF(funcName, $udfInvoker))
    |    .select("res"), Seq(Row(result)))
    |}""".stripMargin)
    }
   */

  test("Test for num args : 2", JavaStoredProcExclude) {
    val result = (1 to 2).reduceLeft(_ + _)
    val columns = (1 to 2).map("c" + _)
    val df = Seq((1, 2)).toDF(columns)
    val sum = udf((c1: Int, c2: Int) => c1 + c2)
    val sum1 = session.udf.registerTemporary((c1: Int, c2: Int) => c1 + c2)
    val funcName = randomName()
    session.udf.registerTemporary(funcName, (c1: Int, c2: Int) => c1 + c2)
    checkAnswer(df.withColumn("res", sum(col("c1"), col("c2"))).select("res"), Seq(Row(result)))
    checkAnswer(df.withColumn("res", sum1(col("c1"), col("c2"))).select("res"), Seq(Row(result)))
    checkAnswer(
      df.withColumn("res", callUDF(funcName, col("c1"), col("c2")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 3", JavaStoredProcExclude) {
    val result = (1 to 3).reduceLeft(_ + _)
    val columns = (1 to 3).map("c" + _)
    val df = Seq((1, 2, 3)).toDF(columns)
    val sum = udf((c1: Int, c2: Int, c3: Int) => c1 + c2 + c3)
    val sum1 = session.udf.registerTemporary((c1: Int, c2: Int, c3: Int) => c1 + c2 + c3)
    val funcName = randomName()
    session.udf.registerTemporary(funcName, (c1: Int, c2: Int, c3: Int) => c1 + c2 + c3)
    checkAnswer(
      df.withColumn("res", sum(col("c1"), col("c2"), col("c3"))).select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn("res", sum1(col("c1"), col("c2"), col("c3"))).select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn("res", callUDF(funcName, col("c1"), col("c2"), col("c3")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 4", JavaStoredProcExclude) {
    val result = (1 to 4).reduceLeft(_ + _)
    val columns = (1 to 4).map("c" + _)
    val df = Seq((1, 2, 3, 4)).toDF(columns)
    val sum = udf((c1: Int, c2: Int, c3: Int, c4: Int) => c1 + c2 + c3 + c4)
    val sum1 =
      session.udf.registerTemporary((c1: Int, c2: Int, c3: Int, c4: Int) => c1 + c2 + c3 + c4)
    val funcName = randomName()
    session.udf
      .registerTemporary(funcName, (c1: Int, c2: Int, c3: Int, c4: Int) => c1 + c2 + c3 + c4)
    checkAnswer(
      df.withColumn("res", sum(col("c1"), col("c2"), col("c3"), col("c4"))).select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn("res", sum1(col("c1"), col("c2"), col("c3"), col("c4"))).select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn("res", callUDF(funcName, col("c1"), col("c2"), col("c3"), col("c4")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 5", JavaStoredProcExclude) {
    val result = (1 to 5).reduceLeft(_ + _)
    val columns = (1 to 5).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5)).toDF(columns)
    val sum = udf((c1: Int, c2: Int, c3: Int, c4: Int, c5: Int) => c1 + c2 + c3 + c4 + c5)
    val sum1 = session.udf.registerTemporary((c1: Int, c2: Int, c3: Int, c4: Int, c5: Int) =>
      c1 + c2 + c3 + c4 + c5)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int) => c1 + c2 + c3 + c4 + c5)
    checkAnswer(
      df.withColumn("res", sum(col("c1"), col("c2"), col("c3"), col("c4"), col("c5")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn("res", sum1(col("c1"), col("c2"), col("c3"), col("c4"), col("c5")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(funcName, col("c1"), col("c2"), col("c3"), col("c4"), col("c5")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 6", JavaStoredProcExclude) {
    val result = (1 to 6).reduceLeft(_ + _)
    val columns = (1 to 6).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6)).toDF(columns)
    val sum =
      udf((c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int) => c1 + c2 + c3 + c4 + c5 + c6)
    val sum1 =
      session.udf.registerTemporary((c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int) => c1 + c2 + c3 + c4 + c5 + c6)
    checkAnswer(
      df.withColumn("res", sum(col("c1"), col("c2"), col("c3"), col("c4"), col("c5"), col("c6")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn("res", sum1(col("c1"), col("c2"), col("c3"), col("c4"), col("c5"), col("c6")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(funcName, col("c1"), col("c2"), col("c3"), col("c4"), col("c5"), col("c6")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 7", JavaStoredProcExclude) {
    val result = (1 to 7).reduceLeft(_ + _)
    val columns = (1 to 7).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7)).toDF(columns)
    val sum = udf((c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7)
    val sum1 = session.udf.registerTemporary(
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7)
    checkAnswer(
      df.withColumn(
          "res",
          sum(col("c1"), col("c2"), col("c3"), col("c4"), col("c5"), col("c6"), col("c7")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(col("c1"), col("c2"), col("c3"), col("c4"), col("c5"), col("c6"), col("c7")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 8", JavaStoredProcExclude) {
    val result = (1 to 8).reduceLeft(_ + _)
    val columns = (1 to 8).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8)).toDF(columns)
    val sum = udf((c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int, c8: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8)
    val sum1 = session.udf.registerTemporary(
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int, c8: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int, c8: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 9", JavaStoredProcExclude) {
    val result = (1 to 9).reduceLeft(_ + _)
    val columns = (1 to 9).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9)).toDF(columns)
    val sum =
      udf((c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int, c8: Int, c9: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9)
    val sum1 = session.udf.registerTemporary(
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int, c8: Int, c9: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (c1: Int, c2: Int, c3: Int, c4: Int, c5: Int, c6: Int, c7: Int, c8: Int, c9: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 10", JavaStoredProcExclude) {
    val result = (1 to 10).reduceLeft(_ + _)
    val columns = (1 to 10).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF(columns)
    val sum = udf(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10)
    val sum1 = session.udf.registerTemporary(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 11", JavaStoredProcExclude) {
    val result = (1 to 11).reduceLeft(_ + _)
    val columns = (1 to 11).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)).toDF(columns)
    val sum = udf(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11)
    val sum1 = session.udf.registerTemporary(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 12", JavaStoredProcExclude) {
    val result = (1 to 12).reduceLeft(_ + _)
    val columns = (1 to 12).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)).toDF(columns)
    val sum = udf(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12)
    val sum1 = session.udf.registerTemporary(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 13", JavaStoredProcExclude) {
    val result = (1 to 13).reduceLeft(_ + _)
    val columns = (1 to 13).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)).toDF(columns)
    val sum = udf(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13)
    val sum1 = session.udf.registerTemporary(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 14", JavaStoredProcExclude) {
    val result = (1 to 14).reduceLeft(_ + _)
    val columns = (1 to 14).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)).toDF(columns)
    val sum = udf(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14)
    val sum1 = session.udf.registerTemporary(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int) => c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 15", JavaStoredProcExclude) {
    val result = (1 to 15).reduceLeft(_ + _)
    val columns = (1 to 15).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)).toDF(columns)
    val sum = udf(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15)
    val sum1 = session.udf.registerTemporary(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 16", JavaStoredProcExclude) {
    val result = (1 to 16).reduceLeft(_ + _)
    val columns = (1 to 16).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)).toDF(columns)
    val sum = udf(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16)
    val sum1 = session.udf.registerTemporary(
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 17", JavaStoredProcExclude) {
    val result = (1 to 17).reduceLeft(_ + _)
    val columns = (1 to 17).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)).toDF(columns)
    val sum = udf((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17)
    val sum1 = session.udf.registerTemporary((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int,
          c17: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 18", JavaStoredProcExclude) {
    val result = (1 to 18).reduceLeft(_ + _)
    val columns = (1 to 18).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)).toDF(columns)
    val sum = udf((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18)
    val sum1 = session.udf.registerTemporary((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int,
          c17: Int,
          c18: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 19", JavaStoredProcExclude) {
    val result = (1 to 19).reduceLeft(_ + _)
    val columns = (1 to 19).map("c" + _)
    val df =
      Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)).toDF(columns)
    val sum = udf((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19)
    val sum1 = session.udf.registerTemporary((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int,
          c17: Int,
          c18: Int,
          c19: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 20", JavaStoredProcExclude) {
    val result = (1 to 20).reduceLeft(_ + _)
    val columns = (1 to 20).map("c" + _)
    val df =
      Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)).toDF(columns)
    val sum = udf((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int,
        c20: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20)
    val sum1 = session.udf.registerTemporary((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int,
        c20: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int,
          c17: Int,
          c18: Int,
          c19: Int,
          c20: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 21", JavaStoredProcExclude) {
    val result = (1 to 21).reduceLeft(_ + _)
    val columns = (1 to 21).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21))
      .toDF(columns)
    val sum = udf((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int,
        c20: Int,
        c21: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20 + c21)
    val sum1 = session.udf.registerTemporary((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int,
        c20: Int,
        c21: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20 + c21)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int,
          c17: Int,
          c18: Int,
          c19: Int,
          c20: Int,
          c21: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20 + c21)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20"),
            col("c21")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20"),
            col("c21")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20"),
            col("c21")))
        .select("res"),
      Seq(Row(result)))
  }

  test("Test for num args : 22", JavaStoredProcExclude) {
    val result = (1 to 22).reduceLeft(_ + _)
    val columns = (1 to 22).map("c" + _)
    val df = Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22))
      .toDF(columns)
    val sum = udf((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int,
        c20: Int,
        c21: Int,
        c22: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20 + c21 + c22)
    val sum1 = session.udf.registerTemporary((
        c1: Int,
        c2: Int,
        c3: Int,
        c4: Int,
        c5: Int,
        c6: Int,
        c7: Int,
        c8: Int,
        c9: Int,
        c10: Int,
        c11: Int,
        c12: Int,
        c13: Int,
        c14: Int,
        c15: Int,
        c16: Int,
        c17: Int,
        c18: Int,
        c19: Int,
        c20: Int,
        c21: Int,
        c22: Int) =>
      c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20 + c21 + c22)
    val funcName = randomName()
    session.udf.registerTemporary(
      funcName,
      (
          c1: Int,
          c2: Int,
          c3: Int,
          c4: Int,
          c5: Int,
          c6: Int,
          c7: Int,
          c8: Int,
          c9: Int,
          c10: Int,
          c11: Int,
          c12: Int,
          c13: Int,
          c14: Int,
          c15: Int,
          c16: Int,
          c17: Int,
          c18: Int,
          c19: Int,
          c20: Int,
          c21: Int,
          c22: Int) =>
        c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14 + c15 + c16 + c17 + c18 + c19 + c20 + c21 + c22)
    checkAnswer(
      df.withColumn(
          "res",
          sum(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20"),
            col("c21"),
            col("c22")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          sum1(
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20"),
            col("c21"),
            col("c22")))
        .select("res"),
      Seq(Row(result)))
    checkAnswer(
      df.withColumn(
          "res",
          callUDF(
            funcName,
            col("c1"),
            col("c2"),
            col("c3"),
            col("c4"),
            col("c5"),
            col("c6"),
            col("c7"),
            col("c8"),
            col("c9"),
            col("c10"),
            col("c11"),
            col("c12"),
            col("c13"),
            col("c14"),
            col("c15"),
            col("c16"),
            col("c17"),
            col("c18"),
            col("c19"),
            col("c20"),
            col("c21"),
            col("c22")))
        .select("res"),
      Seq(Row(result)))
  }

  // system$cancel_all_queries not allowed from owner mode procs
  test("cancel query", JavaStoredProcExclude, UnstableTest) {
    val udf1 = udf((x: Int) => {
      Thread.sleep(20000)
      x
    })

    val query: Future[Boolean] = testCanceled {
      Seq(1, 2, 3).toDF("a").select(udf1(col("a"))).show()
    }

    // wait until action started
    Thread.sleep(10000)

    session.cancelAll()
    assert(Await.result(query, 2 minutes))
  }

  test("negative test to input invalid func name") {
    val doubleUDF = (x: Int) => x + x
    val functionName = "negative test invalid name"
    val ex = intercept[SnowparkClientException] {
      session.udf.registerTemporary(functionName, doubleUDF)
    }
    assert(ex.getMessage.matches(".*The object name .* is invalid."))
  }

  test("empty argument function") {
    val func = () => 100
    val udf1 = functions.udf(func)
    val df = Seq(1).toDF("col")
    assert(df.select(udf1()).collect().head.getInt(0) == 100)

    val udf2 = session.udf.registerTemporary(func)
    assert(df.select(udf2()).collect().head.getInt(0) == 100)

    val tempFuncName = randomFunctionName()
    session.udf.registerTemporary(tempFuncName, func)
    assert(df.select(functions.builtin(tempFuncName)()).collect().head.getInt(0) == 100)

    val permFuncName = randomFunctionName()
    val stageName = randomStageName()
    try {
      createStage(stageName, isTemporary = false)
      session.udf.registerPermanent(permFuncName, func, stageName)
      assert(df.select(functions.builtin(permFuncName)()).collect().head.getInt(0) == 100)
    } finally {
      dropStage(stageName)
      runQuery(s"drop function $permFuncName()", session)
    }
  }

  // The test account needs to access "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."WEB_SALES"
  // Regression test does not have the SNOWFLAKE_SAMPLE_DATA table, so disable this test in Stored Proc
  test("repro SNOW-415682", JavaStoredProcExclude, SampleDataTest) {
    import com.snowflake.snowpark.types._
    val mySess = session

    // Define a one-day slice of the Web_Sales table in TPCDS
    val df = mySess
      .table(Seq("SNOWFLAKE_SAMPLE_DATA", "TPCDS_SF10TCL", "WEB_SALES"))
      .filter($"WS_SOLD_DATE_SK" === 2451952)
      // Add below 2 extra filters to make the result row count is 10
      .filter($"WS_SOLD_TIME_SK" === 35874)
      .filter($"WS_BILL_CUSTOMER_SK" === 10530912)

    // Get the list of column descriptions
    val cols = df.schema

    // Derive the subset of columns containing decimal values, based on data type
    val metricCols = cols
      .filter(_.dataType == DecimalType(7, 2))
      .map(c => col(c.name))

    // Define a set of aggregates representing the min and max of each metric column
    val metricAggs = metricCols
      .map(c => List(max(c), min(c)))
      .flatten

    // Get the results
    val myAggs = df
      .select(metricAggs)
      .collect

    // Construct a set of tuples containing column name, min and max value
    val myAggTuples = metricCols.zipWithIndex
      .map { case (e, i) => (e, myAggs(0)(2 * i), myAggs(0)(2 * i + 1)) }

    // Define a <overly simple> function that normalizes
    // an incoming value based on min and max for that column.
    // Build a UDF for that function
    val normUdf = udf((myVal: Double, myMax: Double, myMin: Double) => {
      (myVal - myMin) / (myMax - myMin)
    })

    // Define the set of columns represening normalized metrics values,
    // by calling the UDF on each metrics column along with the precomputed min and max.
    // Note new column names are constructed for the results
    val metricsNormalized = myAggTuples
      .map {
        case (col, colMin, colMax) =>
          normUdf(col, lit(colMin), lit(colMax)) as
            "norm_" + col.getName.get.dropRight(1).drop(1)
      }

    // Now query the table retrieving normalized column values instead of absolute values
    val myNorms = df
      .select(metricsNormalized)
      // NORM_WS_EXT_DISCOUNT_AMT has a BigDecimal value with (precision = 1, scale = 2)
      .select("NORM_WS_EXT_DISCOUNT_AMT")
      .sort(col("NORM_WS_EXT_DISCOUNT_AMT"))

    assert(
      getShowString(myNorms, 100) ==
        """|------------------------------
           ||"NORM_WS_EXT_DISCOUNT_AMT"  |
           |------------------------------
           ||0.0                         |
           ||0.003556988004215603        |
           ||0.005327891585434567        |
           ||0.031172106954869112        |
           ||0.03785634836528609         |
           ||0.06832313005602315         |
           ||0.1572793596020284          |
           ||0.24924957011943236         |
           ||0.5399685289472378          |
           ||1.0                         |
           |------------------------------
           |""".stripMargin)
  }

  test("register temp UDF doesn't commit open transaction") {
    val udf = (x: Int) => x + 1
    session.sql("begin").collect()
    val udf1 = session.udf.registerTemporary(udf)
    assert(isActiveTransaction(session))
    val df = session.createDataFrame(Seq(1, 2)).toDF(Seq("a"))
    checkAnswer(df.select(udf1(df("a"))), Seq(Row(2), Row(3)))
    assert(isActiveTransaction(session))

    session.sql("commit").collect()
    assert(!isActiveTransaction(session))
  }
}

@UDFTest
class AlwaysCleanUDFSuite extends UDFSuite with AlwaysCleanSession {
  // todo: closure cleaner doesn't work with Java 17, fix in SNOW-991144
  ignore("Test with closure cleaner enabled") {
    val myDf = session.sql("select 'Raymond' NAME")
    val readFileUdf = udf(TestClassWithoutFieldAccess.run)
    myDf.withColumn("CONCAT", readFileUdf(col("NAME"))).show()
  }
}

@UDFTest
class NeverCleanUDFSuite extends UDFSuite with NeverCleanSession {
  test("Test without closure cleaner") {
    val myDf = session.sql("select 'Raymond' NAME")
    // Without closure cleaner, this test will throw error
    intercept[NotSerializableException] {
      val readFileUdf = udf(TestClassWithoutFieldAccess.run)
      myDf.withColumn("CONCAT", readFileUdf(col("NAME"))).show()
    }
  }
}
